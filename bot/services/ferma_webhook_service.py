from __future__ import annotations
import ipaddress
import logging
import os
import json
from typing import List, Any, Optional, Tuple

from aiohttp import web
from sqlalchemy import select, and_
from sqlalchemy.orm import sessionmaker

from db.repositories.receipts_repo import ReceiptsRepo
from db.models import ReceiptStatus, Payment
from config.settings import get_settings  # NEW

# Логгер модуля (и дубли на root.info для видимости при минимальной конфигурации логгера)
log = logging.getLogger("webhook.ferma")

# --- утилиты безопасности ---

def _trusted_cidrs() -> List[ipaddress._BaseNetwork]:
    raw = os.getenv("FERMA_TRUSTED_CIDRS", "")
    nets: List[ipaddress._BaseNetwork] = []
    for part in raw.split(","):
        s = part.strip()
        if not s:
            continue
        try:
            nets.append(ipaddress.ip_network(s))
        except Exception:
            log.warning("Invalid CIDR in FERMA_TRUSTED_CIDRS: %s", s)
    return nets

def _ip_allowed(request: web.Request, cidrs: List[ipaddress._BaseNetwork]) -> bool:
    if not cidrs:  # allow all if not set
        return True
    try:
        peer = request.transport.get_extra_info("peername")
        if not peer:
            return False
        host, _ = peer
        ip = ipaddress.ip_address(host)
        return any(ip in net for net in cidrs)
    except Exception:
        return False

# --- разбор payload и нормализация статуса ---

def _get(data: dict, *keys, default=None):
    cur = data
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default

def _as_int_status(status_code: Any) -> Optional[int]:
    """
    Приводит код к int:
      2 / "2" / "CONFIRMED" -> 2
      1 / "1" / "PROCESSED" -> 1
      3 / "3" / "KKT_ERROR" -> 3
      0 / "0" / "NEW"       -> 0
    """
    if status_code is None:
        return None
    try:
        return int(status_code)
    except Exception:
        pass
    s = str(status_code).strip().upper()
    mapping = {"CONFIRMED": 2, "PROCESSED": 1, "KKT_ERROR": 3, "NEW": 0}
    if s in mapping:
        return mapping[s]
    try:
        return int(s)
    except Exception:
        return None

def _short(s: Any, limit: int = 600) -> str:
    """Обрезаем большие payload'ы в логах."""
    try:
        text = s if isinstance(s, str) else json.dumps(s, ensure_ascii=False)
        return text if len(text) <= limit else text[:limit] + "...[truncated]"
    except Exception:
        return str(s)[:limit]

# --- выбор чата для отсылки чека ---

async def _resolve_target_chat(
    session,
    pr,  # запись в receipts_repo
) -> Tuple[Optional[int], str]:
    """
    Возвращает (chat_id, reason):
      - если Payment.source == "admin_link" и задан LOG_CHAT_ID -> (LOG_CHAT_ID, "admin_link")
      - иначе, если есть payment.user_id                       -> (user_id, "user")
      - иначе, если задан LOG_CHAT_ID                          -> (LOG_CHAT_ID, "fallback_log")
      - иначе                                                  -> (None, "none")
    """
    settings = get_settings()
    log_chat_id = None
    try:
        if getattr(settings, "LOG_CHAT_ID", None):
            log_chat_id = int(settings.LOG_CHAT_ID)
    except Exception:
        log_chat_id = None

    payment_row = None

    # 1) Пытаемся найти платёж по yookassa_payment_id
    try:
        q1 = select(Payment).where(Payment.yookassa_payment_id == pr.payment_id)
        payment_row = (await session.execute(q1)).scalars().first()
    except Exception:
        payment_row = None

    # 2) Альтернатива: provider='yookassa' + provider_payment_id
    if not payment_row:
        try:
            if hasattr(Payment, "provider") and hasattr(Payment, "provider_payment_id"):
                q2 = select(Payment).where(
                    and_(Payment.provider == "yookassa", Payment.provider_payment_id == pr.payment_id)
                )
                payment_row = (await session.execute(q2)).scalars().first()
        except Exception:
            payment_row = None

    # 3) Если есть source == admin_link -> слать в лог-чат
    source = ""
    try:
        source = (getattr(payment_row, "source", None) or "").strip().lower() if payment_row else ""
    except Exception:
        source = ""

    if source == "admin_link" and log_chat_id:
        return log_chat_id, "admin_link"

    # 4) Иначе попробуем пользователя
    try:
        if payment_row and getattr(payment_row, "user_id", None):
            return int(payment_row.user_id), "user"
    except Exception:
        pass

    # 5) Fallback: если не нашли пользователя, но есть лог-чат — туда
    if log_chat_id:
        return log_chat_id, "fallback_log"

    return None, "none"

# --- Фабрика хендлера ДЛЯ app.router.add_post ---

def make_ferma_callback_handler(async_session_factory: sessionmaker):
    cidrs = _trusted_cidrs()

    async def ferma_callback(request: web.Request) -> web.Response:
        if not _ip_allowed(request, cidrs):
            return web.json_response({"ok": False, "error": "forbidden"}, status=403)
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid_json"}, status=400)

        data = payload.get("Data") or {}
        status_code = data.get("StatusCode")
        invoice_id = data.get("InvoiceId")
        receipt_id = data.get("ReceiptId")
        device = data.get("Device") or {}
        ofd_url = device.get("OfdReceiptUrl")

        logging.info(
            "Ferma webhook IN: status=%r invoice=%r receipt=%r ofd=%r",
            status_code, invoice_id, receipt_id, ofd_url
        )

        async with async_session_factory() as session:
            repo = ReceiptsRepo(session)

            # 1) Сначала ищем по ReceiptId — самый надёжный ключ
            pr = None
            if receipt_id:
                try:
                    pr = await repo.get_by_receipt_id(receipt_id)
                except Exception:
                    pr = None
            # 2) Если не нашли — ищем по InvoiceId
            if not pr and invoice_id:
                pr = await repo.get_by_invoice_id(invoice_id)

            if not pr:
                logging.info(
                    "Ferma webhook: unknown invoice/receipt (ignored). invoice=%s receipt=%s",
                    invoice_id, receipt_id
                )
                return web.json_response({"ok": True, "ignored": True, "reason": "unknown_invoice"})

            # Нормализуем код статуса
            code = _as_int_status(status_code)

            branch = "unknown"
            notified = False
            repo_action = None

            if code == 2:  # CONFIRMED
                await repo.mark_confirmed(pr, ofd_url)
                await session.commit()
                repo_action = "mark_confirmed"
                branch = "confirmed"

                bot = request.app.get("bot")
                if not bot:
                    logging.warning("Ferma webhook: app['bot'] is missing; cannot notify")
                elif ofd_url:
                    # NEW: единый выбор маршрута (LOG_CHAT_ID для admin_link, иначе user, иначе fallback в LOG_CHAT_ID)
                    target_chat_id, reason = await _resolve_target_chat(session, pr)
                    if target_chat_id:
                        try:
                            await bot.send_message(
                                chat_id=target_chat_id,
                                text=f"🧾 Чек сформирован: {ofd_url}",
                                disable_web_page_preview=True,
                            )
                            notified = True
                            logging.info(
                                "Ferma webhook: receipt sent to %s (chat_id=%s)",
                                reason, target_chat_id
                            )
                        except Exception:
                            logging.exception("Failed to send receipt message (reason=%s, chat_id=%s)", reason, target_chat_id)
                    else:
                        logging.warning(
                            "Ferma webhook: cannot map receipt to user or log chat (payment_id=%s)",
                            pr.payment_id
                        )

            elif code == 1:  # PROCESSED
                await repo.mark_processed(pr)
                await session.commit()
                repo_action = "mark_processed"
                branch = "processed"

            elif code == 3:  # KKT_ERROR
                await repo.mark_kkt_error(pr, error=str(payload))
                await session.commit()
                repo_action = "mark_kkt_error"
                branch = "kkt_error"
            else:
                branch = f"status_{status_code}"
                logging.info("Ferma webhook: unhandled status=%r for invoice=%s", status_code, invoice_id)

        return web.json_response({
            "ok": True,
            "branch": branch,
            "invoice_id": invoice_id,
            "receipt_id": receipt_id,
            "has_ofd_url": bool(ofd_url),
            "notified": bool(notified),
            "repo_action": repo_action,
        })

    return ferma_callback


# --- (опционально) Старая версия под app.add_routes(routes) ---

def ferma_webhook_route(async_session_factory: sessionmaker) -> web.RouteTableDef:
    routes = web.RouteTableDef()
    handler = make_ferma_callback_handler(async_session_factory)
    path = os.getenv("FERMA_CALLBACK_PATH", "/webhook/ferma")

    @routes.post(path)
    async def _route(request: web.Request):
        return await handler(request)

    return routes