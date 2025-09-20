from __future__ import annotations
import ipaddress
import logging
import os
import json
from typing import List, Callable, Awaitable, Any, Optional

from aiohttp import web
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from db.repositories.receipts_repo import ReceiptsRepo
from db.models import ReceiptStatus, Payment

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
    # сначала попытка прямой int
    try:
        return int(status_code)
    except Exception:
        pass
    s = str(status_code).strip().upper()
    mapping = {
        "CONFIRMED": 2,
        "PROCESSED": 1,
        "KKT_ERROR": 3,
        "NEW": 0,
    }
    if s in mapping:
        return mapping[s]
    # иногда присылают "2 " или " 2"
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

# --- Фабрика хендлера ДЛЯ app.router.add_post ---

def make_ferma_callback_handler(async_session_factory: sessionmaker) -> Callable[[web.Request], Awaitable[web.Response]]:
    """
    Вернёт готовый aiohttp-хендлер под app.router.add_post(PATH, handler)
    """
    cidrs = _trusted_cidrs()

    async def ferma_callback(request: web.Request) -> web.Response:
        if not _ip_allowed(request, cidrs):
            # логируем явный отказ
            logging.info("Ferma webhook forbidden (CIDR). peer=%s", request.transport.get_extra_info("peername"))
            return web.json_response({"ok": False, "error": "forbidden"}, status=403)

        try:
            payload = await request.json()
        except Exception:
            logging.info("Ferma webhook invalid JSON")
            return web.json_response({"ok": False, "error": "invalid_json"}, status=400)

        # поддерживаем оба стиля: с корнем Data или плоский
        data_block = payload.get("Data") if isinstance(payload, dict) else None
        # вытаскиваем значения с запасом
        status_code_raw = _get(payload, "Data", "StatusCode", default=_get(payload, "StatusCode"))
        invoice_id = _get(payload, "Data", "InvoiceId", default=_get(payload, "InvoiceId"))
        receipt_id = _get(payload, "Data", "ReceiptId", default=_get(payload, "ReceiptId"))
        device = _get(payload, "Data", "Device", default=_get(payload, "Device", default={})) or {}
        ofd_url = device.get("OfdReceiptUrl")

        status_code = _as_int_status(status_code_raw)

        # входной лог (и в модульный, и в root — чтобы точно увидеть при базовой конфигурации)
        log.info("Ferma webhook IN: status=%s invoice=%s receipt=%s ofd=%s payload=%s",
                 status_code_raw, invoice_id, receipt_id, ofd_url, _short(payload))
        logging.info("Ferma webhook IN: status=%s invoice=%s receipt=%s", status_code_raw, invoice_id, receipt_id)

        if not invoice_id and not receipt_id:
            return web.json_response({"ok": False, "error": "missing_invoice_and_receipt"}, status=400)

        branch = "unknown"
        user_notified = False
        repo_action = None

        async with async_session_factory() as session:
            repo = ReceiptsRepo(session)

            pr = None
            # 1) пробуем по invoice_id
            if invoice_id:
                pr = await repo.get_by_invoice_id(invoice_id)
            # 2) если не нашли — попробуем по receipt_id (вдруг заранее отправили и callback пришёл с ним)
            if not pr and receipt_id:
                try:
                    pr = await repo.get_by_receipt_id(receipt_id)
                    if pr and not invoice_id:
                        invoice_id = pr.invoice_id
                except Exception:
                    pr = None

            if not pr:
                branch = "ignored_unknown_invoice"
                logging.info("Ferma webhook: unknown invoice/receipt (ignored). invoice=%s receipt=%s", invoice_id, receipt_id)
                return web.json_response({"ok": True, "ignored": True, "reason": "unknown_invoice"})

            # нормализуем код; если не распознали — просто залогируем
            if status_code is None:
                branch = "logged_unknown_status"
                log.warning("Ferma webhook: unknown status_code=%r for invoice=%s", status_code_raw, invoice_id)
                return web.json_response({"ok": True, "ignored": True, "reason": "unknown_status", "raw": status_code_raw})

            # обработка по статусам
            if status_code == 2:          # CONFIRMED
                already_had_url = bool(pr.ofd_receipt_url)
                await repo.mark_confirmed(pr, ofd_url)
                repo_action = "mark_confirmed"
                branch = "confirmed"
                log.info("Ferma CONFIRMED: invoice_id=%s receipt_id=%s ofd=%s", invoice_id, receipt_id, ofd_url)
                logging.info("Ferma CONFIRMED for invoice=%s", invoice_id)

                # Попробуем отправить чек пользователю (если появилась ссылка впервые)
                try:
                    if ofd_url and not already_had_url:
                        bot = request.app.get("bot")
                        if bot is None:
                            log.warning("Ferma webhook: bot missing in app context; cannot notify user")
                            logging.info("Ferma webhook: bot missing in app context")
                        else:
                            q = await session.execute(
                                select(Payment).where(Payment.yookassa_payment_id == pr.payment_id)
                            )
                            payment_row = q.scalars().first()
                            if payment_row and payment_row.user_id:
                                await bot.send_message(
                                    chat_id=payment_row.user_id,
                                    text=f"🧾 Ваш чек сформирован: {ofd_url}",
                                    disable_web_page_preview=True,
                                )
                                user_notified = True
                            else:
                                log.warning("Ferma webhook: payment_row or user_id not found for pr.payment_id=%s", pr.payment_id)
                except Exception as e:
                    log.warning("Failed to notify user about receipt: %s", e, exc_info=True)

            elif status_code == 1:        # PROCESSED
                await repo.mark_processed(pr)
                repo_action = "mark_processed"
                branch = "processed"
                log.info("Ferma PROCESSED: invoice_id=%s receipt_id=%s", invoice_id, receipt_id)

            elif status_code == 3:        # KKT_ERROR
                await repo.mark_kkt_error(pr, error=str(payload))
                repo_action = "mark_kkt_error"
                branch = "kkt_error"
                log.warning("Ferma KKT_ERROR: invoice_id=%s receipt_id=%s payload=%s", invoice_id, receipt_id, _short(payload))

            else:                          # 0 NEW или иной
                branch = f"status_{status_code}"
                log.info("Ferma status=%s invoice_id=%s receipt_id=%s", status_code, invoice_id, receipt_id)

        # Возвращаем развёрнутую диагностическую инфу (попадёт в access-лог размером ответа)
        return web.json_response({
            "ok": True,
            "invoice_id": invoice_id,
            "receipt_id": receipt_id,
            "status_code_raw": status_code_raw,
            "status_code": status_code,
            "branch": branch,
            "repo_action": repo_action,
            "user_notified": user_notified,
            "has_ofd_url": bool(ofd_url),
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
