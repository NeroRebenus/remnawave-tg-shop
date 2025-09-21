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

def make_ferma_callback_handler(async_session_factory: sessionmaker):
    cidrs = _trusted_cidrs()

    async def _notify_user_with_receipt(session, bot, pr, ofd_url) -> bool:
        """
        pr.payment_id — это наш «связующий» идентификатор (обычно YooKassa payment_id).
        Поищем пользователя по двум полям: yookassa_payment_id ИЛИ provider_payment_id='yookassa'.
        """
        from sqlalchemy import select, or_, and_
        from db.models import Payment

        # 1) по yookassa_payment_id
        q1 = select(Payment).where(Payment.yookassa_payment_id == pr.payment_id)
        res = (await session.execute(q1)).scalars().first()
        if not res:
            # 2) fallback: по паре (provider='yookassa', provider_payment_id=...)
            try:
                q2 = select(Payment).where(
                    and_(
                        getattr(Payment, "provider", None) == "yookassa",
                        getattr(Payment, "provider_payment_id", None) == pr.payment_id
                    )
                )
                res = (await session.execute(q2)).scalars().first()
            except Exception:
                res = None

        if not res or not getattr(res, "user_id", None):
            logging.warning("Ferma webhook: cannot map receipt to user (payment_id=%s)", pr.payment_id)
            return False

        try:
            await bot.send_message(
                chat_id=res.user_id,
                text=f"🧾 Ваш чек сформирован: {ofd_url}",
                disable_web_page_preview=True,
            )
            return True
        except Exception:
            logging.exception("Failed to send receipt message to user_id=%s", res.user_id)
            return False

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

        logging.info("Ferma webhook IN: status=%r invoice=%r receipt=%r ofd=%r", status_code, invoice_id, receipt_id, ofd_url)

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
                logging.info("Ferma webhook: unknown invoice/receipt (ignored). invoice=%s receipt=%s", invoice_id, receipt_id)
                return web.json_response({"ok": True, "ignored": True, "reason": "unknown_invoice"})

            # Нормализуем код статуса
            code = None
            try:
                code = int(status_code)
            except Exception:
                s = str(status_code).strip().upper()
                m = {"NEW": 0, "PROCESSED": 1, "CONFIRMED": 2, "KKT_ERROR": 3}
                code = m.get(s)

            branch = "unknown"
            user_notified = False
            repo_action = None

            if code == 2:  # CONFIRMED
                await repo.mark_confirmed(pr, ofd_url)
                await session.commit()
                repo_action = "mark_confirmed"
                branch = "confirmed"

                bot = request.app.get("bot")
                if not bot:
                    logging.warning("Ferma webhook: app['bot'] is missing; cannot notify user")
                elif ofd_url:
                    user_notified = await _notify_user_with_receipt(session, bot, pr, ofd_url)

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
            "user_notified": bool(user_notified),
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
