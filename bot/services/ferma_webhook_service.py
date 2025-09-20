from __future__ import annotations
import ipaddress
import logging
import os
from typing import List, Callable, Awaitable
from aiohttp import web
from sqlalchemy.orm import sessionmaker
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from db.dal import payment_dal, user_dal
from db.repositories.receipts_repo import ReceiptsRepo
from db.models import ReceiptStatus

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

# --- Фабрика хендлера ДЛЯ app.router.add_post ---

def make_ferma_callback_handler(async_session_factory: sessionmaker):
    async def _notify_user(session, bot: Bot, invoice_id: str, ofd_url: Optional[str]):
        user_id: Optional[int] = None
        try:
            pay = await payment_dal.get_payment_by_provider_payment_id(
                session,
                provider="yookassa",
                provider_payment_id=invoice_id,
            )
            if pay:
                user_id = getattr(pay, "user_id", None)
        except Exception:
            log.exception("Cannot resolve user by provider_payment_id=%s", invoice_id)
            return

        if not user_id:
            log.warning("No user_id for invoice_id=%s, skip notify", invoice_id)
            return

        text = "✅ Чек сформирован и зарегистрирован в ФНС."
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Открыть чек", url=ofd_url)]] if ofd_url else []
        )
        try:
            await bot.send_message(user_id, text, reply_markup=kb, disable_web_page_preview=True)
        except Exception as e:
            log.warning("Failed to send receipt message to user %s: %s", user_id, e)

    async def ferma_callback(request: web.Request) -> web.Response:
        bot: Bot = request.app["bot"]
        async_session_factory_local: sessionmaker = request.app["async_session_factory"]

        try:
            payload: Dict[str, Any] = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid_json"}, status=400)

        data = payload.get("Data") or {}
        invoice_id = data.get("InvoiceId")
        receipt_id = data.get("ReceiptId")
        status_code = data.get("StatusCode")
        device = data.get("Device") or {}
        ofd_url = device.get("OfdReceiptUrl")

        if not invoice_id:
            return web.json_response({"ok": False, "error": "missing_invoice_id"}, status=400)

        async with async_session_factory_local() as session:
            repo = ReceiptsRepo(session)
            pr = await repo.get_by_invoice_id(invoice_id)
            if not pr:
                log.warning("Unknown invoice_id in Ferma callback: %s", invoice_id)
                return web.json_response({"ok": True, "ignored": True})

            if status_code == 2:  # CONFIRMED
                await repo.mark_confirmed(pr, ofd_url)
                log.info("CONFIRMED invoice_id=%s receipt_id=%s", invoice_id, receipt_id)
                await _notify_user(session, bot, invoice_id, ofd_url)
            elif status_code == 1:  # PROCESSED
                await repo.mark_processed(pr)
            elif status_code == 3:  # KKT_ERROR
                await repo.mark_kkt_error(pr, error=str(payload))
            # 0/прочие — просто принимаем

        return web.json_response({"ok": True})

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