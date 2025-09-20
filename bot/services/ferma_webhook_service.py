from __future__ import annotations
import ipaddress
import logging
import os
from typing import List, Callable, Awaitable

from aiohttp import web
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from db.repositories.receipts_repo import ReceiptsRepo
from db.models import ReceiptStatus, Payment

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
    # allow all if list пустой
    if not cidrs:
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

def make_ferma_callback_handler(async_session_factory: sessionmaker) -> Callable[[web.Request], Awaitable[web.Response]]:
    """
    Вернёт готовый aiohttp-хендлер под app.router.add_post(PATH, handler)
    """
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

        if not invoice_id:
            return web.json_response({"ok": False, "error": "missing_invoice_id"}, status=400)

        async with async_session_factory() as session:
            repo = ReceiptsRepo(session)
            pr = await repo.get_by_invoice_id(invoice_id)
            if not pr:
                # возможно, ретрай от Ferma по старому invoice_id — просто проигнорим
                return web.json_response({"ok": True, "ignored": True, "reason": "unknown_invoice"})

            if status_code == 2:  # CONFIRMED
                # запомним, была ли ссылка ранее (чтобы избежать дублей сообщений)
                already_had_url = bool(pr.ofd_receipt_url)
                await repo.mark_confirmed(pr, ofd_url)
                log.info("Ferma CONFIRMED: invoice_id=%s receipt_id=%s ofd=%s", invoice_id, receipt_id, ofd_url)

                # 🧾 Попробуем отправить ссылку пользователю (если появилась впервые)
                try:
                    if ofd_url and not already_had_url:
                        bot = request.app.get("bot")
                        if bot is not None:
                            # PaymentReceipt.payment_id == YooKassa payment.id
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
                except Exception as e:
                    # не валим обработку, просто логируем
                    log.warning("Failed to notify user about receipt: %s", e, exc_info=True)

            elif status_code == 1:  # PROCESSED
                await repo.mark_processed(pr)
                log.info("Ferma PROCESSED: invoice_id=%s receipt_id=%s", invoice_id, receipt_id)

            elif status_code == 3:  # KKT_ERROR
                await repo.mark_kkt_error(pr, error=str(payload))
                log.warning("Ferma KKT_ERROR: invoice_id=%s receipt_id=%s payload=%s", invoice_id, receipt_id, payload)

            else:  # 0 NEW или иной
                log.info("Ferma status=%s invoice_id=%s receipt_id=%s", status_code, invoice_id, receipt_id)

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
