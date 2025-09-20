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

        from sqlalchemy import select
        from db.models import Payment  # для поиска user_id по yookassa_payment_id

        async with async_session_factory() as session:
            repo = ReceiptsRepo(session)

            pr = None
            # 1) Пытаемся найти по ReceiptId (надежнее всего)
            if receipt_id:
                pr = await repo.get_by_receipt_id(receipt_id)
            # 2) Если не нашли — пробуем по InvoiceId
            if not pr and invoice_id:
                pr = await repo.get_by_invoice_id(invoice_id)

            if not pr:
                logging.info("Ferma webhook: unknown invoice/receipt (ignored). invoice=%s receipt=%s", invoice_id, receipt_id)
                return web.json_response({"ok": True, "ignored": True, "reason": "unknown_invoice"})

            code = int(status_code) if isinstance(status_code, (int, float, str)) and str(status_code).isdigit() else None

            if code == 2:  # CONFIRMED
                already_had_url = bool(pr.ofd_receipt_url)
                await repo.mark_confirmed(pr, ofd_url)

                # отправим ссылку пользователю, если появилась впервые
                try:
                    if ofd_url and not already_had_url:
                        bot = request.app.get("bot")
                        if bot:
                            q = await session.execute(
                                select(Payment).where(Payment.yookassa_payment_id == pr.payment_id)
                            )
                            payment_row = q.scalars().first()
                            if payment_row and payment_row.user_id:
                                await bot.send_message(
                                    payment_row.user_id,
                                    f"🧾 Ваш чек сформирован: {ofd_url}",
                                    disable_web_page_preview=True,
                                )
                except Exception:
                    logging.exception("Failed to notify user about receipt")

            elif code == 1:  # PROCESSED
                await repo.mark_processed(pr)
            elif code == 3:  # KKT_ERROR
                await repo.mark_kkt_error(pr, error=str(payload))
            else:
                # NEW/unknown — просто залогируем
                logging.info("Ferma webhook: status=%s invoice=%s receipt=%s", status_code, invoice_id, receipt_id)

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
