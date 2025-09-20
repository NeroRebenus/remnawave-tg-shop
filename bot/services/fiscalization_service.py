# app/services/fiscalization.py
from __future__ import annotations
import asyncio
import logging
from typing import Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from db.receipts.db import get_session
from db.repositories.receipts_repo import ReceiptsRepo
from bot.services.ferma_ofd_service import FermaClient, FermaConfig, FermaError
from db.models import ReceiptStatus

log = logging.getLogger("fiscalization")

ferma_client = FermaClient(FermaConfig())  # один на процесс; aiohttp-сессия внутри

async def handle_payment_succeeded(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Главная точка входа: вызывается из вебхука YooKassa после валидации.
    Делает всё по шагам 1-4-5: идемпотентность, отправка чека, постановка fallback-пула.
    """
    obj = payload.get("object") or {}
    payment_id = obj.get("id")
    status = obj.get("status")
    if status != "succeeded":
        return {"ok": True, "skipped": True, "reason": "not_succeeded"}

    amount = float(obj.get("amount", {}).get("value", 0.0))
    description = obj.get("description") or f"Оплата заказа {payment_id}"
    customer = (obj.get("receipt") or {}).get("customer") or {}
    buyer_email = customer.get("email")
    buyer_phone = customer.get("phone")

    # шаг 1: идемпотентность
    async with get_session() as session:
        repo = ReceiptsRepo(session)
        existing = await repo.get_by_payment_id(payment_id)
        if existing and existing.status in (ReceiptStatus.SENT, ReceiptStatus.PROCESSED, ReceiptStatus.CONFIRMED):
            log.info("Payment already fiscalized: payment_id=%s status=%s", payment_id, existing.status.value)
            return {"ok": True, "duplicate": True, "status": existing.status.value, "receipt_id": existing.receipt_id, "ofd_url": existing.ofd_receipt_url}

        invoice_id = payment_id if not existing else existing.invoice_id  # если запись была, не меняем invoice_id
        if not existing:
            existing = await repo.create_new(
                payment_id=payment_id,
                invoice_id=invoice_id,
                amount=amount,
                description=description,
                email=buyer_email,
                phone=buyer_phone,
            )

        # шаг 2-4: отправка чека в Ferma
        try:
            receipt_id = await ferma_client.send_income_receipt(
                invoice_id=invoice_id,
                amount=amount,
                description=description,
                buyer_email=buyer_email,
                buyer_phone=buyer_phone,
                payment_identifiers=payment_id,
            )
            await repo.mark_sent(existing, receipt_id=receipt_id)
            result = {"ok": True, "receipt_id": receipt_id, "invoice_id": invoice_id}
        except FermaError as e:
            log.exception("Ferma error on send_income_receipt. invoice_id=%s", invoice_id)
            await repo.mark_failed(existing, error=str(e))
            return {"ok": False, "error": "ferma_send_failed", "detail": str(e)}
        except Exception as e:  # сетевые/пр.
            log.exception("Unexpected error on send_income_receipt. invoice_id=%s", invoice_id)
            await repo.mark_failed(existing, error=str(e))
            return {"ok": False, "error": "unexpected_send_error", "detail": str(e)}

        # шаг 5: fallback-опрос статуса (если callback не придёт)
        cfg = ferma_client.cfg
        asyncio.create_task(_fallback_poll_status(invoice_id, existing.id, cfg))

        return result

async def _fallback_poll_status(invoice_id: str, local_id: int, cfg: FermaConfig):
    # ждём стартовую задержку
    try:
        await asyncio.sleep(cfg.fallback_delay_sec)
        for i in range(cfg.fallback_retries):
            data = await ferma_client.check_status(invoice_id=invoice_id)
            status_code = data.get("StatusCode")
            device = data.get("Device") or {}
            ofd_url = device.get("OfdReceiptUrl")
            async with get_session() as session:
                repo = ReceiptsRepo(session)
                pr = await repo.get_by_invoice_id(invoice_id)
                if not pr:
                    return
                # уже закрыто где-то callback’ом?
                if pr.status in (ReceiptStatus.CONFIRMED, ReceiptStatus.KKT_ERROR):
                    return
                if status_code == 2:  # CONFIRMED
                    await repo.mark_confirmed(pr, ofd_url)
                    log.info("Fallback confirmed: invoice_id=%s ofd=%s", invoice_id, ofd_url)
                    return
                elif status_code == 1:  # PROCESSED
                    await repo.mark_processed(pr)
                elif status_code == 3:  # KKT_ERROR
                    await repo.mark_kkt_error(pr, error=str(data))
                    log.warning("Fallback KKT_ERROR: invoice_id=%s data=%s", invoice_id, data)
                    return
                # иначе продолжаем попытки
            await asyncio.sleep(cfg.fallback_interval_sec)
    except Exception:
        log.exception("fallback poll failed for invoice_id=%s", invoice_id)