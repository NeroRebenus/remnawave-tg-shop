# bot/services/fiscalization_service.py
from __future__ import annotations

import asyncio
import logging
from typing import Optional, Dict, Any

from aiogram import Bot
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from db.repositories.receipts_repo import ReceiptsRepo
from db.models import ReceiptStatus, Payment
from bot.services.ferma_ofd_service import FermaClient, FermaError
from config.settings import get_settings

log = logging.getLogger("fiscalization")

# ----------------------------- Ferma client singleton -----------------------------

_ferma_client: Optional[FermaClient] = None

def _get_ferma_client() -> FermaClient:
    """
    Ленивая инициализация клиента Ferma из Settings.
    Не создаём FermaConfig напрямую — клиент соберёт его сам из Settings.
    """
    global _ferma_client
    if _ferma_client is None:
        _ferma_client = FermaClient(settings=get_settings())
    return _ferma_client


# ----------------------------- Public entrypoint -----------------------------

async def fiscalize_on_yookassa_succeeded(
    async_session_factory: sessionmaker,
    payload: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Главная точка входа. Вызывается из обработчика вебхука YooKassa
    при событии payment.succeeded (после всех ваших проверок и DB-коммитов).

    Делает:
      1) Идемпотентность по payment_id (YooKassa)
      2) Отправляет чек в Ferma (Income)
      3) Ставит фоновый fallback-опрос статуса на случай, если колбэк Ferma не придёт
    """
    try:
        obj = payload.get("object") or {}
        status = obj.get("status")
        if status != "succeeded":
            return {"ok": True, "skipped": True, "reason": "not_succeeded"}

        payment_id = _as_str(obj.get("id"))
        if not payment_id:
            return {"ok": False, "error": "missing_payment_id"}

        amount_str = _as_str(((obj.get("amount") or {}).get("value")))
        amount = _to_float(amount_str, default=0.0)
        description = _truncate_label(obj.get("description") or f"Оплата заказа {payment_id}")

        cust = (obj.get("receipt") or {}).get("customer") or {}
        buyer_email = _safe_strip(cust.get("email"))
        buyer_phone = _safe_strip(cust.get("phone"))

        # NEW: извлекаем metadata, чтобы понять источник платежа и маршрут для отправки чека  # <<<
        metadata = obj.get("metadata") or {}  # <<<
        source = (metadata.get("source") or "").strip().lower()  # <<<
        s = get_settings()
        # Если платёж создан через админскую ссылку И задан LOG_CHAT_ID — шлём чек туда  # <<<
        route_chat_id: Optional[int] = None  # <<<
        try:  # <<<
            if source == "admin_link" and getattr(s, "LOG_CHAT_ID", None):  # <<<
                route_chat_id = int(s.LOG_CHAT_ID)  # <<<
        except Exception:  # <<<
            route_chat_id = None  # <<<

        # Если email/phone отсутствуют, можно подставить дефолтный email из настроек
        if not buyer_email and getattr(s, "YOOKASSA_DEFAULT_RECEIPT_EMAIL", None):
            buyer_email = s.YOOKASSA_DEFAULT_RECEIPT_EMAIL

        client = _get_ferma_client()

        # --- Шаг 1: идемпотентность по payment_id
        async with async_session_factory() as session:
            repo = ReceiptsRepo(session)

            pr = await repo.get_by_payment_id(payment_id)
            if pr and pr.status in (ReceiptStatus.SENT, ReceiptStatus.PROCESSED, ReceiptStatus.CONFIRMED):
                log.info(
                    "Fiscalization skipped (already done): payment_id=%s status=%s",
                    payment_id, pr.status.value
                )
                return {
                    "ok": True,
                    "duplicate": True,
                    "status": pr.status.value,
                    "receipt_id": pr.receipt_id,
                    "invoice_id": pr.invoice_id,
                    "ofd_url": pr.ofd_receipt_url
                }

            # invoice_id фиксируем на payment_id YK (если запись уже была — не меняем)
            invoice_id = pr.invoice_id if pr else payment_id

            if not pr:
                pr = await repo.create_new(
                    payment_id=payment_id,
                    invoice_id=invoice_id,
                    amount=amount,
                    description=description,
                    email=buyer_email,
                    phone=buyer_phone,
                )

            # --- Шаг 2: отправка чека в Ferma
            try:
                send_result = await client.send_income_receipt(
                    invoice_id=invoice_id,
                    amount=amount,
                    description=description,
                    buyer_email=buyer_email,
                    buyer_phone=buyer_phone,
                    payment_identifiers=payment_id,  # для удобства сверки
                )

                receipt_id = send_result.get("receipt_id")
                ferma_invoice_id = send_result.get("invoice_id") or invoice_id

                # Сохраняем ReceiptId и (если есть) настоящий InvoiceId Ferma
                await repo.mark_sent(pr, receipt_id=receipt_id, new_invoice_id=ferma_invoice_id)

                # ВАЖНО: фиксируем изменения до прихода колбэка
                await session.commit()

                result = {"ok": True, "receipt_id": receipt_id, "invoice_id": ferma_invoice_id}

            except FermaError as e:
                await session.rollback()
                log.exception("Ferma error on send_income_receipt. invoice_id=%s", invoice_id)
                await repo.mark_failed(pr, error=str(e))
                # фиксируем статус ошибки, чтобы не болталась транзакция
                await session.commit()
                return {"ok": False, "error": "ferma_send_failed", "detail": str(e)}

            except Exception as e:
                await session.rollback()
                log.exception("Unexpected error on send_income_receipt. invoice_id=%s", invoice_id)
                await repo.mark_failed(pr, error=str(e))
                await session.commit()
                return {"ok": False, "error": "unexpected_send_error", "detail": str(e)}


        # --- Шаг 3: fallback-опрос статуса (параллельно, вне транзакции)
        cfg = client.cfg
        # NEW: передаём маршрут (route_chat_id) в фоновую задачу                 # <<<
        asyncio.create_task(_fallback_poll_status(async_session_factory, client, invoice_id, cfg, route_chat_id))  # <<<

        return result

    except Exception as e:
        log.exception("fiscalize_on_yookassa_succeeded fatal error")
        return {"ok": False, "error": "fatal", "detail": str(e)}


# ----------------------------- Background fallback -----------------------------

async def _fallback_poll_status(
    async_session_factory: sessionmaker,
    client: FermaClient,
    invoice_id: str,
    cfg,
    route_chat_id: Optional[int] = None,   # NEW: куда слать чек, если это admin_link   # <<<
) -> None:
    """
    Периодически опрашивает Ferma по invoice_id и обновляет запись в БД,
    если колбэк не пришёл. Останавливается на CONFIRMED/KKT_ERROR
    или после исчерпания попыток.
    """
    try:
        # начальная задержка
        await asyncio.sleep(int(getattr(cfg, "fallback_delay_sec", 180)))
        retries = int(getattr(cfg, "fallback_retries", 5))
        interval = int(getattr(cfg, "fallback_interval_sec", 180))

        for _ in range(max(retries, 0)):
            data = await client.check_status(invoice_id=invoice_id)
            status_code = data.get("StatusCode")
            device = data.get("Device") or {}
            ofd_url = device.get("OfdReceiptUrl")

            async with async_session_factory() as session:
                repo = ReceiptsRepo(session)
                pr = await repo.get_by_invoice_id(invoice_id)
                if not pr:
                    # локальной записи нет — прекращаем
                    return

                # уже закрыто колбэком или прошлой итерацией?
                if pr.status in (ReceiptStatus.CONFIRMED, ReceiptStatus.KKT_ERROR):
                    return

                if status_code == 2:  # CONFIRMED
                    had_url_before = bool(pr.ofd_receipt_url)
                    await repo.mark_confirmed(pr, ofd_url)
                    log.info("Fallback confirmed: invoice_id=%s ofd=%s", invoice_id, ofd_url)

                    # Уведомляем: в LOG_CHAT_ID для admin_link, иначе пользователю
                    try:
                        from sqlalchemy import select, and_
                        from db.models import Payment
                        s = get_settings()
                        bot = Bot(token=s.BOT_TOKEN)
                        try:
                            q = await session.execute(select(Payment).where(Payment.yookassa_payment_id == pr.payment_id))
                            payment_row = q.scalars().first()
                            if not payment_row and hasattr(Payment, "provider") and hasattr(Payment, "provider_payment_id"):
                                q2 = await session.execute(
                                    select(Payment).where(and_(Payment.provider == "yookassa", Payment.provider_payment_id == pr.payment_id))
                                )
                                payment_row = q2.scalars().first()

                            # Куда отправлять:
                            target_chat_id: Optional[int] = None
                            if route_chat_id:
                                # Если это админская ссылка и LOG_CHAT_ID задан — шлём в лог-чат
                                target_chat_id = route_chat_id
                            else:
                                # Иначе как раньше — пользователю (если есть user_id)
                                if payment_row and payment_row.user_id:
                                    target_chat_id = int(payment_row.user_id)

                            if target_chat_id and ofd_url:
                                await bot.send_message(
                                    target_chat_id,
                                    f"🧾 Чек сформирован: {ofd_url}",
                                    disable_web_page_preview=True
                                )
                            else:
                                log.warning(
                                    "Receipt ready, but nowhere to send (route_chat_id=%s, user_id=%s).",
                                    route_chat_id, getattr(payment_row, "user_id", None)
                                )
                        finally:
                            await bot.session.close()
                    except Exception:
                        log.exception("Fallback: failed to notify about receipt")
                    return

                elif status_code == 1:  # PROCESSED (в обработке)
                    await repo.mark_processed(pr)

                elif status_code == 3:  # KKT_ERROR
                    await repo.mark_kkt_error(pr, error=str(data))
                    log.warning("Fallback KKT_ERROR: invoice_id=%s data=%s", invoice_id, data)
                    return

                # status_code == 0 (NEW) или неизвестен — ждём дальше

            await asyncio.sleep(interval)

    except asyncio.CancelledError:
        # спокойно выходим, если нас отменили при выключении приложения
        return
    except Exception:
        log.exception("fallback poll failed for invoice_id=%s", invoice_id)


# ----------------------------- Helpers -----------------------------

def _as_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        s = str(v)
    except Exception:
        return None
    return s

def _safe_strip(v: Any) -> Optional[str]:
    s = _as_str(v)
    return s.strip() if s else None

def _to_float(s: Optional[str], default: float = 0.0) -> float:
    if s is None:
        return default
    try:
        return float(str(s).replace(",", "."))
    except Exception:
        return default

def _truncate_label(label: str, limit: int = 128) -> str:
    try:
        return (label or "Оплата услуг")[:limit]
    except Exception:
        return "Оплата услуг"