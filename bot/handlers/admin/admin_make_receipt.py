# bot/handlers/admin_make_receipt.py
from __future__ import annotations

import re
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
from sqlalchemy.orm import sessionmaker

from config.settings import Settings
from bot.services.ferma_ofd_service import FermaClient, FermaError
from db.repositories.receipts_repo import ReceiptsRepo

log = logging.getLogger(__name__)
router = Router()

# -------------------- Ленивая инициализация FermaClient (синглтон) --------------------

_ferma_client_singleton: Optional[FermaClient] = None

def get_ferma_client() -> FermaClient:
    global _ferma_client_singleton
    if _ferma_client_singleton is None:
        # Берёт конфиг из Settings автоматически внутри FermaClient
        _ferma_client_singleton = FermaClient()
        log.info("FermaClient singleton created for admin_make_receipt handler")
    return _ferma_client_singleton

# -------------------- Утилиты парсинга --------------------

# простой парсер key=value (поддержка кавычек)
_KV_RE = re.compile(
    r"""
    (?P<key>[A-Za-z_][A-Za-z0-9_]*)
    \s*=\s*
    (?:
        "(?P<qval>[^"]*)"        # "value"
      | '(?P<sval>[^']*)'        # 'value'
      | (?P<val>[^ \t]+)         # bare
    )
    """,
    re.VERBOSE,
)

def _parse_kv(s: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for m in _KV_RE.finditer(s or ""):
        key = m.group("key").lower()
        val = m.group("qval") or m.group("sval") or m.group("val") or ""
        out[key] = val
    return out

def _boolish(s: Optional[str]) -> bool:
    if not s:
        return False
    return s.strip().lower() in {"1", "true", "yes", "on", "y", "да", "истина"}

# -------------------- Хендлер команды --------------------

@router.message(Command("make_receipt"))
async def make_receipt_cmd(
    msg: Message,
    settings: Settings,                     # уже прокидывается вашим DI
    async_session_factory: sessionmaker,    # уже прокидывается вашим DI
):
    """
    /make_receipt amount=400 desc="Оплата X" invoice=inv-123 email=user@ex.com phone=+7999...
                   vat=Vat20 tax=SimpleIn method=4 type=4 measure=PIECE
                   internet=1 bill="https://shop..." tz=3 callback=https://...
                   inn=7841465198 pid=3061...  # PaymentIdentifiers (необязательно)

    Обязательные минимум: amount, desc.
    Если invoice не указан — сгенерируем INV-<timestamp>.
    Остальные поля подхватываются из .env/Settings, но могут быть переопределены.
    """
    # --- доступ только для админов ---
    if not (msg.from_user and msg.from_user.id in settings.ADMIN_IDS):
        return await msg.reply("⛔ Доступ только для администраторов.")

    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) == 1:
        return await msg.reply(
            "Использование:\n"
            "/make_receipt amount=400 desc=\"Оплата X\" [invoice=...] [email=...] [phone=...]\n"
            "[vat=VatNo|Vat20|Vat10|Vat0] [tax=SimpleIn] [type=4] [method=4] [measure=PIECE]\n"
            "[internet=1] [bill=https://...] [tz=3] [callback=https://...] [inn=7841465198] [pid=...]"
        )

    kv = _parse_kv(parts[1])

    # --- обязательные ---
    amount_str = kv.get("amount")
    desc = kv.get("desc") or kv.get("description")
    if not amount_str or not desc:
        return await msg.reply("Нужно указать минимум: amount=... и desc=\"...\"")

    try:
        amount = float(str(amount_str).replace(",", "."))
        if amount <= 0:
            raise ValueError
    except Exception:
        return await msg.reply("amount должен быть положительным числом.")

    invoice = kv.get("invoice") or f"INV-{int(datetime.now(timezone.utc).timestamp())}"
    email = kv.get("email")
    phone = kv.get("phone")
    pid = kv.get("pid")  # PaymentIdentifiers (опц.)

    # --- overrides (всё опционально) ---
    overrides: Dict[str, Any] = {}
    if "vat" in kv: overrides["vat"] = kv["vat"]
    if "tax" in kv or "taxation" in kv or "taxation_system" in kv:
        overrides["taxation_system"] = kv.get("tax") or kv.get("taxation") or kv.get("taxation_system")
    if "type" in kv: overrides["payment_type"] = int(kv["type"])
    if "method" in kv: overrides["payment_method"] = int(kv["method"])
    if "measure" in kv: overrides["measure"] = kv["measure"]
    if "internet" in kv: overrides["is_internet"] = _boolish(kv["internet"])
    if "bill" in kv: overrides["bill_address"] = kv["bill"]
    if "tz" in kv: overrides["timezone"] = int(kv["tz"])
    if "callback" in kv: overrides["callback_url"] = kv["callback"]
    if "inn" in kv: overrides["inn"] = kv["inn"]
    if "group" in kv: overrides["group_code"] = int(kv["group"])  # если у вас добавлена поддержка Group в клиенте

    # --- отправляем чек через ленивый FermaClient ---
    client = get_ferma_client()
    try:
        send_res = await client.send_income_receipt(
            invoice_id=invoice,
            amount=amount,
            description=desc,
            buyer_email=email,
            buyer_phone=phone,
            payment_identifiers=pid,
            overrides=overrides or None,
        )
    except FermaError as e:
        log.exception("Admin make_receipt: Ferma error")
        return await msg.reply(f"❌ Ошибка Ferma: {e}")
    except Exception as e:
        log.exception("Admin make_receipt: unexpected error")
        return await msg.reply(f"❌ Не удалось отправить чек: {e}")

    # --- на всякий случай, сохраним локальную запись, если её нет ---
    try:
        async with async_session_factory() as session:
            repo = ReceiptsRepo(session)
            pr = await repo.get_by_invoice_id(invoice)
            if not pr:
                await repo.create_new(
                    payment_id=invoice,           # для админ-команды payment_id может не существовать — используем invoice
                    invoice_id=invoice,
                    amount=amount,
                    description=desc,
                    email=email,
                    phone=phone,
                )
    except Exception:
        log.exception("Admin make_receipt: failed to write local receipt record (ignored)")

    receipt_id = send_res.get("receipt_id")
    inv_back = send_res.get("invoice_id") or invoice

    return await msg.reply(
        "✅ Чек поставлен в очередь в Ферме.\n"
        f"• InvoiceId: <code>{inv_back}</code>\n"
        f"• ReceiptId: <code>{receipt_id}</code>\n\n"
        "Статус и ссылку ОФД пришлёт вебхук Ferma. Если колбэк не придёт — сработает фолбэк-опрос.",
        parse_mode="HTML",
    )
