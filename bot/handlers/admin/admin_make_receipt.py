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

# (опционально) локальная запись о чеке; если у тебя нет этого репозитория — блок ниже просто не сработает
try:
    from db.repositories.receipts_repo import ReceiptsRepo  # noqa: F401
    HAS_RECEIPTS_REPO = True
except Exception:
    HAS_RECEIPTS_REPO = False

log = logging.getLogger(__name__)
router = Router()

# ============================ Ленивая инициализация FermaClient (синглтон) ============================

_ferma_client_singleton: Optional[FermaClient] = None

def get_ferma_client() -> FermaClient:
    """
    Создаёт один FermaClient на процесс и переиспользует его в каждом вызове.
    Никаких зависимостей через DI не требуется.
    """
    global _ferma_client_singleton
    if _ferma_client_singleton is None:
        _ferma_client_singleton = FermaClient()
        log.info("FermaClient singleton created in admin_make_receipt")
    return _ferma_client_singleton

# ======================================= Парсер key=value =======================================

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
    return bool(s) and s.strip().lower() in {"1", "true", "yes", "on", "y", "да", "истина"}

# ========================================= Хендлер команды =========================================

@router.message(Command("make_receipt"))
async def make_receipt_cmd(
    msg: Message,
    settings: Settings,                  # у тебя уже есть middleware, который прокидывает Settings
    async_session_factory: sessionmaker, # и фабрика сессий — оставляем как есть
):
    """
    Создать чек в Ферме с произвольными параметрами.

    Пример:
      /make_receipt amount=400 desc="Оплата X" [invoice=INV-1] [email=user@ex.com] [phone=+7999...] [pid=...]
                    [vat=Vat20|Vat10|Vat0|VatNo] [tax=SimpleIn|Common|...] [type=4] [method=4] [measure=PIECE]
                    [internet=1] [bill="https://..."] [tz=3] [callback=https://...] [inn=7841465198]
                    [cashless_sum=400] [cashless_info="Полная оплата безналичными"]
                    [pi="2:50,7:350"]   # PaymentItems (тег 1215): PaymentType:Sum через запятую
    """
    # --- доступ только админам ---
    if msg.from_user and msg.from_user.id not in settings.ADMIN_IDS:
        await msg.reply("⛔ Доступ только для администраторов.")
        return

    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) == 1:
        await msg.reply(
            "Использование:\n"
            "/make_receipt amount=400 desc=\"Оплата X\" [invoice=INV-1] [email=...] [phone=...] [pid=...]\n"
            "[vat=VatNo] [tax=SimpleIn] [type=4] [method=4] [measure=PIECE] [internet=1]\n"
            "[bill=https://...] [tz=3] [callback=https://...] [inn=7841465198]\n"
            "[cashless_sum=400] [cashless_info=\"Полная оплата безналичными\"]\n"
            "[pi=\"2:50,7:350\"]  # PaymentItems (тег 1215): 'PaymentType:Sum' через запятую"
        )
        return

    kv = _parse_kv(parts[1])

    # обязательные
    amount_str = kv.get("amount")
    desc = kv.get("desc") or kv.get("description")
    if not amount_str or not desc:
        await msg.reply("Нужно указать минимум: amount=... и desc=\"...\"")
        return

    try:
        amount = float(amount_str.replace(",", "."))
        if amount <= 0:
            raise ValueError
    except Exception:
        await msg.reply("amount должен быть положительным числом.")
        return

    invoice = kv.get("invoice") or f"INV-{int(datetime.now(timezone.utc).timestamp())}"
    email = kv.get("email")
    phone = kv.get("phone")
    pid   = kv.get("pid")  # PaymentIdentifiers (опционально)

    # overrides (необязательные)
    overrides: Dict[str, Any] = {}
    if "vat" in kv: overrides["vat"] = kv["vat"]
    if "tax" in kv or "taxation" in kv or "taxation_system" in kv:
        overrides["taxation_system"] = kv.get("tax") or kv.get("taxation") or kv.get("taxation_system")
    if "type" in kv:
        try: overrides["payment_type"] = int(kv["type"])
        except Exception: pass
    if "method" in kv:
        try: overrides["payment_method"] = int(kv["method"])
        except Exception: pass
    if "measure" in kv: overrides["measure"] = kv["measure"]
    if "internet" in kv: overrides["is_internet"] = _boolish(kv["internet"])
    if "bill" in kv: overrides["bill_address"] = kv["bill"]
    if "tz" in kv:
        try: overrides["timezone"] = int(kv["tz"])
        except Exception: pass
    if "callback" in kv: overrides["callback_url"] = kv["callback"]
    if "inn" in kv: overrides["inn"] = kv["inn"]
    if "cashless_sum" in kv:
        try:
            overrides["cashless_sum"] = float(kv["cashless_sum"].replace(",", "."))
        except Exception:
            pass
    if "cashless_info" in kv:
        overrides["cashless_info"] = kv["cashless_info"]

    # PaymentItems (тег 1215) — строка вида "2:50,7:350"
    pi_raw = kv.get("pi") or kv.get("paymentitems") or kv.get("payment_items")
    if pi_raw:
        pi_list: list[dict] = []
        for part in pi_raw.split(","):
            part = part.strip()
            if not part or ":" not in part:
                continue
            t_str, s_str = part.split(":", 1)
            try:
                ptype = int(t_str.strip())
                ssum = float(s_str.strip().replace(",", "."))
                if ssum > 0:
                    pi_list.append({"PaymentType": ptype, "Sum": round(ssum, 2)})
            except Exception:
                continue
        if pi_list:
            overrides["payment_items"] = pi_list

    # --- отправка чека в Ферму (ленивый singleton) ---
    ferma_client = get_ferma_client()
    try:
        send_res = await ferma_client.send_income_receipt(
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
        await msg.reply(f"❌ Ошибка Ferma: {e}")
        return
    except Exception as e:
        log.exception("Admin make_receipt: unexpected error")
        await msg.reply(f"❌ Не удалось отправить чек: {e}")
        return

    # --- (опционально) записать локально о попытке чека ---
    if HAS_RECEIPTS_REPO:
        try:
            async with async_session_factory() as session:
                repo = ReceiptsRepo(session)  # type: ignore[name-defined]
                pr = await repo.get_by_invoice_id(invoice)
                if not pr:
                    await repo.create_new(
                        payment_id=invoice,
                        invoice_id=invoice,
                        amount=amount,
                        description=desc,
                        email=email,
                        phone=phone,
                    )
        except Exception:
            log.exception("Admin make_receipt: failed to write local receipt record")

    # --- ответ администратору ---
    receipt_id = send_res.get("receipt_id")
    inv_back   = send_res.get("invoice_id") or invoice
    await msg.reply(
        "✅ Чек поставлен в очередь в Ферме.\n"
        f"• InvoiceId: <code>{inv_back}</code>\n"
        f"• ReceiptId: <code>{receipt_id}</code>\n\n"
        "Ссылку ОФД пришлёт вебхук Ferma; если колбэк не придёт — сработает фолбэк-опрос.",
        parse_mode="HTML",
    )