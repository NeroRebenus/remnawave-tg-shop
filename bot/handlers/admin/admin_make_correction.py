# bot/handlers/admin_make_correction.py
from __future__ import annotations

import re
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from config.settings import Settings
from bot.services.ferma_ofd_service import FermaClient, FermaError

log = logging.getLogger(__name__)
router = Router()

# -------------------- ЛЕНИВЫЙ FermaClient (без DI) --------------------
_ferma_client_singleton: Optional[FermaClient] = None

def get_ferma_client() -> FermaClient:
    global _ferma_client_singleton
    if _ferma_client_singleton is None:
        _ferma_client_singleton = FermaClient()
        log.info("FermaClient singleton created for admin_make_correction handler")
    return _ferma_client_singleton

# -------------------- Парсер key=value --------------------
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

# -------------------- /make_correction --------------------
@router.message(Command("make_correction"))
async def make_correction_cmd(msg: Message, settings: Settings):
    """
    Оформляет чек КОРРЕКЦИИ (IncomeCorrection) по Ferma Cloud KKT.
    Поддерживает PaymentItems (тег 1215) через pi="2:100,7:50".

    Пример:
    /make_correction amount=100 desc="Отмена зачета аванса" \
      invoice=INV-CORR-001 corr_type=SELF corr_desc="Ошибочный чек" \
      corr_receipt_date=17.01.21 corr_receipt_id=3144062149 \
      pi="2:100" vat=VatNo tax=SimpleIn

    Параметры:
      amount=...          — сумма чека (обяз.)
      desc="..."          — наименование позиции (обяз.)
      invoice=...         — твой идентификатор операции (если не задан — генерируем)
      email=... phone=... — контакты покупателя (необяз.)
      vat=Vat20|Vat10|Vat0|VatNo
      tax=SimpleIn|Common|SimpleInOut|Unified|UnifiedAgricultural|Patent
      type=...            — PaymentType на ПОЗИЦИИ (по умолчанию 4 — полный расчёт)
      method=...          — PaymentMethod на ПОЗИЦИИ (по умолчанию 4 — полный расчёт)
      measure=PIECE|...   — единица измерения (ФФД 1.2)
      internet=1|0        — признак интернет-торговли
      bill="https://..."  — адрес расчётов
      tz=3                — часовой пояс ККТ (1..11), 0/нет — не отправлять
      inn=...             — переопределить ИНН (иначе из .env)
      pid=...             — PaymentIdentifiers (опционально)

      corr_type=SELF|INSTRUCTION           — тип коррекции (обяз. для коррекции)
      corr_desc="..."                      — описание причины (обяз.)
      corr_receipt_date=DD.MM.YY           — дата ошибочного чека (обяз., формат как в доке)
      corr_receipt_id=NNNNNNNN             — номер ошибочного чека (обяз.)

      pi="2:100,7:50"                      — PaymentItems (тег 1215), формат "Тип:Сумма" через запятую:
                                             1 — полная предоплата (100%)
                                             2 — предоплата (аванс)
                                             3 — частичная предоплата
                                             4 — полный расчёт
                                             5 — частичный расчёт и кредит
                                             6 — передача в кредит
                                             7 — оплата кредита
    """
    # Доступ только админам
    if msg.from_user and msg.from_user.id not in settings.ADMIN_IDS:
        return await msg.reply("⛔ Доступ только для администраторов.")

    args = msg.text.split(maxsplit=1)
    if len(args) == 1:
        return await msg.reply(
            "Использование:\n"
            "/make_correction amount=100 desc=\"Отмена зачёта аванса\" "
            "invoice=INV-CORR-001 corr_type=SELF corr_desc=\"Ошибочный чек\" "
            "corr_receipt_date=17.01.21 corr_receipt_id=3144062149 "
            "pi=\"2:100\" [vat=VatNo] [tax=SimpleIn] [type=4] [method=4] [measure=PIECE] "
            "[internet=1] [bill=https://...] [tz=3] [inn=...] [pid=...]"
        )

    kv = _parse_kv(args[1])

    # обязательные поля
    amount_str = kv.get("amount")
    desc = kv.get("desc") or kv.get("description")
    if not amount_str or not desc:
        return await msg.reply("Нужно указать минимум: amount=... и desc=\"...\"")

    try:
        amount = float(amount_str.replace(",", "."))
        if amount <= 0:
            raise ValueError
    except Exception:
        return await msg.reply("amount должен быть положительным числом.")

    invoice = kv.get("invoice") or f"INV-CORR-{int(datetime.now(timezone.utc).timestamp())}"
    email = kv.get("email")
    phone = kv.get("phone")
    pid = kv.get("pid")

    # -------- CorrectionInfo (обязательные для коррекции) --------
    corr_type = (kv.get("corr_type") or "").strip().upper()
    corr_desc = kv.get("corr_desc") or kv.get("correction_desc")
    corr_receipt_date = kv.get("corr_receipt_date") or kv.get("correction_receipt_date")
    corr_receipt_id = kv.get("corr_receipt_id") or kv.get("correction_receipt_id")

    if corr_type not in {"SELF", "INSTRUCTION"}:
        return await msg.reply("corr_type обязателен и должен быть SELF или INSTRUCTION.")
    if not corr_desc:
        return await msg.reply("corr_desc обязателен (описание причины коррекции).")
    if not corr_receipt_date:
        return await msg.reply("corr_receipt_date обязателен (например, 17.01.21).")
    if not corr_receipt_id:
        return await msg.reply("corr_receipt_id обязателен (номер ошибочного чека).")

    # не жёстко валидируем дату, просто слегка нормализуем
    corr_info = {
        "Type": corr_type,
        "Description": corr_desc,
        "ReceiptDate": corr_receipt_date,  # формат Ferma "DD.MM.YY"
        "ReceiptId": corr_receipt_id,
    }

    # -------- overrides --------
    overrides: dict[str, Any] = {}

    # ключевое: это именно чек КОРРЕКЦИИ
    overrides["force_type"] = "IncomeCorrection"

    # базовые поля (необязательные, берутся из .env при отсутствии)
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

    # CorrectionInfo (важно!)
    overrides["correction_info"] = corr_info

    # PaymentItems: строка вида "2:100,7:350"
    pi_raw = kv.get("pi") or kv.get("paymentitems") or kv.get("payment_items")
    if pi_raw:
        pi_list: list[dict] = []
        for part in pi_raw.split(","):
            part = part.strip()
            if not part or ":" not in part:
                continue
            t_str, s_str = part.split(":", 1)
            try:
                pt = int(t_str.strip())
                ssum = float(s_str.strip().replace(",", "."))
                if ssum > 0:
                    pi_list.append({"PaymentType": pt, "Sum": round(ssum, 2)})
            except Exception:
                continue
        if pi_list:
            overrides["payment_items"] = pi_list

    # -------- отправка --------
    ferma = get_ferma_client()
    try:
        send_res = await ferma.send_income_receipt(
            invoice_id=invoice,
            amount=amount,
            description=desc,
            buyer_email=email,
            buyer_phone=phone,
            payment_identifiers=pid,
            overrides=overrides,
        )
    except FermaError as e:
        log.exception("Admin make_correction: Ferma error")
        return await msg.reply(f"❌ Ошибка Ferma: {e}")
    except Exception as e:
        log.exception("Admin make_correction: unexpected error")
        return await msg.reply(f"❌ Не удалось отправить чек коррекции: {e}")

    receipt_id = send_res.get("receipt_id")
    inv_back = send_res.get("invoice_id") or invoice
    return await msg.reply(
        "✅ Чек КОРРЕКЦИИ поставлен в очередь в Ферме.\n"
        f"• InvoiceId: <code>{inv_back}</code>\n"
        f"• ReceiptId: <code>{receipt_id}</code>\n\n"
        "Статус и ссылка ОФД придут колбэком Ferma; если колбэк не придёт — сработает фолбэк-опрос.",
        parse_mode="HTML",
    )