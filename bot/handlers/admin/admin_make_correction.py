# bot/handlers/admin_make_correction.py
from __future__ import annotations

import re
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from config.settings import Settings, get_settings
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
    Чек КОРРЕКЦИИ (Type=IncomeCorrection) + поддержка PaymentItems (тег 1215) через pi="2:100,7:50".

    Пример:
    /make_correction amount=100 desc="Отмена зачета аванса" \
      invoice=INV-CORR-001 corr_type=SELF corr_desc="Ошибочный чек" \
      corr_receipt_date=17.01.21 pi="2:100" vat=VatNo tax=SimpleIn

    Важно:
      - corr_receipt_id *опционален* (если указан — включим в CorrectionInfo).

    Параметры (основные):
      amount=...          — сумма чека (обяз.)
      desc="..."          — наименование позиции (обяз.)
      invoice=...         — идентификатор операции (если не задан — генерируем)
      email=... phone=... — контакты покупателя (опционально)
      pid=...             — PaymentIdentifiers (опционально)

      vat=Vat20|Vat10|Vat0|VatNo
      tax=SimpleIn|Common|SimpleInOut|Unified|UnifiedAgricultural|Patent
      type=...            — PaymentType на позиции (по умолчанию 4)
      method=...          — PaymentMethod на позиции (по умолчанию 4)
      measure=PIECE|...   — единица измерения (по умолчанию PIECE)
      internet=1|0        — признак интернет-торговли
      bill="https://..."  — адрес расчётов
      tz=3                — часовой пояс ККТ (1..11), 0/нет — не отправлять
      inn=...             — переопределить ИНН
      callback=https://.. — переопределить CallbackUrl

      corr_type=SELF|INSTRUCTION           — тип коррекции (обяз.)
      corr_desc="..."                      — причина (обяз.)
      corr_receipt_date=DD.MM.YY           — дата ошибочного чека (обяз.)
      corr_receipt_id=NNNNNNNN             — номер ошибочного чека (НЕобязателен)

      pi="2:100,7:50"                      — PaymentItems (тег 1215): "Тип:Сумма"
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
            "corr_receipt_date=17.01.21 [corr_receipt_id=3144062149] "
            "pi=\"2:100\" [vat=VatNo] [tax=SimpleIn] [type=4] [method=4] [measure=PIECE] "
            "[internet=1] [bill=https://...] [tz=3] [inn=...] [pid=...] [callback=https://...]"
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

    # -------- CorrectionInfo (corr_receipt_id — опционален) --------
    corr_type = (kv.get("corr_type") or "").strip().upper()
    corr_desc = kv.get("corr_desc") or kv.get("correction_desc")
    corr_receipt_date = kv.get("corr_receipt_date") or kv.get("correction_receipt_date")
    corr_receipt_id = kv.get("corr_receipt_id") or kv.get("correction_receipt_id")  # ← опционально

    if corr_type not in {"SELF", "INSTRUCTION"}:
        return await msg.reply("corr_type обязателен и должен быть SELF или INSTRUCTION.")
    if not corr_desc:
        return await msg.reply("corr_desc обязателен (описание причины коррекции).")
    if not corr_receipt_date:
        return await msg.reply("corr_receipt_date обязателен (например, 17.01.21).")

    correction_info = {
        "Type": corr_type,
        "Description": corr_desc,
        "ReceiptDate": corr_receipt_date,  # формат Ferma "DD.MM.YY"
    }
    if corr_receipt_id:
        correction_info["ReceiptId"] = corr_receipt_id

    # -------- дефолты из Settings, c возможностью override --------
    s = get_settings()

    inn = (kv.get("inn") or getattr(s, "FERMA_INN", "") or "").strip()
    if not inn:
        return await msg.reply("FERMA_INN не задан (ни в команде, ни в настройках).")

    taxation = (kv.get("tax") or kv.get("taxation") or kv.get("taxation_system")
                or getattr(s, "FERMA_TAXATION_SYSTEM", "") or "").strip() or None
    vat = kv.get("vat") or getattr(s, "FERMA_VAT", "VatNo")
    measure = kv.get("measure") or getattr(s, "FERMA_MEASURE", "PIECE")

    try:
        payment_type_item = int(kv.get("type") or getattr(s, "FERMA_PAYMENT_TYPE", 4))
    except Exception:
        payment_type_item = 4
    try:
        payment_method_item = int(kv.get("method") or getattr(s, "FERMA_PAYMENT_METHOD", 4))
    except Exception:
        payment_method_item = 4

    is_internet = _boolish(kv.get("internet")) if "internet" in kv else bool(getattr(s, "FERMA_IS_INTERNET", False))

    bill_address = (kv.get("bill") or getattr(s, "FERMA_BILL_ADDRESS", None) or "").strip() or None

    tz_val = kv.get("tz", getattr(s, "FERMA_TIMEZONE", 0))
    try:
        tz_int = int(tz_val) if tz_val is not None else 0
    except Exception:
        tz_int = 0
    timezone_num = tz_int if 1 <= tz_int <= 11 else None

    callback_url = kv.get("callback") or getattr(s, "ferma_full_callback_url", None)
    if not callback_url and getattr(s, "WEBHOOK_BASE_URL", None):
        base = s.WEBHOOK_BASE_URL.rstrip("/")
        path = getattr(s, "ferma_callback_path", "/webhook/ferma")
        callback_url = f"{base}{path}"

    # -------- CashlessPayments (по умолчанию вся сумма безналом) --------
    cashless_block = [{
        "PaymentSum": round(float(amount), 2),
        "PaymentMethodFlag": "1",                      # безнал
        "PaymentIdentifiers": pid or invoice,          # удобно для трассировки
        "AdditionalInformation": "Полная оплата безналичными",
    }]

    # -------- Позиции --------
    item = {
        "Label": (desc or f"Отмена/коррекция {invoice}")[:128],
        "Price": round(float(amount), 2),
        "Quantity": 1.0,
        "Amount": round(float(amount), 2),
        "Vat": vat,
        "Measure": measure,
        "PaymentMethod": payment_method_item,
        "PaymentType": payment_type_item,
    }
    items = [item]

    # -------- PaymentItems (тег 1215) из pi="2:100,7:50" --------
    payment_items = None
    pi_raw = kv.get("pi") or kv.get("paymentitems") or kv.get("payment_items")
    if pi_raw:
        tmp: list[dict] = []
        for part in pi_raw.split(","):
            part = part.strip()
            if not part or ":" not in part:
                continue
            t_str, s_str = part.split(":", 1)
            try:
                pt = int(t_str.strip())
                ssum = float(s_str.strip().replace(",", "."))
                if ssum > 0:
                    tmp.append({"PaymentType": pt, "Sum": round(ssum, 2)})
            except Exception:
                continue
        if tmp:
            payment_items = tmp

    # -------- CustomerReceipt --------
    customer_receipt: dict[str, Any] = {
        "Items": items,
        "CashlessPayments": cashless_block,
        "PaymentType": payment_type_item,  # поле чека (как в примере Ferma)
        "CorrectionInfo": correction_info,
    }
    if taxation:
        customer_receipt["TaxationSystem"] = taxation
    if bill_address:
        customer_receipt["BillAddress"] = bill_address
    if email:
        customer_receipt["Email"] = email
    if phone:
        customer_receipt["Phone"] = phone
    if payment_items:
        customer_receipt["PaymentItems"] = payment_items
    if timezone_num is not None:
        customer_receipt["Timezone"] = timezone_num

    # -------- Корневой Request --------
    request_obj: dict[str, Any] = {
        "Inn": inn,
        "Type": "IncomeCorrection",
        "InvoiceId": invoice,
        "CustomerReceipt": customer_receipt,
    }
    if callback_url:
        request_obj["CallbackUrl"] = callback_url
    if is_internet:
        request_obj["IsInternet"] = True
    if pid:
        request_obj["Data"] = {"PaymentIdentifiers": [pid]}

    payload = {"Request": request_obj}

    # -------- Отправка через низкоуровневый FermaClient._post_json --------
    ferma = get_ferma_client()
    try:
        # у клиента должен быть _post_json(path, json_body, use_token=True)
        resp = await ferma._post_json("/api/kkt/cloud/receipt", payload, use_token=True)  # type: ignore[attr-defined]
    except FermaError as e:
        log.exception("Admin make_correction: Ferma error")
        return await msg.reply(f"❌ Ошибка Ferма: {e}")
    except AttributeError as e:
        log.exception("Admin make_correction: FermaClient lacks _post_json")
        return await msg.reply("❌ В текущем FermaClient нет метода _post_json. Обнови bot.services.ferma_ofd_service.")
    except Exception as e:
        log.exception("Admin make_correction: unexpected error")
        return await msg.reply(f"❌ Не удалось отправить чек коррекции: {e}")

    data = (resp or {}).get("Data") or resp or {}
    receipt_id = data.get("ReceiptId")
    inv_back = data.get("InvoiceId") or invoice
    if not receipt_id:
        return await msg.reply(f"❌ Ferma не вернула ReceiptId. Ответ: {resp}")

    return await msg.reply(
        "✅ Чек КОРРЕКЦИИ поставлен в очередь в Ферме.\n"
        f"• InvoiceId: <code>{inv_back}</code>\n"
        f"• ReceiptId: <code>{receipt_id}</code>\n\n"
        "Статус и ссылка ОФД придут колбэком Ferma; если колбэк не придёт — сработает фолбэк-опрос.",
        parse_mode="HTML",
    )