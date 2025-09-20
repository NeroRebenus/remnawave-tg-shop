from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
import logging, os

import aiohttp

try:
    # Предпочитаем использовать общий singleton настроек проекта
    from config.settings import Settings, get_settings
except Exception:  # pragma: no cover
    Settings = None  # type: ignore
    get_settings = None  # type: ignore

log = logging.getLogger("ferma")


@dataclass(slots=True)
class FermaConfig:
    # Базовые
    base_url: str
    login: str
    password: str
    inn: str

    # Параметры чека/позиции
    taxation_system: str
    is_internet: bool
    bill_address: Optional[str]
    vat: str
    payment_type: int
    payment_method: int
    measure: str
    timezone_num: Optional[int]  # None => не отправлять

    # Вебхуки
    callback_url_full: Optional[str]

    # Fallback-пулы статуса (используются внешней логикой)
    fallback_delay_sec: int
    fallback_retries: int
    fallback_interval_sec: int

    @classmethod
    def from_settings(cls, s: "Settings") -> "FermaConfig":
        """
        Собирает конфиг из Settings. Все поля приведены к нужным типам,
        а callback_url_full формируется через вычисляемые свойства Settings.
        """
        # Timezone: 0 -> None (не передавать), иначе 1..11
        tz_raw = getattr(s, "FERMA_TIMEZONE", 0) or 0
        tz_num: Optional[int] = int(tz_raw) if int(tz_raw) != 0 else None

        return cls(
            base_url=s.FERMA_BASE_URL or "https://ferma.ofd.ru",
            login=s.FERMA_LOGIN or "",
            password=s.FERMA_PASSWORD or "",
            inn=s.FERMA_INN or "",
            taxation_system=s.FERMA_TAXATION_SYSTEM or "SimpleIn",
            is_internet=bool(s.FERMA_IS_INTERNET),
            bill_address=s.FERMA_BILL_ADDRESS or None,
            vat=s.FERMA_VAT or "VatNo",
            payment_type=int(s.FERMA_PAYMENT_TYPE or 4),
            payment_method=int(s.FERMA_PAYMENT_METHOD or 4),
            measure=s.FERMA_MEASURE or "PIECE",
            timezone_num=tz_num,
            callback_url_full=getattr(s, "ferma_full_callback_url", None),
            fallback_delay_sec=int(s.FERMA_STATUS_FALLBACK_DELAY_SEC or 180),
            fallback_retries=int(s.FERMA_STATUS_FALLBACK_RETRIES or 5),
            fallback_interval_sec=int(s.FERMA_STATUS_FALLBACK_INTERVAL_SEC or 180),
        )

    @classmethod
    def from_env_or_defaults(cls) -> "FermaConfig":
        """
        На случай, если Settings недоступен (не должен случиться в твоём проекте),
        подстрахуемся простыми дефолтами.
        """
        # Импорт здесь, чтобы не тянуть os для нормального пути с Settings
        import os

        tz_raw = os.getenv("FERMA_TIMEZONE", "0")
        tz_num: Optional[int] = int(tz_raw) if tz_raw and tz_raw != "0" else None

        base_url = os.getenv("FERMA_BASE_URL", "https://ferma.ofd.ru")
        webhook_base = os.getenv("WEBHOOK_BASE_URL") or None
        callback_path = os.getenv("FERMA_CALLBACK_PATH", "/webhook/ferma")
        callback_url_full = f"{webhook_base.rstrip('/')}{callback_path}" if webhook_base else None

        return cls(
            base_url=base_url,
            login=os.getenv("FERMA_LOGIN", "") or "",
            password=os.getenv("FERMA_PASSWORD", "") or "",
            inn=os.getenv("FERMA_INN", "") or "",
            taxation_system=os.getenv("FERMA_TAXATION_SYSTEM", "SimpleIn"),
            is_internet=(os.getenv("FERMA_IS_INTERNET", "true").lower() == "true"),
            bill_address=os.getenv("FERMA_BILL_ADDRESS") or None,
            vat=os.getenv("FERMA_VAT", "VatNo"),
            payment_type=int(os.getenv("FERMA_PAYMENT_TYPE", "4")),
            payment_method=int(os.getenv("FERMA_PAYMENT_METHOD", "4")),
            measure=os.getenv("FERMA_MEASURE", "PIECE"),
            timezone_num=tz_num,
            callback_url_full=callback_url_full,
            fallback_delay_sec=int(os.getenv("FERMA_STATUS_FALLBACK_DELAY_SEC", "180")),
            fallback_retries=int(os.getenv("FERMA_STATUS_FALLBACK_RETRIES", "5")),
            fallback_interval_sec=int(os.getenv("FERMA_STATUS_FALLBACK_INTERVAL_SEC", "180")),
        )


class FermaError(RuntimeError):
    def __init__(self, http_status: int, payload: Any):
        super().__init__(f"Ferma API failed: HTTP {http_status} {payload}")
        self.http_status = http_status
        self.payload = payload

log = logging.getLogger("ferma.client")

class FermaClient:
    """
    Асинхронный клиент Ferma (авторизация, отправка чека, статус).
    Конфиг берём из Settings, либо из окружения (fallback).
    Сетевые вызовы выполняются с ретраями (экспоненциальный бэкофф).
    """

    def __init__(
        self,
        cfg: Optional[FermaConfig] = None,
        session: Optional[aiohttp.ClientSession] = None,
        settings: Optional["Settings"] = None,
    ):
        if cfg is not None:
            self.cfg = cfg
        else:
            s = settings
            if s is None and get_settings is not None:
                # Подтянем общий singleton, если есть
                try:
                    s = get_settings()
                except Exception:
                    s = None
            self.cfg = FermaConfig.from_settings(s) if s is not None else FermaConfig.from_env_or_defaults()

        self._session = session
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

    # --------------------- session/token helpers ---------------------

    async def _session_get(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _ensure_token(self):
        """
        Кешируем токен; обновляем заранее (за 60 сек до истечения).
        """
        if self._token and self._token_expiry and (self._token_expiry - datetime.now(timezone.utc)).total_seconds() > 60:
            return

        body = {"Login": self.cfg.login, "Password": self.cfg.password}
        data = await self._post_json("/api/Authorization/CreateAuthToken", body, use_token=False)
        if data.get("Status") != "Success":
            raise FermaError(200, data)
        d = data.get("Data") or {}
        self._token = d.get("AuthToken")
        exp = d.get("ExpirationDateUtc")
        self._token_expiry = _parse_ferma_utc(exp)

    # --------------------- public API ---------------------


    async def send_income_receipt(
        self,
        invoice_id: str,
        amount: float,
        description: str,
        buyer_email: str | None = None,
        buyer_phone: str | None = None,
        payment_identifiers: str | None = None,
    ) -> dict:
        """
        POST /api/kkt/cloud/receipt  (Ferma)
        Формируем простой чек прихода (Type="Income").
        Возвращает: {"receipt_id": str, "invoice_id": str | None}
        """
        s = self.cfg  # настройки (get_settings)

        # --- строгая подготовка INN ---
        inn = (str(getattr(s, "FERMA_INN", "")).strip()
            or str(os.getenv("FERMA_INN", "")).strip())
        if not inn.isdigit() or len(inn) not in (10, 12):
            log.error("FERMA_INN is empty/invalid. Got: %r (settings=%r, env=%r)",
                    inn, getattr(s, "FERMA_INN", None), os.getenv("FERMA_INN"))
            raise FermaError(400, {"Status":"Failed","Error":{"Code":1007,"Message":f"ENV FERMA_INN is invalid: {inn!r}"}})


        if not inn.isdigit() or len(inn) not in (10, 12):
            # Лог + корректное формирование FermaError (status, payload)
            log.error("FERMA_INN invalid or not set. Got: %r", inn)
            raise FermaError(
                400,
                {
                    "Status": "Failed",
                    "Error": {
                        "Code": 1007,
                        "Message": f"ENV FERMA_INN is invalid: {inn!r}",
                    },
                },
            )


        # --- опциональная группа касс (на тесте часто нужна 555) ---
        group_code = getattr(s, "FERMA_GROUP_CODE", None)
        if group_code is not None:
            group_code = str(group_code).strip() or None

        # --- собираем CustomerReceipt ---
        customer_receipt = {
            "Items": [
                {
                    "Label": (description or f"Оплата заказа {invoice_id}")[:128],
                    "Price": float(amount),
                    "Quantity": 1,
                    "Amount": float(amount),
                    "Vat": getattr(s, "FERMA_VAT", "VatNo"),
                    "Measure": getattr(s, "FERMA_MEASURE", "PIECE"),
                    "PaymentMethod": int(getattr(s, "FERMA_PAYMENT_METHOD", 4)),
                    "PaymentType": int(getattr(s, "FERMA_PAYMENT_TYPE", 4)),
                }
            ],
            "TotalSum": float(amount),
            "TaxationSystem": getattr(s, "FERMA_TAXATION_SYSTEM", None),
            "BillAddress": getattr(s, "FERMA_BILL_ADDRESS", None) or None,
            "Email": buyer_email or None,
            "Phone": buyer_phone or None,
        }
        customer_receipt = {k: v for k, v in customer_receipt.items() if v is not None}

        # --- корневой Request ---
        request_obj = {
            "Inn": inn,
            "Type": "Income",
            "InvoiceId": invoice_id,
            "CustomerReceipt": customer_receipt,
            "CallbackUrl": (
                (getattr(s, "PUBLIC_BASE_URL", "").rstrip("/") + getattr(s, "FERMA_CALLBACK_PATH", "/webhook/ferma"))
                if getattr(s, "PUBLIC_BASE_URL", None) else None
            ),
        }
        if group_code:
            request_obj["GroupCode"] = group_code  # ← важно для тестовой группы касс (555)

        # internet-флаг по желанию
        if str(getattr(s, "FERMA_IS_INTERNET", "false")).lower() == "true":
            request_obj["IsInternet"] = True

        if payment_identifiers:
            request_obj["Data"] = {"PaymentIdentifiers": [payment_identifiers]}

        payload = {"Request": request_obj}

        # --- отладочный лог фактического тела запроса ---
        try:
            import json as _json
            log.info("Ferma SEND receipt: inn=%s, group=%s, invoice=%s, amount=%.2f, payload=%s",
                    inn, group_code, invoice_id, float(amount),
                    _json.dumps(payload, ensure_ascii=False)[:1200])
        except Exception:
            pass
        log.info("Ferma SEND: Inn=%s, InvoiceId=%s, Amount=%.2f", inn, invoice_id, float(amount))

        # --- вызов Ferma ---
        resp = await self._post_json("/api/kkt/cloud/receipt", payload, use_token=True)
        data = (resp or {}).get("Data") or resp or {}

        receipt_id = data.get("ReceiptId")
        ferma_invoice_id = data.get("InvoiceId")
        if not receipt_id:
            raise FermaError(f"Invalid response from Ferma: {resp}")

        return {"receipt_id": receipt_id, "invoice_id": ferma_invoice_id}





    async def check_status(self, *, invoice_id: str | None = None, receipt_id: str | None = None) -> dict:
        """
        POST /api/kkt/cloud/status — вернуть Data c полями StatusCode/Device.OfdReceiptUrl и т.п.
        Можно передавать либо ReceiptId, либо InvoiceId (одно из них обязательно).
        """
        if not invoice_id and not receipt_id:
            raise FermaError("check_status requires either invoice_id or receipt_id")

        payload = {
            **({"InvoiceId": invoice_id} if invoice_id else {}),
            **({"ReceiptId": receipt_id} if receipt_id else {}),
        }
        resp = await self._post_json("/api/kkt/cloud/status", payload, use_token=True)
        data = (resp or {}).get("Data") or {}
        return data


    # --------------------- build payload helpers ---------------------

    def _make_item(self, label: str, amount: float) -> Dict[str, Any]:
        return {
            "Label": (label or "Оплата услуг")[:128],
            "Price": round(float(amount), 2),
            "Quantity": 1.0,
            "Amount": round(float(amount), 2),
            "Vat": self.cfg.vat,
            "PaymentMethod": self.cfg.payment_method,
            "PaymentType": self.cfg.payment_type,
            "Measure": self.cfg.measure,
        }

    def _build_receipt_payload(
        self,
        *,
        type_: str,
        invoice_id: str,
        items: List[Dict[str, Any]],
        amount: float,
        buyer_email: Optional[str],
        buyer_phone: Optional[str],
        payment_identifiers: str,
        customer_name: Optional[str],
    ) -> Dict[str, Any]:
        cr: Dict[str, Any] = {
            "TaxationSystem": self.cfg.taxation_system,
            "CashlessPayments": [{
                "PaymentSum": round(float(amount), 2),
                "PaymentMethodFlag": "1",
                "PaymentIdentifiers": payment_identifiers,
                "AdditionalInformation": "Полная оплата безналичными",
            }],
            "PaymentType": self.cfg.payment_type,
            "Items": items,
        }
        if buyer_email:
            cr["Email"] = buyer_email
        if buyer_phone:
            cr["Phone"] = buyer_phone
        if self.cfg.is_internet:
            cr["IsInternet"] = True
        if self.cfg.bill_address:
            cr["BillAddress"] = self.cfg.bill_address
        if self.cfg.timezone_num is not None:
            cr["Timezone"] = self.cfg.timezone_num
        if customer_name:
            cr["ClientInfo"] = {"Name": customer_name}

        req: Dict[str, Any] = {
            "Inn": self.cfg.inn,
            "Type": type_,
            "InvoiceId": invoice_id,
            "CustomerReceipt": cr,
        }

        # Абсолютный URL колбэка, если задан
        if self.cfg.callback_url_full:
            req["CallbackUrl"] = self.cfg.callback_url_full

        return {"Request": req}

    # --------------------- low level HTTP with retries ---------------------

    async def _post_json(self, path: str, json_body: Dict[str, Any], *, use_token: bool) -> Dict[str, Any]:
        sess = await self._session_get()
        url = self.cfg.base_url.rstrip("/") + path
        params: Dict[str, str] = {}
        if use_token:
            if not self._token:
                await self._ensure_token()
            params["AuthToken"] = self._token  # type: ignore

        headers = {"Content-Type": "application/json"}

        attempts = 5
        backoff = 0.6
        for i in range(1, attempts + 1):
            try:
                async with sess.post(url, params=params, json=json_body, headers=headers) as r:
                    data = await _safe_json(r)
                    if 200 <= r.status < 300:
                        return data
                    # retry на перегрузку/тайм-ауты/429
                    if r.status in (429, 500, 502, 503, 504):
                        raise aiohttp.ClientResponseError(r.request_info, r.history, status=r.status, message="server error")
                    raise FermaError(r.status, data)
            except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, aiohttp.ServerTimeoutError, aiohttp.ClientResponseError):
                if i == attempts:
                    raise
                await asyncio.sleep(backoff)
                backoff *= 2


# --------------------- helpers ---------------------

async def _safe_json(resp: aiohttp.ClientResponse) -> Dict[str, Any]:
    try:
        return await resp.json(content_type=None)
    except Exception:
        t = await resp.text()
        return {"Status": "Failed", "_raw": t}

def _parse_ferma_utc(exp_iso: Optional[str]) -> datetime:
    """
    Ferma отдаёт ExpirationDateUtc в ISO. Приведём к UTC.
    Если формат неожиданно неверный — считаем, что токен вот-вот истекает,
    чтобы получить новый при следующем вызове.
    """
    if not exp_iso:
        return datetime.now(timezone.utc) + timedelta(seconds=1)
    try:
        dt = datetime.fromisoformat(exp_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc) + timedelta(seconds=1)