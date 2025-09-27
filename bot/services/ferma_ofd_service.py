from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
import os

import aiohttp

from config.settings import get_settings, Settings

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
        try:
            tz_int = int(tz_raw)
        except Exception:
            tz_int = 0
        tz_num: Optional[int] = tz_int if tz_int != 0 else None

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
        tz_raw = os.getenv("FERMA_TIMEZONE", "0")
        tz_num: Optional[int] = None
        try:
            tz_int = int(tz_raw) if tz_raw else 0
            tz_num = tz_int if tz_int != 0 else None
        except Exception:
            tz_num = None

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
            s = get_settings()
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

    async def _auth(self) -> None:
        """
        Логин в Ferma и сохранение токена с временем истечения.
        """
        sess = await self._session_get()
        url = self.cfg.base_url.rstrip("/") + "/api/Authorization/CreateAuthToken"
        body = {"Login": self.cfg.login, "Password": self.cfg.password}

        async with sess.post(url, json=body) as r:
            data = await _safe_json(r)
            if r.status != 200 or data.get("Status") != "Success":
                log.error("Ferma auth failed: status=%s body=%s", r.status, data)
                raise FermaError(r.status, data)

            d = data.get("Data") or {}
            self._token = d.get("AuthToken")
            exp = d.get("ExpirationDateUtc")
            self._token_expiry = _parse_ferma_utc(exp)
            # Немного полезного лога (паролей тут нет)
            log.info(
                "Ferma auth OK. Token expires at %s (UTC). Base=%s Login=%s",
                self._token_expiry, self.cfg.base_url, self.cfg.login
            )

    async def _ensure_token(self):
        """
        Кешируем токен; обновляем заранее (за 60 сек до истечения).
        """
        if self._token and self._token_expiry:
            if (self._token_expiry - datetime.now(timezone.utc)).total_seconds() > 60:
                return
        await self._auth()

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
        Формирует чек прихода (Type='Income') для Ferma Cloud KKT.
        Все поля берутся из .env через Settings (self.cfg). Ничего не хардкодим.
        Возвращает: {"receipt_id": str, "invoice_id": str | None}
        """
        s = get_settings()

        # --- Inn (обязателен) ---
        inn = str(getattr(s, "FERMA_INN", "")).strip()

        # --- Опциональные параметры из .env ---
        taxation = (getattr(s, "FERMA_TAXATION_SYSTEM", None) or "").strip() or None
        vat = getattr(s, "FERMA_VAT", "VatNo")
        payment_type = int(getattr(s, "FERMA_PAYMENT_TYPE", 4))
        payment_method = int(getattr(s, "FERMA_PAYMENT_METHOD", 4))
        measure = getattr(s, "FERMA_MEASURE", "PIECE")
        is_internet = bool(getattr(s, "FERMA_IS_INTERNET", False))
        bill_address = (getattr(s, "FERMA_BILL_ADDRESS", None) or "").strip() or None
        timezone = getattr(s, "FERMA_TIMEZONE", 0)
        try:
            timezone = int(timezone)
        except Exception:
            timezone = 0
        timezone = timezone if 1 <= timezone <= 11 else None

        # --- CallbackUrl (важно для колбэка Ferma) ---
        callback_url = getattr(s, "ferma_full_callback_url", None)
        if not callback_url and getattr(s, "WEBHOOK_BASE_URL", None):
            base = s.WEBHOOK_BASE_URL.rstrip("/")
            path = getattr(s, "ferma_callback_path", "/webhook/ferma")
            callback_url = f"{base}{path}"

        # --- CustomerReceipt (одной строкой на всю сумму) ---
        total = float(amount)
        item = {
            "Label": (description or f"Оплата заказа {invoice_id}")[:128],
            "Price": total,
            "Quantity": 1,
            "Amount": total,
            "Vat": vat,
            "Measure": measure,
            "PaymentMethod": payment_method,
            "PaymentType": payment_type,
        }
        customer_receipt = {
            "Items": [item],
            "TotalSum": total,
            **({"TaxationSystem": taxation} if taxation else {}),
            **({"BillAddress": bill_address} if bill_address else {}),
            **({"Email": buyer_email} if buyer_email else {}),
            **({"Phone": buyer_phone} if buyer_phone else {}),
        }

        # --- Корневой Request ---
        request_obj: dict = {
            "Inn": inn,
            "Type": "Income",
            "InvoiceId": invoice_id,
            "CustomerReceipt": customer_receipt,
            **({"CallbackUrl": callback_url} if callback_url else {}),
            **({"IsInternet": True} if is_internet else {}),
            **({"Timezone": timezone} if timezone is not None else {}),
            **({"Data": {"PaymentIdentifiers": [payment_identifiers]}}
               if payment_identifiers else {}),
        }
        payload = {"Request": request_obj}

        # Отладочный лог полезен при интеграциях (без чувствительных данных)
        try:
            log.info(
                "Ferma SEND receipt: inn=%s, group=%s, invoice=%s, amount=%.2f, callback=%r, payload_keys=%s",
                inn, getattr(s, "FERMA_GROUP_CODE", None), invoice_id, total, callback_url, list(payload["Request"].keys())
            )
        except Exception:
            pass

        # Вызов Ferma Cloud
        resp = await self._post_json("/api/kkt/cloud/receipt", payload, use_token=True)
        data = (resp or {}).get("Data") or resp or {}

        receipt_id = data.get("ReceiptId")
        ferma_invoice_id = data.get("InvoiceId")
        if not receipt_id:
            raise FermaError(400, {"Status": "Failed", "Error": {"Message": f"Invalid response from Ferma: {resp}"}})

        return {"receipt_id": receipt_id, "invoice_id": ferma_invoice_id}

    async def check_status(self, *, invoice_id: str | None = None, receipt_id: str | None = None) -> dict:
        """
        POST /api/kkt/cloud/status — вернуть Data c полями StatusCode/Device.OfdReceiptUrl и т.п.
        Можно передавать либо ReceiptId, либо InvoiceId (одно из них обязательно).
        """
        if not invoice_id and not receipt_id:
            raise ValueError("check_status requires either invoice_id or receipt_id")

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

    # --------------------- low level HTTP with retries + re-auth ---------------------

    async def _post_json(self, path: str, json_body: Dict[str, Any], *, use_token: bool) -> Dict[str, Any]:
        """
        Базовый POST.
        - Если use_token=True, гарантируем валидный токен (ленивый логин/рефреш).
        - Отправляем токен как query param AuthToken=<...> (как в твоей рабочей версии).
        - При 401 или ответе с Code=1001 делаем ОДИН повтор с принудительным ре-логином.
        - На 429/5xx — экспоненциальный ретрай.
        """
        sess = await self._session_get()
        url = self.cfg.base_url.rstrip("/") + path

        params: Dict[str, str] = {}
        if use_token:
            await self._ensure_token()
            if self._token:
                params["AuthToken"] = self._token  # type: ignore

        headers = {"Content-Type": "application/json"}

        attempts = 5
        backoff = 0.6

        async def _once(with_refresh: bool = False) -> tuple[int, Dict[str, Any]]:
            # если запрос после принудительного ре-логина
            if with_refresh:
                # сброс токена и повторная аутентификация
                self._token = None
                self._token_expiry = None
                try:
                    await self._auth()
                except Exception as e:
                    # вернуть 401 с телом ошибки авторизации
                    return 401, {"Status": "Failed", "Error": {"Message": f"Auth refresh failed: {e}"}}
                # обновить параметр
                if self._token:
                    params["AuthToken"] = self._token  # type: ignore

            async with sess.post(url, params=params, json=json_body, headers=headers) as r:
                data = await _safe_json(r)
                return r.status, data

        for i in range(1, attempts + 1):
            status, data = await _once(with_refresh=False)

            # штатный успех
            if 200 <= status < 300:
                # но проверим «логическую» ошибку авторизации
                if (isinstance(data, dict)
                        and data.get("Status") == "Failed"
                        and isinstance(data.get("Error"), dict)
                        and str((data["Error"].get("Code"))) == "1001"):
                    # «Клиент не авторизован» в теле — как 401
                    status = 401
                else:
                    return data

            # 401 — принудительный ре-логин и один повтор
            if status == 401 and use_token:
                status2, data2 = await _once(with_refresh=True)
                if 200 <= status2 < 300:
                    return data2
                # если повтор тоже не удался — бросаем ошибку
                raise FermaError(status2, data2)

            # retry на перегрузку/тайм-ауты/429
            if status in (429, 500, 502, 503, 504):
                if i == attempts:
                    raise FermaError(status, data)
                await asyncio.sleep(backoff)
                backoff *= 2
                continue

            # прочие ошибки — сразу исключение
            raise FermaError(status, data)

        # теоретически недостижимо
        raise FermaError(599, {"Status": "Failed", "Error": {"Message": "Unexpected retry loop exit"}})


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