# bot/handlers/admin/paylink.py
from __future__ import annotations

import re
import logging
from typing import Optional, Dict

from aiogram import Router, types, F
from aiogram.filters import Command
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import Settings
from bot.middlewares.i18n import JsonI18n

from db.dal.pricing import get_or_init_user_price_plan
from bot.services.yookassa_service import YooKassaService

router = Router(name="admin_paylink_router")

# Периоды: алиасы -> число месяцев
PERIOD_MAP = {"1m": 1, "3m": 3, "6m": 6, "12m": 12}

# Кэш одного инстанса сервиса, чтобы не конфигурировать SDK каждый раз
_yk_service_singleton: Optional[YooKassaService] = None


def _get_yk_service(settings: Settings, bot_username: Optional[str]) -> YooKassaService:
    global _yk_service_singleton
    if _yk_service_singleton is None:
        _yk_service_singleton = YooKassaService(
            shop_id=settings.YOOKASSA_SHOP_ID,
            secret_key=settings.YOOKASSA_SECRET_KEY,
            configured_return_url=getattr(settings, "YOOKASSA_RETURN_URL", None),
            bot_username_for_default_return=bot_username,
            settings_obj=settings,
        )
    return _yk_service_singleton


def _is_admin(user_id: int, settings: Settings) -> bool:
    # Поддержим оба возможных названия из настроек
    admin_ids = (
        getattr(settings, "ADMIN_TG_IDS", None)
        or getattr(settings, "ADMIN_IDS", None)
        or []
    )
    try:
        return int(user_id) in {int(x) for x in admin_ids}
    except Exception:
        return False


def _parse_cmd(text: str) -> Optional[Dict]:
    """
    Парсим:
      /paylink <tg_id> <period> [comment ...] [amount=XXXX] [ttl=MIN]

    Пример:
      /paylink 123456789 3m Клиент Иван amount=1990 ttl=90
    """
    if not text:
        return None
    parts = text.strip().split(maxsplit=3)
    if len(parts) < 3:
        return None
    _, tg_id_str, period = parts[:3]
    rest = parts[3] if len(parts) == 4 else ""

    try:
        tg_id = int(tg_id_str)
    except ValueError:
        return None
    if period not in PERIOD_MAP:
        return None

    # Опциональные ключи в хвосте
    amount_override: Optional[float] = None
    ttl_minutes: Optional[int] = None

    # Ищем amount=XXXX и ttl=YYY в конце строки (могут быть в любой последовательности)
    amount_match = re.search(r"(?:^|\s)amount=(\d+(?:[.,]\d{1,2})?)\b", rest)
    ttl_match = re.search(r"(?:^|\s)ttl=(\d{1,5})\b", rest)

    if amount_match:
        amount_str = amount_match.group(1).replace(",", ".")
        try:
            amount_override = float(amount_str)
        except ValueError:
            amount_override = None
        # вырезаем из комментария
        rest = (rest[:amount_match.start()] + rest[amount_match.end():]).strip()

    if ttl_match:
        try:
            ttl_minutes = int(ttl_match.group(1))
        except ValueError:
            ttl_minutes = None
        rest = (rest[:ttl_match.start()] + rest[ttl_match.end():]).strip()

    comment = rest.strip() or None

    return {
        "tg_id": tg_id,
        "period": period,
        "comment": comment,
        "amount_override": amount_override,
        "ttl_minutes": ttl_minutes,
    }


def _price_from_plan(user_plan, months: int) -> Optional[float]:
    """
    Подстрой под имена полей в твоей модели прайса.
    Ожидаемые поля (пример):
      rub_price_1_month, rub_price_3_months, rub_price_6_months, rub_price_12_months
    """
    mapping = {
        1: getattr(user_plan, "rub_price_1_month", None),
        3: getattr(user_plan, "rub_price_3_months", None),
        6: getattr(user_plan, "rub_price_6_months", None),
        12: getattr(user_plan, "rub_price_12_months", None),
    }
    val = mapping.get(months)
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None


@router.message(Command("paylink"))
async def cmd_paylink(
    message: types.Message,
    session: AsyncSession,
    i18n: JsonI18n,
    settings: Settings,
):
    """
    Создать платёжную ссылку YooKassa для клиента по tg_id и периоду.
    Команда только для админов.

    Использование:
      /paylink <tg_id> <period> [comment] [amount=XXXX] [ttl=MIN]
    Периоды:
      1m, 3m, 6m, 12m
    """
    # 0) Проверка прав
    if not _is_admin(message.from_user.id, settings):
        return await message.answer("Недостаточно прав.")

    parsed = _parse_cmd(message.text or "")
    if not parsed:
        return await message.answer(
            "Использование:\n"
            "/paylink <tg_id> <period> [comment] [amount=XXXX] [ttl=MIN]\n"
            "Примеры:\n"
            "  /paylink 123456789 3m Клиент Иван\n"
            "  /paylink 123456789 12m VIP клиент amount=4990 ttl=180"
        )

    tg_id = parsed["tg_id"]
    period = parsed["period"]
    comment = parsed["comment"]
    amount_override = parsed["amount_override"]
    ttl_minutes = parsed["ttl_minutes"] or 120

    months = PERIOD_MAP[period]

    # 1) Прайс-план пользователя (или создаём дефолтный)
    user_plan = await get_or_init_user_price_plan(session, tg_id)
    if not user_plan:
        return await message.answer("Не удалось получить прайс-план пользователя.")

    # 2) Цена: из плана или переопределённая
    amount_rub: Optional[float] = amount_override or _price_from_plan(user_plan, months)
    if amount_rub is None:
        return await message.answer("Цена для выбранного периода не найдена.")

    # 3) Инициализация сервиса YooKassa
    bot_username = message.bot.username if hasattr(message.bot, "username") else None
    yk = _get_yk_service(settings, bot_username)

    # 4) Создаём платёж с confirmation_url
    try:
        result = await yk.create_admin_payment_link(
            tg_id=tg_id,
            period=period,
            amount_rub=amount_rub,
            comment=comment,
            ttl_minutes=ttl_minutes,
            # Если у тебя в админке есть мейл клиента — передай его сюда:
            receipt_email=None,
            receipt_phone=None,
            # Можно переопределить return_url при необходимости:
            return_url_override=None,
            extra_metadata={
                "months": months,
                "admin_initiator_id": str(message.from_user.id),
            },
        )
    except Exception as e:
        logging.exception("create_admin_payment_link failed")
        return await message.answer(f"Ошибка при создании ссылки: {e}")

    if not result or not result.get("confirmation_url"):
        return await message.answer("Не удалось создать ссылку оплаты (проверьте логи).")

    url = result["confirmation_url"]
    await message.answer(
        "Ссылка на оплату создана ✅\n\n"
        f"👤 tg_id: <code>{tg_id}</code>\n"
        f"📦 период: <b>{period}</b>\n"
        f"💰 сумма: <b>{amount_rub:.2f} RUB</b>\n"
        f"⏳ срок действия: ~{ttl_minutes} мин.\n"
        + (f"📝 комментарий: <i>{comment}</i>\n" if comment else "")
        + "\n"
        f"{url}",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )