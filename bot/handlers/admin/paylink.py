# bot/handlers/admin/paylink.py
from __future__ import annotations

import re
import logging
from typing import Optional, Dict, Union

from aiogram import Router, types
from aiogram.filters import Command
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import Settings

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
    admin_ids = (
        getattr(settings, "ADMIN_TG_IDS", None)
        or getattr(settings, "ADMIN_IDS", None)
        or []
    )
    try:
        return int(user_id) in {int(x) for x in admin_ids}
    except Exception:
        return False


def _parse_cmd(text: str) -> Optional[Dict[str, Union[int, str, float, None]]]:
    """
    Поддерживаем два варианта первого аргумента:
      /paylink <tg_id|@username> <period> [comment ...] [amount=XXXX] [ttl=MIN]

    Примеры:
      /paylink 123456789 3m Клиент Иван amount=1990 ttl=90
      /paylink @panel_nick 6m Оплата от менеджера amount=3490
    """
    if not text:
        return None
    parts = text.strip().split(maxsplit=3)
    if len(parts) < 3:
        return None

    _, id_part, period = parts[:3]
    rest = parts[3] if len(parts) == 4 else ""

    # id_part может быть числом (tg_id) или ником (@username / username)
    tg_id: Optional[int] = None
    username: Optional[str] = None
    try:
        tg_id = int(id_part)
    except ValueError:
        username = id_part.lstrip("@").lower()

    if period not in PERIOD_MAP:
        return None

    # Опциональные ключи: amount=XXXX и ttl=YYY
    amount_override: Optional[float] = None
    ttl_minutes: Optional[int] = None

    amount_match = re.search(r"(?:^|\s)amount=(\d+(?:[.,]\d{1,2})?)\b", rest)
    ttl_match = re.search(r"(?:^|\s)ttl=(\d{1,5})\b", rest)

    if amount_match:
        amount_str = amount_match.group(1).replace(",", ".")
        try:
            amount_override = float(amount_str)
        except ValueError:
            amount_override = None
        rest = (rest[:amount_match.start()] + rest[amount_match.end():]).strip()

    if ttl_match:
        try:
            ttl_minutes = int(ttl_match.group(1))
        except ValueError:
            ttl_minutes = None
        rest = (rest[:ttl_match.start()] + rest[ttl_match.end():]).strip()

    comment = rest.strip() or None

    return {
        "tg_id": tg_id,                 # None если указан username
        "username": username,           # None если указан tg_id
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
    settings: Settings,
):
    """
    Создать платёжную ссылку YooKassa для клиента:
      /paylink <tg_id|@username> <period> [comment] [amount=XXXX] [ttl=MIN]

    Периоды: 1m, 3m, 6m, 12m

    Правила:
    - Если передан tg_id: цена берётся из прайс-плана пользователя (можно переопределить amount=...)
    - Если передан @username панели: ОБЯЗАТЕЛЕН параметр amount=..., т.к. прайс-плана в БД бота может не быть.
      В метаданные будет добавлено id_type=panel_username и username=<ник>.
    """
    if not _is_admin(message.from_user.id, settings):
        await message.answer("Недостаточно прав.")
        return

    parsed = _parse_cmd(message.text or "")
    if not parsed:
        await message.answer(
            "Использование:\n"
            "/paylink <tg_id|@username> <period> [comment] [amount=XXXX] [ttl=MIN]\n"
            "Примеры:\n"
            "  /paylink 123456789 3m Клиент Иван\n"
            "  /paylink @panel_nick 6m Оплата от менеджера amount=3490\n"
            "  /paylink 123456789 12m VIP клиент amount=4990 ttl=180"
        )
        return

    tg_id: Optional[int] = parsed["tg_id"]  # может быть None
    username: Optional[str] = parsed["username"]  # может быть None
    period: str = parsed["period"]  # гарантированно из PERIOD_MAP
    comment: Optional[str] = parsed["comment"]
    amount_override: Optional[float] = parsed["amount_override"]
    ttl_minutes: int = parsed["ttl_minutes"] or 120

    months = PERIOD_MAP[period]

    amount_rub: Optional[float] = None
    extra_metadata: Dict[str, object] = {
        "months": months,
        "admin_initiator_id": str(message.from_user.id),
    }

    # Режим 1: указан tg_id -> берём цену из прайс-плана (или override)
    if tg_id is not None:
        user_plan = await get_or_init_user_price_plan(session, tg_id)
        if not user_plan:
            await message.answer("Не удалось получить прайс-план пользователя.")
            return
        amount_rub = amount_override or _price_from_plan(user_plan, months)
        if amount_rub is None:
            await message.answer("Цена для выбранного периода не найдена.")
            return
        # В этом режиме можно оставить идентификацию по tg_id (если вдруг пригодится где-то ещё)
        # Но для админ-линков мы теперь ориентируемся по нику панели, поэтому tg_id в metadata не обязателен.
        # Оставим совместимость: запишем tg_id как справочный.
        extra_metadata["tg_id"] = str(tg_id)

    # Режим 2: указан username панели -> обязателен amount=...
    elif username:
        if amount_override is None:
            await message.answer(
                "Для ссылок по нику панели укажите цену, например:\n"
                f"/paylink @{username} {period} Комментарий amount=1990"
            )
            return
        amount_rub = amount_override
        extra_metadata["id_type"] = "panel_username"
        extra_metadata["username"] = username  # уже нормализован (без @, lower)
    else:
        await message.answer("Укажите tg_id или @username в качестве первого аргумента.")
        return

    # 3) Инициализация сервиса YooKassa
    bot_username = message.bot.username if hasattr(message.bot, "username") else None
    yk = _get_yk_service(settings, bot_username)

    # 4) Создаём платёж с confirmation_url
    try:
        result = await yk.create_admin_payment_link(
            tg_id=tg_id or 0,  # не используется в логике admin_link по нику, но параметр обязателен
            period=period,
            amount_rub=amount_rub,
            comment=comment,
            ttl_minutes=ttl_minutes,
            receipt_email=None,
            receipt_phone=None,
            return_url_override=None,
            extra_metadata=extra_metadata,
        )
    except Exception as e:
        logging.exception("create_admin_payment_link failed")
        await message.answer(f"Ошибка при создании ссылки: {e}")
        return

    if not result or not result.get("confirmation_url"):
        await message.answer("Не удалось создать ссылку оплаты (проверьте логи).")
        return

    url = result["confirmation_url"]

    # Текст подтверждения с учётом режима
    if tg_id is not None:
        header = f"👤 tg_id: <code>{tg_id}</code>\n"
    else:
        header = f"👤 username (панель): <b>@{username}</b>\n"

    await message.answer(
        "Ссылка на оплату создана ✅\n\n"
        + header +
        f"📦 период: <b>{period}</b>\n"
        f"💰 сумма: <b>{amount_rub:.2f} RUB</b>\n"
        f"⏳ срок действия: ~{ttl_minutes} мин.\n"
        + (f"📝 комментарий: <i>{comment}</i>\n" if comment else "")
        + "\n"
        f"{url}",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
