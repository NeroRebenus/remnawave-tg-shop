# bot/handlers/admin/paylink.py
from __future__ import annotations

import re
import logging
from typing import Optional, Dict

from aiogram import Router, types
from aiogram.filters import Command
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import Settings
from bot.services.yookassa_service import YooKassaService

router = Router(name="admin_paylink_router")

# Периоды: алиасы -> число месяцев
PERIOD_MAP = {"1m": 1, "3m": 3, "6m": 6, "12m": 12}

# Кэш одного инстанса сервиса YooKassa
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


def _parse_cmd(text: str) -> Optional[Dict]:
    """
    /paylink <panel_username> <period> [comment ...] amount=XXXX [ttl=MIN]

    Пример:
      /paylink ivanov 3m Оплата подписки amount=1990 ttl=90
      /paylink 123456 12m Клиент с числовым ником amount=4990
    """
    if not text:
        return None
    parts = text.strip().split(maxsplit=3)
    if len(parts) < 3:
        return None

    _, username, period = parts[:3]
    rest = parts[3] if len(parts) == 4 else ""

    username = username.lower().strip()
    if period not in PERIOD_MAP:
        return None

    # Опциональные ключи
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
        "username": username,
        "period": period,
        "comment": comment,
        "amount": amount_override,
        "ttl_minutes": ttl_minutes,
    }


@router.message(Command("paylink"))
async def cmd_paylink(
    message: types.Message,
    session: AsyncSession,
    settings: Settings,
):
    """
    Создать платёжную ссылку YooKassa для клиента по username из панели.
    Обязательно нужно указать amount=...
    """
    if not _is_admin(message.from_user.id, settings):
        await message.answer("Недостаточно прав.")
        return

    parsed = _parse_cmd(message.text or "")
    if not parsed:
        await message.answer(
            "Использование:\n"
            "/paylink <panel_username> <period> [comment] amount=XXXX [ttl=MIN]\n"
            "Примеры:\n"
            "  /paylink ivanov 3m Оплата подписки amount=1990 ttl=90\n"
            "  /paylink 123456 12m Клиент с числовым ником amount=4990"
        )
        return

    username: str = parsed["username"]
    period: str = parsed["period"]
    comment: Optional[str] = parsed["comment"]
    amount_rub: Optional[float] = parsed["amount"]
    ttl_minutes: int = parsed["ttl_minutes"] or 120
    months = PERIOD_MAP[period]

    if amount_rub is None:
        await message.answer(
            "Для создания ссылки обязательно укажите цену, например:\n"
            f"/paylink {username} {period} Комментарий amount=1990"
        )
        return

    # Инициализация YooKassa
    bot_username = message.bot.username if hasattr(message.bot, "username") else None
    yk = _get_yk_service(settings, bot_username)

    try:
        result = await yk.create_admin_payment_link(
            tg_id=0,  # не используется
            period=period,
            amount_rub=amount_rub,
            comment=comment,
            ttl_minutes=ttl_minutes,
            receipt_email=None,
            receipt_phone=None,
            return_url_override=None,
            extra_metadata={
                "months": months,
                "admin_initiator_id": str(message.from_user.id),
                "source": "admin_link",
                "id_type": "panel_username",
                "username": username,
            },
        )
    except Exception as e:
        logging.exception("create_admin_payment_link failed")
        await message.answer(f"Ошибка при создании ссылки: {e}")
        return

    if not result or not result.get("confirmation_url"):
        await message.answer("Не удалось создать ссылку оплаты (проверьте логи).")
        return

    url = result["confirmation_url"]

    await message.answer(
        "Ссылка на оплату создана ✅\n\n"
        f"👤 username (панель): <b>{username}</b>\n"
        f"📦 период: <b>{period}</b>\n"
        f"💰 сумма: <b>{amount_rub:.2f} RUB</b>\n"
        f"⏳ срок действия: ~{ttl_minutes} мин.\n"
        + (f"📝 комментарий: <i>{comment}</i>\n" if comment else "")
        + "\n"
        f"{url}",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
