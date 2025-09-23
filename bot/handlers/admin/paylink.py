# bot/handlers/admin/paylink.py
from __future__ import annotations
from aiogram import Router, types, F
from aiogram.filters import Command
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import Settings
from bot.middlewares.i18n import JsonI18n
from db.dal.pricing import get_or_init_user_price_plan
from bot.services.yookassa_service import create_admin_payment_link

router = Router(name="admin_paylink_router")

PERIOD_MAP = {"1m": 1, "3m": 3, "6m": 6, "12m": 12}

def _parse_args(text: str):
    # /paylink <tg_id> <period> [comment...]
    parts = text.split(maxsplit=3)
    if len(parts) < 3:
        return None
    _, tg_id_str, period = parts[:3]
    comment = parts[3] if len(parts) == 4 else None
    if period not in PERIOD_MAP:
        return None
    try:
        tg_id = int(tg_id_str)
    except ValueError:
        return None
    return tg_id, period, comment

@router.message(Command("paylink"))
async def cmd_paylink(message: types.Message, session: AsyncSession, i18n: JsonI18n):
    """
    Пример:
    /paylink 123456789 3m Клиент Иван
    """
    parsed = _parse_args(message.text or "")
    if not parsed:
        return await message.answer(
            "Использование:\n"
            "/paylink <tg_id> <period> [comment]\n"
            "period ∈ {1m,3m,6m,12m}"
        )

    tg_id, period, comment = parsed

    # 1) Получаем либо создаём прайс-план пользователя
    user_plan = await get_or_init_user_price_plan(session, tg_id)

    # 2) Берём цену в рублях за выбранный период из плана
    months = PERIOD_MAP[period]
    # имена полей могут отличаться у вас — скорректируйте при необходимости
    price_map = {
        1: user_plan.rub_price_1_month,
        3: user_plan.rub_price_3_months,
        6: user_plan.rub_price_6_months,
        12: user_plan.rub_price_12_months,
    }
    amount_rub = int(price_map[months])

    # 3) Создаём ссылку оплаты
    confirmation_url = await create_admin_payment_link(
        tg_id=tg_id,
        period=period,
        amount_rub=amount_rub,
        comment=comment,
        ttl_minutes=120,  # можно поменять
    )

    # 4) Отдаём ссылку админу
    await message.answer(
        f"Ссылка на оплату для tg_id={tg_id}, {period}: \n{confirmation_url}\n\n"
        f"Срок действия: ~2 часа."
    )