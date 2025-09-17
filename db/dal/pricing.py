# db/dal/pricing.py

from typing import Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from config.settings import Settings
from db.models import UserPricePlan  # или ваша модель
from sqlalchemy import select, update, insert

def _defaults_from_env() -> Dict[str, Optional[int]]:
    """
    Берём дефолтные цены из Settings().subscription_options и Settings().stars_subscription_options.
    """
    s = Settings()  # инстанс (pydantic BaseSettings подтянет .env)
    rub = s.subscription_options or {}
    stars = s.stars_subscription_options or {}
    return {
        "rub_1m": rub.get(1),
        "rub_3m": rub.get(3),
        "rub_6m": rub.get(6),
        "rub_12m": rub.get(12),
        "stars_1m": stars.get(1),
        "stars_3m": stars.get(3),
        "stars_6m": stars.get(6),
        "stars_12m": stars.get(12),
    }

async def get_or_init_user_price_plan(session: AsyncSession, user_id: int) -> UserPricePlan:
    # если есть — вернуть
    q = await session.execute(select(UserPricePlan).where(UserPricePlan.user_id == user_id))
    plan = q.scalar_one_or_none()
    if plan:
        return plan

    # если нет — создать из env-дефолтов
    d = _defaults_from_env()
    stmt = insert(UserPricePlan).values(user_id=user_id, **d).returning(UserPricePlan)
    res = await session.execute(stmt)
    plan = res.scalar_one()
    # коммит снаружи (вы уже делаете commit в обработчике)
    return plan

async def update_user_price_plan(
    session: AsyncSession,
    user_id: int,
    created_by_admin_id: Optional[int] = None,
    **fields: int,
) -> UserPricePlan:
    q = await session.execute(select(UserPricePlan).where(UserPricePlan.user_id == user_id))
    plan = q.scalar_one_or_none()
    if not plan:
        # инициализируем дефолтами и позже обновим
        plan = await get_or_init_user_price_plan(session, user_id)

    upd = {k: v for k, v in fields.items() if v is not None}
    if upd:
        stmt = (
            update(UserPricePlan)
            .where(UserPricePlan.user_id == user_id)
            .values(**upd, updated_by_admin_id=created_by_admin_id)
            .returning(UserPricePlan)
        )
        res = await session.execute(stmt)
        plan = res.scalar_one()
    return plan
