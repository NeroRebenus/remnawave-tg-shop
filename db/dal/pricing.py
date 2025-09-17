from typing import Literal, TypedDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import Settings
from db.models import UserPricePlan
from typing import Dict, Optional
from sqlalchemy import select, update, insert

Period = Literal["1m", "3m", "6m", "12m"]

class EffectivePrices(TypedDict):
    rub_1m: int; rub_3m: int; rub_6m: int; rub_12m: int
    stars_1m: int | None; stars_3m: int | None; stars_6m: int | None; stars_12m: int | None

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
async def get_or_init_user_price_plan(session: AsyncSession, user_id: int, *, created_by_admin_id: int | None = None) -> UserPricePlan:
    plan = await session.scalar(select(UserPricePlan).where(UserPricePlan.user_id == user_id))
    if plan:
        return plan

    d = _defaults_from_env()
    plan = UserPricePlan(
        user_id=user_id,
        rub_1m=d["rub_1m"], rub_3m=d["rub_3m"], rub_6m=d["rub_6m"], rub_12m=d["rub_12m"],
        stars_1m=d["stars_1m"], stars_3m=d["stars_3m"], stars_6m=d["stars_6m"], stars_12m=d["stars_12m"],
        created_by_admin_id=created_by_admin_id,
    )
    session.add(plan)
    await session.flush()
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

async def get_effective_prices(session: AsyncSession, user_id: int) -> EffectivePrices:
    """
    Возвращает «эффективные» цены: значения из UserPricePlan,
    а если какое-то поле отсутствует/None — подставляет дефолт из .env.
    """
    plan = await get_or_init_user_price_plan(session, user_id)
    defaults = _defaults_from_env()

    def pick(db_val, key: str):
        # Берём значение из плана, иначе дефолт из .env
        return db_val if db_val is not None else defaults[key]

    return EffectivePrices(
        rub_1m=pick(plan.rub_1m,  "rub_1m"),
        rub_3m=pick(plan.rub_3m,  "rub_3m"),
        rub_6m=pick(plan.rub_6m,  "rub_6m"),
        rub_12m=pick(plan.rub_12m, "rub_12m"),
        stars_1m=pick(plan.stars_1m,  "stars_1m"),
        stars_3m=pick(plan.stars_3m,  "stars_3m"),
        stars_6m=pick(plan.stars_6m,  "stars_6m"),
        stars_12m=pick(plan.stars_12m, "stars_12m"),
    )
