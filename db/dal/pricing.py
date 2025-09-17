from typing import Literal, TypedDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.Settings import Settings
from db.models import UserPricePlan

Period = Literal["1m", "3m", "6m", "12m"]

class EffectivePrices(TypedDict):
    rub_1m: int; rub_3m: int; rub_6m: int; rub_12m: int
    stars_1m: int | None; stars_3m: int | None; stars_6m: int | None; stars_12m: int | None

def _defaults_from_env() -> EffectivePrices:
    return EffectivePrices(
        rub_1m=Settings.RUB_PRICE_1_MONTH,
        rub_3m=Settings.RUB_PRICE_3_MONTHS,
        rub_6m=Settings.RUB_PRICE_6_MONTHS,
        rub_12m=Settings.RUB_PRICE_12_MONTHS,
        stars_1m=getattr(Settings, "STARS_PRICE_1_MONTH", None),
        stars_3m=getattr(Settings, "STARS_PRICE_3_MONTHS", None),
        stars_6m=getattr(Settings, "STARS_PRICE_6_MONTHS", None),
        stars_12m=getattr(Settings, "STARS_PRICE_12_MONTHS", None),
    )

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
    *,
    rub_1m: int | None = None, rub_3m: int | None = None, rub_6m: int | None = None, rub_12m: int | None = None,
    stars_1m: int | None = None, stars_3m: int | None = None, stars_6m: int | None = None, stars_12m: int | None = None,
    created_by_admin_id: int | None = None,
) -> UserPricePlan:
    plan = await get_or_init_user_price_plan(session, user_id, created_by_admin_id=created_by_admin_id)
    if rub_1m is not None:  plan.rub_1m = rub_1m
    if rub_3m is not None:  plan.rub_3m = rub_3m
    if rub_6m is not None:  plan.rub_6m = rub_6m
    if rub_12m is not None: plan.rub_12m = rub_12m

    if stars_1m is not None:  plan.stars_1m = stars_1m
    if stars_3m is not None:  plan.stars_3m = stars_3m
    if stars_6m is not None:  plan.stars_6m = stars_6m
    if stars_12m is not None: plan.stars_12m = stars_12m

    await session.flush()
    return plan

async def get_effective_prices(session: AsyncSession, user_id: int) -> EffectivePrices:
    plan = await get_or_init_user_price_plan(session, user_id)
    return EffectivePrices(
        rub_1m=plan.rub_1m, rub_3m=plan.rub_3m, rub_6m=plan.rub_6m, rub_12m=plan.rub_12m,
        stars_1m=plan.stars_1m, stars_3m=plan.stars_3m, stars_6m=plan.stars_6m, stars_12m=plan.stars_12m,
    )
