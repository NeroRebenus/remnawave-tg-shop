from __future__ import annotations

from typing import Optional, Dict

from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import Settings
from bot.middlewares.i18n import JsonI18n
from db.dal import user_dal
from db.dal.pricing import get_or_init_user_price_plan, update_user_price_plan

router = Router(name="admin_user_price_router")


class AdminPriceStates(StatesGroup):
    waiting_user_id = State()
    waiting_prices = State()


def _t(i18n_data: dict, settings: Settings):
    """Удобный шорткат для i18n."""
    lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    return (lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)) if i18n else (lambda key, **kwargs: key)


def _parse_prices(s: str) -> Dict[str, int]:
    """
    Принимает строку вида:
      1m=1200;3m=3000;6m=5000;12m=9000;1m*=120;12m*=900
    Где * — цены в Stars. Возвращает словарь полей для update_user_price_plan.
    """
    s = (s or "").strip().replace(" ", "")
    if not s:
        return {}
    result: Dict[str, int] = {}
    parts = [p for p in s.split(";") if p]
    valid = {"1m", "3m", "6m", "12m"}
    for part in parts:
        if "=" not in part:
            raise ValueError(f"Нет '=' в '{part}'")
        key, val = part.split("=", 1)
        is_stars = key.endswith("*")
        period = key[:-1] if is_stars else key
        if period not in valid:
            raise ValueError(f"Неизвестный период: '{period}' (ожидалось 1m/3m/6m/12m)")
        if not val.isdigit():
            raise ValueError(f"Значение должно быть целым числом: '{part}'")
        amount = int(val)
        if amount < 0:
            raise ValueError(f"Цена не может быть отрицательной: '{part}'")
        field = ("stars_" if is_stars else "rub_") + period  # -> rub_1m / stars_3m
        result[field] = amount
    return result


@router.callback_query(F.data == "admin_action:user_price_prompt")
async def admin_user_price_prompt(
    cb: types.CallbackQuery,
    state: FSMContext,
    i18n_data: dict,
    settings: Settings,
):
    _ = _t(i18n_data, settings)
    await state.set_state(AdminPriceStates.waiting_user_id)
    await cb.message.edit_text(
        _("admin_user_price_enter_id", default="Введите ID пользователя (Telegram user_id) для редактирования цены.")
    )
    await cb.answer()


@router.message(AdminPriceStates.waiting_user_id)
async def admin_user_price_got_id(
    msg: types.Message,
    state: FSMContext,
    session: AsyncSession,
    i18n_data: dict,
    settings: Settings,
):
    _ = _t(i18n_data, settings)
    try:
        user_id = int(msg.text.strip())
    except Exception:
        await msg.answer(_("admin_user_price_bad_id", default="Некорректный ID. Отправьте целое число."))
        return

    # Если пользователя нет — создадим минимальную запись User
    db_user = await user_dal.get_user_by_id(session, user_id)
    created_new = False
    if not db_user:
        data = {
            "user_id": user_id,
            "username": None,
            "first_name": None,
            "last_name": None,
            "language_code": i18n_data.get("current_language", settings.DEFAULT_LANGUAGE),
            "referred_by_id": None,
            # registration_date у тебя либо server_default, либо выставится в DAL
        }
        try:
            db_user, created_new = await user_dal.create_user(session, data)
        except Exception as e:
            await msg.answer(_("admin_user_price_create_user_failed", default=f"Ошибка создания пользователя: {e}"))
            return

    await state.update_data(target_user_id=user_id, created_new=created_new)

    # Гарантируем наличие плана цен (по умолчанию из .env)
    await get_or_init_user_price_plan(session, user_id=user_id)
    await session.flush()

    await state.set_state(AdminPriceStates.waiting_prices)
    await msg.answer(
        _("admin_user_price_enter_prices",
          default=(
              "Отправьте цены в формате:\n"
              "`1m=1200;3m=3000;6m=5000;12m=9000`\n"
              "Для Stars используйте `*`: например, `1m*=120;12m*=900`.\n"
              "Можно указывать частично — изменятся только переданные поля."
          )),
        parse_mode="Markdown",
    )


@router.message(AdminPriceStates.waiting_prices)
async def admin_user_price_save(
    msg: types.Message,
    state: FSMContext,
    session: AsyncSession,
    i18n_data: dict,
    settings: Settings,
):
    _ = _t(i18n_data, settings)
    data = await state.get_data()
    user_id = data.get("target_user_id")
    if not user_id:
        await msg.answer(_("admin_user_price_state_lost", default="Состояние потеряно. Запустите процесс заново."))
        await state.clear()
        return

    try:
        parsed = _parse_prices(msg.text)
    except Exception as e:
        await msg.answer(_("admin_user_price_parse_error", default=f"Ошибка разбора: {e}\nПопробуйте снова."))
        return

    try:
        plan = await update_user_price_plan(
            session,
            user_id=user_id,
            created_by_admin_id=msg.from_user.id,
            **parsed,
        )
        await session.commit()
    except Exception as e:
        await session.rollback()
        await msg.answer(_("admin_user_price_save_failed", default=f"Ошибка сохранения цен: {e}"))
        return

    def fmt(v): return "—" if v is None else str(v)
    await msg.answer(
        _("admin_user_price_saved_ok",
          default=(
              "✅ Цены сохранены для пользователя {uid}:\n"
              "RUB: 1m={r1}, 3m={r3}, 6m={r6}, 12m={r12}\n"
              "Stars: 1m={s1}, 3m={s3}, 6m={s6}, 12m={s12}"
          )).format(
            uid=user_id,
            r1=plan.rub_1m, r3=plan.rub_3m, r6=plan.rub_6m, r12=plan.rub_12m,
            s1=fmt(plan.stars_1m), s3=fmt(plan.stars_3m), s6=fmt(plan.stars_6m), s12=fmt(plan.stars_12m),
        )
    )
    await state.clear()
