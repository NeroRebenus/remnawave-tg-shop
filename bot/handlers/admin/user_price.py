from __future__ import annotations

from typing import Optional, Dict, Tuple

from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import Settings
from bot.middlewares.i18n import JsonI18n
from db.dal import user_dal
from db.dal.pricing import get_or_init_user_price_plan, update_user_price_plan
from bot.keyboards.inline.admin_keyboards import get_back_to_admin_panel_keyboard

router = Router(name="admin_user_price_router")


class AdminPriceStates(StatesGroup):
    waiting_user_query = State()   # <-- ID ИЛИ @username
    waiting_value = State()        # <-- ввод конкретной цены (rub/stars + период)


def _t(i18n_data: dict, settings: Settings):
    lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    return (lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)) if i18n else (lambda key, **kwargs: key)


# ------------ UI helpers ------------

def _fmt_currency(v: Optional[int]) -> str:
    return "—" if v is None else str(v)

def _rub_button_text(period_ru: str, amount: Optional[int]) -> str:
    return f"{period_ru}: {_fmt_currency(amount)} ₽"

def _stars_button_text(period_ru: str, amount: Optional[int]) -> str:
    return f"{period_ru}: {_fmt_currency(amount)} ⭐"

def _period_human(p: str) -> str:
    return {"1m":"1 мес","3m":"3 мес","6m":"6 мес","12m":"12 мес"}.get(p, p)

def _edit_cb(user_id: int, kind: str, period: str) -> str:
    # kind: "rub" | "stars"
    return f"price:edit:{kind}:{period}:{user_id}"

def _back_to_prices_cb(user_id: int) -> str:
    return f"price:back:{user_id}"

async def _send_start_prompt(target: types.Message | types.CallbackQuery, i18n_data: dict, settings: Settings):
    _ = _t(i18n_data, settings)
    text = _(
        "admin_user_price_enter_query",
        default=("👤 Редактирование индивидуальных цен\n\n"
                 "Введите <b>Telegram ID</b> или <b>@username</b> пользователя.\n\n"
                 "Примеры:\n"
                 "• <code>123456789</code>\n"
                 "• <code>@john_doe</code>")
    )
    kb = get_back_to_admin_panel_keyboard(
        i18n_data.get("current_language", settings.DEFAULT_LANGUAGE),
        i18n_data.get("i18n_instance"),
    )
    msg = target.message if isinstance(target, types.CallbackQuery) else target
    try:
        await msg.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await msg.answer(text, reply_markup=kb, parse_mode="HTML")


async def _price_menu_markup(i18n_data: dict, settings: Settings, plan, user_id: int) -> types.InlineKeyboardMarkup:
    from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardButton
    _ = _t(i18n_data, settings)
    lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n = i18n_data.get("i18n_instance")

    b = InlineKeyboardBuilder()

    # Заголовки
    b.row(InlineKeyboardButton(text=_("admin_user_price_section_rub", default="💰 RUB"), callback_data="noop"))
    b.row(
        InlineKeyboardButton(text=_rub_button_text("1 мес", plan.rub_1m), callback_data=_edit_cb(user_id, "rub", "1m")),
        InlineKeyboardButton(text=_rub_button_text("3 мес", plan.rub_3m), callback_data=_edit_cb(user_id, "rub", "3m")),
    )
    b.row(
        InlineKeyboardButton(text=_rub_button_text("6 мес", plan.rub_6m), callback_data=_edit_cb(user_id, "rub", "6m")),
        InlineKeyboardButton(text=_rub_button_text("12 мес", plan.rub_12m), callback_data=_edit_cb(user_id, "rub", "12m")),
    )

    b.row(InlineKeyboardButton(text=_("admin_user_price_section_stars", default="⭐ Stars"), callback_data="noop"))
    b.row(
        InlineKeyboardButton(text=_stars_button_text("1 мес", plan.stars_1m), callback_data=_edit_cb(user_id, "stars", "1m")),
        InlineKeyboardButton(text=_stars_button_text("3 мес", plan.stars_3m), callback_data=_edit_cb(user_id, "stars", "3m")),
    )
    b.row(
        InlineKeyboardButton(text=_stars_button_text("6 мес", plan.stars_6m), callback_data=_edit_cb(user_id, "stars", "6m")),
        InlineKeyboardButton(text=_stars_button_text("12 мес", plan.stars_12m), callback_data=_edit_cb(user_id, "stars", "12m")),
    )

    # Низ — назад в админку
    back_admin = get_back_to_admin_panel_keyboard(lang, i18n)
    for row in back_admin.inline_keyboard:
        b.row(*row)

    return b.as_markup()


async def _show_price_menu(msg_or_cb: types.Message | types.CallbackQuery, i18n_data: dict, settings: Settings, session: AsyncSession, user_id: int):
    plan = await get_or_init_user_price_plan(session, user_id=user_id)
    kb = await _price_menu_markup(i18n_data, settings, plan, user_id)
    _ = _t(i18n_data, settings)
    title = _("admin_user_price_menu_title", user_id = user_id)
    msg = msg_or_cb.message if isinstance(msg_or_cb, types.CallbackQuery) else msg_or_cb
    try:
        await msg.edit_text(title, reply_markup=kb)
    except Exception:
        await msg.answer(title, reply_markup=kb)


# ------------ Flow ------------

@router.callback_query(F.data == "admin_action:user_price_prompt")
async def admin_user_price_prompt(
    cb: types.CallbackQuery,
    state: FSMContext,
    i18n_data: dict,
    settings: Settings,
):
    await state.set_state(AdminPriceStates.waiting_user_query)
    await _send_start_prompt(cb, i18n_data, settings)
    await cb.answer()


@router.message(AdminPriceStates.waiting_user_query)
async def admin_user_price_got_query(
    msg: types.Message,
    state: FSMContext,
    session: AsyncSession,
    i18n_data: dict,
    settings: Settings,
):
    _ = _t(i18n_data, settings)
    text = (msg.text or "").strip()

    # распознаём id / @username
    user_id: Optional[int] = None
    if text.isdigit():
        user_id = int(text)
    elif text.startswith("@") and len(text) > 1:
        user = await user_dal.get_user_by_username(session, text[1:])
        if user:
            user_id = user.user_id
    else:
        # попробуем как username без @
        user = await user_dal.get_user_by_username(session, text)
        if user:
            user_id = user.user_id

    if not user_id:
        await msg.answer(_("admin_user_price_bad_id", default="Некорректный ID/username. Введите число или @username."))
        return

    # Ensure user exists (если нет — создаём минимальную запись)
    db_user = await user_dal.get_user_by_id(session, user_id)
    if not db_user:
        data = {
            "user_id": user_id,
            "username": None,
            "first_name": None,
            "last_name": None,
            "language_code": i18n_data.get("current_language", settings.DEFAULT_LANGUAGE),
            "referred_by_id": None,
        }
        try:
            await user_dal.create_user(session, data)
            await session.flush()
        except Exception as e:
            await msg.answer(_("admin_user_price_create_user_failed", default=f"Ошибка создания пользователя: {e}"))
            return

    # Ensure plan
    await get_or_init_user_price_plan(session, user_id=user_id)
    await session.flush()

    # Переходим к меню цен
    await state.update_data(target_user_id=user_id)
    await _show_price_menu(msg, i18n_data, settings, session, user_id)


@router.callback_query(F.data.startswith("price:edit:"))
async def price_edit_prompt(
    cb: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    i18n_data: dict,
    settings: Settings,
):
    # format: price:edit:{kind}:{period}:{user_id}
    try:
        _, _, kind, period, user_id_str = cb.data.split(":")
        user_id = int(user_id_str)
        assert kind in {"rub", "stars"}
        assert period in {"1m", "3m", "6m", "12m"}
    except Exception:
        try:
            await cb.answer("Bad payload", show_alert=True)
        except Exception:
            pass
        return

    _ = _t(i18n_data, settings)

    await state.set_state(AdminPriceStates.waiting_value)
    await state.update_data(target_user_id=user_id, kind=kind, period=period)

    hint = _("admin_user_price_enter_value_hint",
             default=("Введите <b>целое число</b> для цены "
                      "или отправьте <code>clear</code>, чтобы очистить (только для Stars)."))
    title = _("admin_user_price_edit_title", default=f"✏️ {_period_human(period)} — {('RUB' if kind=='rub' else 'Stars')}")
    text = f"{title}\n\n{hint}"

    from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardButton
    b = InlineKeyboardBuilder()
    # быстрые пресеты для удобства (можно подстроить под себя)
    if kind == "rub":
        for v in (500, 900, 1200, 1500, 3000):
            b.button(text=str(v), callback_data=f"price:preset:{v}")
        b.adjust(5)
    else:
        for v in (50, 100, 120, 150, 300):
            b.button(text=str(v), callback_data=f"price:preset:{v}")
        # и кнопка очистить для Stars
        b.button(text=_("admin_user_price_clear_button", default="Очистить"), callback_data="price:preset:clear")
        b.adjust(5, 1)

    # назад к списку цен
    b.row(InlineKeyboardButton(text=_("admin_user_price_back_to_menu", default="⬅️ К ценам"),
                               callback_data=_back_to_prices_cb(user_id)))
    # и “в админку”
    back_admin = get_back_to_admin_panel_keyboard(i18n_data.get("current_language", settings.DEFAULT_LANGUAGE),
                                                  i18n_data.get("i18n_instance"))
    for row in back_admin.inline_keyboard:
        b.row(*row)

    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup(), parse_mode="HTML")
    except Exception:
        await cb.message.answer(text, reply_markup=b.as_markup(), parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data.startswith("price:preset:"))
async def price_preset_click(
    cb: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    i18n_data: dict,
    settings: Settings,
):
    data = await state.get_data()
    user_id = data.get("target_user_id")
    kind = data.get("kind")
    period = data.get("period")
    if not (user_id and kind and period):
        await cb.answer("State lost", show_alert=True)
        return

    preset = cb.data.split(":")[-1]
    await _apply_value_and_show_menu(cb, state, session, i18n_data, settings, user_id, kind, period, preset)


@router.callback_query(F.data.startswith("price:back:"))
async def price_back_to_menu(
    cb: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    i18n_data: dict,
    settings: Settings,
):
    try:
        _, _, user_id_str = cb.data.split(":")
        user_id = int(user_id_str)
    except Exception:
        user_id = (await state.get_data()).get("target_user_id")

    await state.set_state(AdminPriceStates.waiting_user_query)  # по факту мы не ждём ввод; просто держим “внутри” сценария
    await _show_price_menu(cb, i18n_data, settings, session, user_id)
    await cb.answer()


@router.message(AdminPriceStates.waiting_value)
async def price_value_typed(
    msg: types.Message,
    state: FSMContext,
    session: AsyncSession,
    i18n_data: dict,
    settings: Settings,
):
    data = await state.get_data()
    user_id = data.get("target_user_id")
    kind = data.get("kind")
    period = data.get("period")
    if not (user_id and kind and period):
        await msg.answer("State lost. Start again.")
        await state.clear()
        return

    await _apply_value_and_show_menu(msg, state, session, i18n_data, settings, user_id, kind, period, (msg.text or "").strip())


async def _apply_value_and_show_menu(
    msg_or_cb: types.Message | types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    i18n_data: dict,
    settings: Settings,
    user_id: int,
    kind: str,
    period: str,
    raw_value: str,
):
    _ = _t(i18n_data, settings)
    # Нормализуем
    new_val: Optional[int]
    if kind == "stars" and raw_value.lower() == "clear":
        new_val = None
    else:
        if not raw_value.isdigit():
            err = _("admin_user_price_parse_error", default="Ошибка: нужно целое число или 'clear' для Stars.")
            if isinstance(msg_or_cb, types.CallbackQuery):
                try:
                    await msg_or_cb.answer(err, show_alert=True)
                except Exception:
                    pass
            else:
                await msg_or_cb.answer(err)
            return
        new_val = int(raw_value)

    # Маппим поле
    field = f"{'rub' if kind=='rub' else 'stars'}_{period}"
    try:
        kwargs = {field: new_val}
        await update_user_price_plan(
            session,
            user_id=user_id,
            created_by_admin_id=(msg_or_cb.from_user.id if isinstance(msg_or_cb, (types.CallbackQuery, types.Message)) else None),
            **kwargs,
        )
        await session.commit()
    except Exception as e:
        await session.rollback()
        txt = _("admin_user_price_save_failed", default=f"Ошибка сохранения цен: {e}")
        if isinstance(msg_or_cb, types.CallbackQuery):
            try:
                await msg_or_cb.answer(txt, show_alert=True)
            except Exception:
                pass
        else:
            await msg_or_cb.answer(txt)
        return

    # Показать обновлённое меню
    await state.set_state(AdminPriceStates.waiting_user_query)
    await _show_price_menu(msg_or_cb, i18n_data, settings, session, user_id)
    if isinstance(msg_or_cb, types.CallbackQuery):
        try:
            await msg_or_cb.answer(_("admin_user_price_saved_ok_toast", default="Сохранено"))
        except Exception:
            pass