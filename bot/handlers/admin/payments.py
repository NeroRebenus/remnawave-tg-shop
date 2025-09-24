# bot/handlers/admin/payments.py
import logging
import csv
import io
import json
from aiogram import Router, F, types, Bot
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Any, Dict
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import Settings
from db.dal import payment_dal
from db.models import Payment
from bot.keyboards.inline.admin_keyboards import get_back_to_admin_panel_keyboard
from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardButton
from bot.middlewares.i18n import JsonI18n

router = Router(name="admin_payments_router")


# ----------------------------- helpers -----------------------------

def _safe_load_metadata(payment: Payment) -> Dict[str, Any]:
    """
    Пытаемся аккуратно получить метаданные платежа (если в модели они есть).
    Поддерживаем несколько возможных вариантов хранения:
    - payment.metadata (dict) — уже разобранный JSON
    - payment.metadata_json / payment.meta / payment.raw_metadata (str|dict)
    Если поля нет — возвращаем пустой dict.
    """
    candidates = [
        getattr(payment, "metadata", None),
        getattr(payment, "metadata_json", None),
        getattr(payment, "meta", None),
        getattr(payment, "raw_metadata", None),
    ]
    for cand in candidates:
        if cand is None:
            continue
        if isinstance(cand, dict):
            return cand
        if isinstance(cand, str):
            try:
                return json.loads(cand)
            except Exception:
                # бывает, что там просто строка — тогда пропускаем
                continue
    return {}


async def get_payments_with_pagination(session: AsyncSession, page: int = 0,
                                       page_size: int = 10) -> tuple[List[Payment], int]:
    """Get payments with pagination and total count."""
    offset = page * page_size

    # Get total count
    total_count = await payment_dal.get_payments_count(session)

    # Get payments for current page
    payments = await payment_dal.get_recent_payment_logs_with_user(
        session, limit=page_size, offset=offset
    )

    return payments, total_count

# --- NEW: admin_link by panel username -> extend in Panel
async def process_adminlink_panel_username_payment(
    payment_info_from_webhook: dict,
    panel_service,
    settings: Settings,
    bot: Bot,
) -> bool:
    """
    Автоматическое продление подписки в ПАНЕЛИ по никe (username) из metadata.
    Возвращает True при успешном продлении в панели.
    """
    md = payment_info_from_webhook.get("metadata", {}) or {}
    source = (md.get("source") or "").strip().lower()
    id_type = (md.get("id_type") or "").strip().lower()
    username_raw = (md.get("username") or md.get("panel_username") or "").strip()

    if source != "admin_link" or not username_raw:
        return False
    if id_type not in {"panel_username", "username", ""}:
        # не наш режим
        return False

    # Нормализуем ник панели: без @ и в нижнем регистре
    panel_username = username_raw.lstrip("@").lower()

    # Месяцы: либо months, либо period ("3m" и т.п.)
    months = 0
    months_raw = md.get("months")
    try:
        months = int(str(months_raw)) if months_raw is not None else 0
    except Exception:
        months = 0
    if months <= 0:
        period = str(md.get("period") or "").lower().strip()
        months = {"1m": 1, "3m": 3, "6m": 6, "12m": 12}.get(period, 0)

    if months <= 0:
        logging.error("AdminLink(panel_username): bad months/period in metadata: %s", md)
        return False

    yk_payment_id = payment_info_from_webhook.get("id")
    amount = (payment_info_from_webhook.get("amount") or {}).get("value")
    currency = (payment_info_from_webhook.get("amount") or {}).get("currency")

    # Пытаемся продлить в панели по нику
    try:
        ok = False
        # Предпочтительно — прямой метод
        if hasattr(panel_service, "extend_subscription_by_username"):
            ok = await panel_service.extend_subscription_by_username(
                username=panel_username,
                months=months,
                reason=f"YK admin_link {yk_payment_id}",
            )
        else:
            # Fallback: найти uuid по нику и продлить по uuid
            panel_uuid = None
            if hasattr(panel_service, "get_user_uuid_by_username"):
                panel_uuid = await panel_service.get_user_uuid_by_username(panel_username)
            if not panel_uuid:
                logging.error("AdminLink(panel_username): panel user not found by username=%s", panel_username)
                return False

            if hasattr(panel_service, "extend_subscription_by_uuid"):
                ok = await panel_service.extend_subscription_by_uuid(
                    user_uuid=panel_uuid,
                    months=months,
                    reason=f"YK admin_link {yk_payment_id}",
                )
            else:
                # Последний шанс: общий extend (если есть)
                if hasattr(panel_service, "extend_subscription"):
                    ok = await panel_service.extend_subscription(
                        identifier=panel_uuid,
                        months=months,
                        reason=f"YK admin_link {yk_payment_id}",
                    )
                else:
                    logging.error("AdminLink(panel_username): no suitable PanelApiService method to extend")
                    return False

        if not ok:
            logging.error("AdminLink(panel_username): panel extension returned False for @%s", panel_username)
            return False

        # Лог-уведомление в служебный чат
        try:
            if getattr(settings, "LOG_CHAT_ID", None):
                await bot.send_message(
                    int(settings.LOG_CHAT_ID),
                    (
                        "✅ Продлена подписка в ПАНЕЛИ (admin_link)\n"
                        f"👤 @{panel_username}\n"
                        f"🕒 +{months} мес.\n"
                        f"💳 {amount} {currency}\n"
                        f"🧾 YK: <code>{yk_payment_id}</code>"
                    ),
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
        except Exception:
            # не критично
            pass

        logging.info("AdminLink(panel_username): extended in panel: @%s +%s mo (YK %s)", panel_username, months, yk_payment_id)
        return True

    except Exception:
        logging.exception("AdminLink(panel_username): panel extension failed for @%s", panel_username)
        return False

def format_payment_text(payment: Payment, i18n: JsonI18n, lang: str) -> str:
    """Format single payment info as text."""
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)

    status_emoji = "✅" if payment.status == 'succeeded' else (
        "⏳" if payment.status in ['pending', 'pending_yookassa'] else "❌"
    )

    # Попробуем дополнить информацию о пользователе данными из metadata (для admin_link по нику панели)
    md = _safe_load_metadata(payment)
    panel_username = (md.get("username") or md.get("panel_username") or "")
    id_type = (md.get("id_type") or "").lower()
    src = (md.get("source") or "").lower()

    # Базовая строка о пользователе
    user_info = f"User {payment.user_id}"
    if payment.user and payment.user.username:
        user_info += f" (@{payment.user.username})"
    elif payment.user and payment.user.first_name:
        user_info += f" ({payment.user.first_name})"

    # Если это платёж из админ-ссылки по нику панели — покажем его явно
    if src == "admin_link" and id_type in {"panel_username", "username"} and panel_username:
        user_info += f" • panel:@{panel_username}"

    payment_date = payment.created_at.strftime('%Y-%m-%d %H:%M') if payment.created_at else "N/A"

    provider_text = {
        'yookassa': 'YooKassa',
        'tribute': 'Tribute',
        'telegram_stars': 'Telegram Stars',
        'cryptopay': 'CryptoPay'
    }.get(payment.provider, payment.provider or 'Unknown')

    # Если в метаданных есть период/месяцы — красиво покажем
    months = md.get("months")
    period = md.get("period")
    period_hint = ""
    if isinstance(months, (int, str)) and str(months).isdigit():
        period_hint = f"\n🕒 {months} mo."
    elif isinstance(period, str) and period:
        period_hint = f"\n🕒 {period}"

    desc = payment.description or 'N/A'

    return (
        f"{status_emoji} <b>{payment.amount} {payment.currency}</b>\n"
        f"👤 {user_info}\n"
        f"💳 {provider_text}\n"
        f"📅 {payment_date}\n"
        f"📋 {payment.status}{period_hint}\n"
        f"📝 {desc}"
    )


# ----------------------------- views -----------------------------

async def view_payments_handler(callback: types.CallbackQuery, i18n_data: dict,
                                settings: Settings, session: AsyncSession, page: int = 0):
    """Display paginated list of all payments."""
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n or not callback.message:
        await callback.answer("Error processing request.", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    page_size = 5  # Show 5 payments per page
    payments, total_count = await get_payments_with_pagination(session, page, page_size)
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1

    if not payments and page == 0:
        await callback.message.edit_text(
            _("admin_no_payments_found", default="Платежи не найдены."),
            reply_markup=get_back_to_admin_panel_keyboard(current_lang, i18n),
            parse_mode="HTML"
        )
        await callback.answer()
        return

    # Format payments text
    text_parts = [_("admin_payments_header", default="💰 <b>Все платежи</b>")]
    text_parts.append(_("admin_payments_pagination_info",
                        shown=len(payments),
                        total=total_count,
                        current_page=page + 1,
                        total_pages=total_pages) + "\n")

    for i, payment in enumerate(payments, 1):
        text_parts.append(f"<b>{page * page_size + i}.</b> {format_payment_text(payment, i18n, current_lang)}")
        text_parts.append("")  # Empty line between payments

    # Build keyboard with pagination and export
    builder = InlineKeyboardBuilder()

    # Pagination buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"payments_page:{page-1}"))

    nav_buttons.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))

    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"payments_page:{page+1}"))

    if nav_buttons:
        builder.row(*nav_buttons)

    # Export and refresh buttons
    builder.row(
        InlineKeyboardButton(
            text=_("admin_export_payments_csv", default="📊 Экспорт CSV"),
            callback_data="payments_export_csv"
        ),
        InlineKeyboardButton(
            text=_("admin_refresh_payments", default="🔄 Обновить"),
            callback_data=f"payments_page:{page}"
        )
    )

    # Back button
    builder.row(InlineKeyboardButton(
        text=_("back_to_admin_panel_button"),
        callback_data="admin_section:stats_monitoring"
    ))

    await callback.message.edit_text(
        "\n".join(text_parts),
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("payments_page:"))
async def payments_pagination_handler(callback: types.CallbackQuery, i18n_data: dict,
                                      settings: Settings, session: AsyncSession):
    """Handle pagination for payments list."""
    try:
        page = int(callback.data.split(":")[1])
        await view_payments_handler(callback, i18n_data, settings, session, page)
    except (ValueError, IndexError):
        await callback.answer("Error processing pagination.", show_alert=True)


@router.callback_query(F.data == "payments_export_csv")
async def export_payments_csv_handler(callback: types.CallbackQuery, i18n_data: dict,
                                      settings: Settings, session: AsyncSession):
    """Export all successful payments to CSV file."""
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n:
        await callback.answer("Language service error.", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    try:
        # Get all successful payments
        all_payments = await payment_dal.get_all_succeeded_payments_with_user(session)

        if not all_payments:
            await callback.answer(
                _("admin_no_payments_to_export", default="Нет платежей для экспорта."),
                show_alert=True
            )
            return

        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header (добавим столбцы для admin_link по нику панели)
        writer.writerow([
            _("admin_csv_payment_id", default="ID"),
            _("admin_csv_user_id", default="User ID"),
            _("admin_csv_username", default="Username"),
            _("admin_csv_first_name", default="First Name"),
            _("admin_csv_amount", default="Amount"),
            _("admin_csv_currency", default="Currency"),
            _("admin_csv_provider", default="Provider"),
            _("admin_csv_status", default="Status"),
            _("admin_csv_description", default="Description"),
            _("admin_csv_months", default="Months"),
            _("admin_csv_created_at", default="Created At"),
            _("admin_csv_provider_payment_id", default="Provider Payment ID"),
            "Source",               # new
            "ID Type",              # new
            "Panel Username",       # new
            "Period"                # new
        ])

        # Write payment data
        for payment in all_payments:
            md = _safe_load_metadata(payment)
            panel_username = md.get("username") or md.get("panel_username") or ""
            months = md.get("months") or ""
            period = md.get("period") or ""
            src = md.get("source") or ""
            id_type = md.get("id_type") or ""

            writer.writerow([
                payment.payment_id,
                payment.user_id,
                payment.user.username if payment.user and payment.user.username else "",
                payment.user.first_name if payment.user and payment.user.first_name else "",
                payment.amount,
                payment.currency,
                payment.provider or "",
                payment.status,
                payment.description or "",
                payment.subscription_duration_months or months or "",
                payment.created_at.strftime('%Y-%m-%d %H:%M:%S') if payment.created_at else "",
                payment.provider_payment_id or "",
                src,
                id_type,
                panel_username,
                period,
            ])

        # Prepare file
        csv_content = output.getvalue().encode('utf-8-sig')  # UTF-8 with BOM for Excel
        output.close()

        # Generate filename with current date
        current_time = datetime.now().strftime('%Y-%m-%d_%H-%M')
        filename = f"payments_export_{current_time}.csv"

        # Send file
        from aiogram.types import BufferedInputFile
        file = BufferedInputFile(csv_content, filename=filename)

        await callback.message.reply_document(
            document=file,
            caption=_("admin_payments_export_success",
                      default="📊 Payments export completed!\nTotal records: {count}",
                      count=len(all_payments))
        )

        await callback.answer(
            _("admin_export_sent", default="File sent!"),
            show_alert=False
        )

    except Exception as e:
        logging.error(f"Failed to export payments CSV: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка экспорта: {str(e)}", show_alert=True)


@router.callback_query(F.data == "noop")
async def noop_handler(callback: types.CallbackQuery):
    """Handle no-op callback (for pagination display)."""
    await callback.answer()
