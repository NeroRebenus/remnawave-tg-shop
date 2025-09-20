import os
import asyncio
import logging
from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from sqlalchemy.orm import sessionmaker

from config.settings import Settings
from bot.services.ferma_webhook_service import make_ferma_callback_handler  # <-- NEW


async def build_and_start_web_app(
    dp: Dispatcher,
    bot: Bot,
    settings: Settings,
    async_session_factory: sessionmaker,
):
    # ограничим размер тела запроса (на всякий случай)
    app = web.Application(client_max_size=256 * 1024)

    app["bot"] = bot
    app["dp"] = dp
    app["settings"] = settings
    app["async_session_factory"] = async_session_factory
    # Inject shared instances used by webhook handlers
    app["i18n"] = dp.get("i18n_instance")
    for key in (
        "yookassa_service",
        "subscription_service",
        "referral_service",
        "panel_service",
        "stars_service",
        "cryptopay_service",
        "tribute_service",
        "panel_webhook_service",
    ):
        if hasattr(dp, "workflow_data") and key in dp.workflow_data:  # type: ignore
            app[key] = dp.workflow_data[key]  # type: ignore

    # Регистрируем aiogram-хендлеры
    setup_application(app, dp, bot=bot)

    telegram_uses_webhook_mode = bool(settings.WEBHOOK_BASE_URL)
    if telegram_uses_webhook_mode:
        telegram_webhook_path = f"/{settings.BOT_TOKEN}"
        # важно: add_post ждёт path и handler
        app.router.add_post(telegram_webhook_path, SimpleRequestHandler(dispatcher=dp, bot=bot))
        # маскируем токен в логах
        masked = telegram_webhook_path[:5] + "..." if len(telegram_webhook_path) > 8 else "***"
        logging.info(f"Telegram webhook route configured at: [POST] {masked} (relative to base URL)")

    # --- прочие вебхуки твоего проекта ---
    from bot.handlers.user.payment import yookassa_webhook_route
    from bot.services.tribute_service import tribute_webhook_route
    from bot.services.crypto_pay_service import cryptopay_webhook_route
    from bot.services.panel_webhook_service import panel_webhook_route

    tribute_path = settings.tribute_webhook_path
    if tribute_path.startswith("/"):
        app.router.add_post(tribute_path, tribute_webhook_route)
        logging.info(f"Tribute webhook route configured at: [POST] {tribute_path}")

    cp_path = settings.cryptopay_webhook_path
    if cp_path.startswith("/"):
        app.router.add_post(cp_path, cryptopay_webhook_route)
        logging.info(f"CryptoPay webhook route configured at: [POST] {cp_path}")

    # YooKassa webhook
    yk_path = settings.yookassa_webhook_path
    if settings.WEBHOOK_BASE_URL and yk_path and yk_path.startswith("/"):
        app.router.add_post(yk_path, yookassa_webhook_route)
        logging.info(f"YooKassa webhook route configured at: [POST] {yk_path}")

    panel_path = settings.panel_webhook_path
    if panel_path.startswith("/"):
        app.router.add_post(panel_path, panel_webhook_route)
        logging.info(f"Panel webhook route configured at: [POST] {panel_path}")

    # --- Ferma OFD callback (ЭТО ГЛАВНОЕ) ---
    # Используем фабрику, которая вернёт корректный aiohttp-хендлер под add_post
    ferma_path = settings.ferma_callback_path  # из Settings (нормализованный путь)
    if ferma_path.startswith("/"):
        ferma_handler = make_ferma_callback_handler(async_session_factory)
        app.router.add_post(ferma_path, ferma_handler)
        logging.info(f"Ferma webhook route configured at: [POST] {ferma_path}")
    else:
        logging.error("FERMA_CALLBACK_PATH must start with '/'. Skipping Ferma route registration.")

    # --- запуск AIOHTTP ---
    web_app_runner = web.AppRunner(app)
    await web_app_runner.setup()
    site = web.TCPSite(
        web_app_runner,
        host=settings.WEB_SERVER_HOST,
        port=settings.WEB_SERVER_PORT,
    )
    await site.start()
    logging.info(f"AIOHTTP server started on http://{settings.WEB_SERVER_HOST}:{settings.WEB_SERVER_PORT}")

    # Run until cancelled
    await asyncio.Event().wait()