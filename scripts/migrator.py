# scripts/migrator.py
import asyncio
import logging
import sys

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.exc import SQLAlchemyError

# импортируй свой Base и модели
from db.models import Base  # Base = declarative_base() в твоих моделях
from config.settings import Settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - migrator - %(levelname)s - %(message)s",
)

async def run():
    settings = Settings()  # возьмёт POSTGRES_* из .env; DATABASE_URL собирается в @property
    engine = create_async_engine(settings.DATABASE_URL, echo=False, future=True)

    logging.info("Connecting to DB and creating tables if not exist...")
    try:
        async with engine.begin() as conn:
            # ВАЖНО: run_sync позволяет вызвать sync-методы над async-connection
            await conn.run_sync(Base.metadata.create_all)
        logging.info("✅ Schema created/verified successfully.")
    except SQLAlchemyError as e:
        logging.exception("❌ Migration failed:")
        await engine.dispose()
        # ненулевой код — чтобы depends_on не пропустил основное приложение
        sys.exit(1)
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(run())
