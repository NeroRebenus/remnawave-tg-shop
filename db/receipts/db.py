# db/receipts/db.py
from __future__ import annotations
from contextlib import asynccontextmanager
from typing import Optional

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

try:
    # Бери DSN из общих настроек (Postgres)
    from config.settings import get_settings
    _SETTINGS = get_settings()
    _DATABASE_URL = _SETTINGS.DATABASE_URL  # напр. postgresql+asyncpg://...
except Exception:
    # последний fallback: если вдруг Settings недоступен
    import os
    _DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./app.db")

# Глобальный фабричный метод сессий; по умолчанию создадим свой,
# но ниже дадим API, чтобы подменить на общий из приложения.
_engine = create_async_engine(
    _DATABASE_URL,
    future=True,
    pool_pre_ping=True,
)
AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=_engine,
    expire_on_commit=False,
    autoflush=False,
    class_=AsyncSession,
)

def set_session_factory(factory: async_sessionmaker[AsyncSession]) -> None:
    """
    Позволяет приложению заменить фабрику сессий на общую (из init_db_connection).
    Обязательно вызови это на старте, чтобы fallback-задачи и вебхуки Ferma работали с той же БД.
    """
    global AsyncSessionLocal
    AsyncSessionLocal = factory

@asynccontextmanager
async def get_session() -> AsyncSession:
    """
    Контекстный менеджер для получения сессии.
    Работает с тем фабричным методом, который установлен в AsyncSessionLocal.
    """
    session = AsyncSessionLocal()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()