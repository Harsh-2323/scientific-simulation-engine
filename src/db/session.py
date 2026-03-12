"""
Async SQLAlchemy engine and session factory.

Provides a single async engine connected to PostgreSQL via asyncpg,
and an async session factory for use in API routes, workers, and
background processes.
"""

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.config import settings

# Async engine using asyncpg driver
engine = create_async_engine(
    settings.database_url,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_pool_max_overflow,
    echo=False,
)

# Session factory — produces AsyncSession instances bound to the engine
async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_session() -> AsyncSession:
    """
    Yield an async database session.

    Intended for use as a FastAPI dependency or in async context managers.
    The session is committed on success and rolled back on exception.

    Yields:
        AsyncSession: A fresh database session.
    """
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
