"""
FastAPI dependency injection providers.

Provides database sessions and Redis clients as injectable
dependencies for route handlers. These are the standard way
to access infrastructure in FastAPI endpoints.
"""

from typing import AsyncGenerator

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db.session import async_session_factory

# Module-level Redis client — initialized on app startup, closed on shutdown
_redis_client: redis.Redis | None = None


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Yield an async database session for a single request.

    The session auto-commits on success and rolls back on exception.
    Used as a FastAPI Depends() dependency in route handlers.

    Yields:
        AsyncSession: A scoped database session.
    """
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_redis() -> redis.Redis:
    """
    Return the shared Redis client instance.

    The client is initialized once on app startup via init_redis()
    and reused across all requests for connection pooling.

    Returns:
        Redis async client connected to the configured Redis URL.

    Raises:
        RuntimeError: If called before init_redis() during startup.
    """
    if _redis_client is None:
        raise RuntimeError("Redis client not initialized. Call init_redis() on app startup.")
    return _redis_client


async def init_redis() -> None:
    """
    Initialize the module-level Redis client.

    Called once during FastAPI's lifespan startup event.
    Creates a connection pool using the configured Redis URL.
    """
    global _redis_client
    _redis_client = redis.from_url(
        settings.redis_url,
        decode_responses=True,
    )


async def close_redis() -> None:
    """
    Close the Redis client and release the connection pool.

    Called during FastAPI's lifespan shutdown event.
    """
    global _redis_client
    if _redis_client is not None:
        await _redis_client.close()
        _redis_client = None
