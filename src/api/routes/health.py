"""
Health check endpoint.

Reports the connectivity status of PostgreSQL and Redis,
allowing load balancers and monitoring systems to detect
when the service is degraded or unhealthy.
"""

from datetime import datetime, timezone

import redis.asyncio as redis
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import get_db, get_redis

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    """Health check result showing infrastructure connectivity."""

    status: str          # "healthy", "degraded", "unhealthy"
    postgres: bool
    redis: bool
    timestamp: datetime


@router.get("/health", response_model=HealthResponse)
async def health_check(
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis),
) -> HealthResponse:
    """
    Check connectivity to PostgreSQL and Redis.

    Returns 'healthy' if both are reachable, 'degraded' if one is down,
    and 'unhealthy' if both are unreachable.
    """
    pg_ok = False
    redis_ok = False

    # Test PostgreSQL connectivity
    try:
        await db.execute(text("SELECT 1"))
        pg_ok = True
    except Exception:
        pass

    # Test Redis connectivity
    try:
        await redis_client.ping()
        redis_ok = True
    except Exception:
        pass

    # Determine overall status
    if pg_ok and redis_ok:
        status = "healthy"
    elif pg_ok or redis_ok:
        status = "degraded"
    else:
        status = "unhealthy"

    return HealthResponse(
        status=status,
        postgres=pg_ok,
        redis=redis_ok,
        timestamp=datetime.now(timezone.utc),
    )
