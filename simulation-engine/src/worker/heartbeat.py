"""
Worker heartbeat manager.

Maintains a Redis key with TTL that proves the worker is alive.
The reaper checks these keys to detect dead workers. If a worker
stops refreshing its heartbeat, the key expires and the reaper
knows to reclaim its jobs.
"""

import uuid
from datetime import datetime, timezone, timedelta

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.repositories.job_repo import update_job_lease
from src.config import settings

import structlog

logger = structlog.get_logger(__name__)


def heartbeat_key(worker_id: str) -> str:
    """Build the Redis key used for this worker's heartbeat."""
    return f"worker:{worker_id}:heartbeat"


async def refresh_heartbeat(
    redis_client: redis.Redis,
    worker_id: str,
) -> None:
    """
    Set/refresh the worker's heartbeat key in Redis with a TTL.

    The TTL equals the lease timeout, so the key expires around
    the same time the lease would go stale if not refreshed.

    Args:
        redis_client: Active async Redis client.
        worker_id: Unique identifier of this worker.
    """
    key = heartbeat_key(worker_id)
    await redis_client.set(
        key,
        "alive",
        ex=settings.worker_lease_timeout_seconds,
    )


async def refresh_job_lease(
    session: AsyncSession,
    job_id: uuid.UUID,
) -> None:
    """
    Extend a job's lease expiration in PostgreSQL.

    Called alongside the Redis heartbeat refresh. The reaper uses
    lease_expires_at to detect stale running jobs, so this must
    be updated regularly while the job is executing.

    Args:
        session: Active async database session.
        job_id: UUID of the running job.
    """
    new_expiry = datetime.now(timezone.utc) + timedelta(
        seconds=settings.worker_lease_timeout_seconds
    )
    await update_job_lease(session, job_id, new_expiry)


async def remove_heartbeat(
    redis_client: redis.Redis,
    worker_id: str,
) -> None:
    """
    Delete the worker's heartbeat key from Redis.

    Called during graceful shutdown or when a worker finishes a job.

    Args:
        redis_client: Active async Redis client.
        worker_id: Unique identifier of this worker.
    """
    key = heartbeat_key(worker_id)
    await redis_client.delete(key)
