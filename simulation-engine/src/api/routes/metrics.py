"""
Metrics API route — system health snapshot for operators.

Returns queue depths, worker counts, job throughput, and failure
rates. Designed for monitoring dashboards and alerting systems.
"""

from datetime import datetime, timezone, timedelta

import redis.asyncio as redis
from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import get_db, get_redis
from src.models.job import Job
from src.models.worker import WorkerRegistry
from src.queue.redis_stream import get_queue_depths
from src.schemas.metrics import MetricsResponse

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("", response_model=MetricsResponse)
async def get_metrics(
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis),
) -> MetricsResponse:
    """
    Get a snapshot of system-wide operational metrics.

    Queries both Postgres (job counts, worker counts) and Redis
    (queue depths) to build a complete picture of system health.
    """
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)

    # Query Redis for queue depths across all priority streams
    queue_depth = await get_queue_depths(redis_client)
    total_queue_depth = sum(queue_depth.values())

    # Count active workers (status is 'busy', not 'idle' or 'stopped')
    active_workers_result = await db.execute(
        select(func.count(WorkerRegistry.id)).where(
            WorkerRegistry.status == "busy"
        )
    )
    active_workers = active_workers_result.scalar() or 0

    # Count currently running jobs
    running_result = await db.execute(
        select(func.count(Job.id)).where(Job.status == "running")
    )
    running_jobs = running_result.scalar() or 0

    # Count jobs completed in the last hour
    completed_result = await db.execute(
        select(func.count(Job.id)).where(
            Job.status == "completed",
            Job.completed_at >= one_hour_ago,
        )
    )
    completed_last_hour = completed_result.scalar() or 0

    # Count jobs that failed in the last hour
    failed_result = await db.execute(
        select(func.count(Job.id)).where(
            Job.status == "failed",
            Job.updated_at >= one_hour_ago,
        )
    )
    failed_last_hour = failed_result.scalar() or 0

    # Count total dead-lettered jobs
    dead_letter_result = await db.execute(
        select(func.count(Job.id)).where(Job.status == "dead_letter")
    )
    dead_letter_count = dead_letter_result.scalar() or 0

    return MetricsResponse(
        queue_depth=queue_depth,
        total_queue_depth=total_queue_depth,
        active_workers=active_workers,
        running_jobs=running_jobs,
        completed_last_hour=completed_last_hour,
        failed_last_hour=failed_last_hour,
        dead_letter_count=dead_letter_count,
    )
