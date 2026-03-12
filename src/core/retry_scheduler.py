"""
Retry scheduler — re-enqueues jobs whose backoff timer has expired.

Runs as an independent background process, scanning for jobs in
'retry_scheduled' status whose next_retry_at has passed. Transitions
them back to 'queued' and adds them to the appropriate Redis stream.

Like the reaper, this process is stateless and idempotent.
"""

import asyncio
from datetime import datetime, timezone

import redis.asyncio as redis
from sqlalchemy import select

from src.config import settings
from src.core.state_machine import transition_job
from src.db.session import async_session_factory
from src.models.enums import JobStatus
from src.models.job import Job
from src.queue.publisher import enqueue_job

import structlog

logger = structlog.get_logger(__name__)


class RetryScheduler:
    """
    Periodic process that re-enqueues retry-eligible jobs.

    Scans for jobs in 'retry_scheduled' status whose next_retry_at
    timestamp has passed, transitions them to 'queued', and enqueues
    them into the Redis stream for their priority level.
    """

    def __init__(self) -> None:
        """Initialize the retry scheduler."""
        self._redis: redis.Redis | None = None
        self._running = False

    async def start(self) -> None:
        """
        Run the retry scheduler loop forever.

        Connects to Redis, then scans for due retries every
        retry_scheduler_interval_seconds.
        """
        self._redis = redis.from_url(settings.redis_url, decode_responses=True)
        self._running = True
        logger.info("retry_scheduler_started", interval=settings.retry_scheduler_interval_seconds)

        try:
            while self._running:
                try:
                    await self.run_cycle()
                except Exception as e:
                    logger.error("retry_scheduler_error", error=str(e))
                await asyncio.sleep(settings.retry_scheduler_interval_seconds)
        finally:
            if self._redis:
                await self._redis.close()

    async def stop(self) -> None:
        """Signal the scheduler to stop after the current cycle."""
        self._running = False

    async def run_cycle(self) -> None:
        """
        Find all retry_scheduled jobs past their next_retry_at and re-enqueue them.

        For each due job:
            1. Transition: retry_scheduled -> queued
            2. Clear error fields
            3. XADD to the appropriate Redis priority stream
        """
        now = datetime.now(timezone.utc)

        async with async_session_factory() as session:
            # Find jobs whose retry timer has elapsed
            stmt = (
                select(Job)
                .where(
                    Job.status == "retry_scheduled",
                    Job.next_retry_at <= now,
                )
                .with_for_update(skip_locked=True)
            )
            result = await session.execute(stmt)
            due_jobs = list(result.scalars().all())

            if not due_jobs:
                return

            logger.info("retries_due", count=len(due_jobs))

            for job in due_jobs:
                # Transition back to queued
                updated = await transition_job(
                    session,
                    job.id,
                    JobStatus.QUEUED,
                    expected_status=JobStatus.RETRY_SCHEDULED,
                    triggered_by="scheduler",
                    metadata={"retry_count": job.retry_count},
                )

                if updated:
                    # Re-enqueue into Redis
                    await enqueue_job(self._redis, job.id, job.priority)
                    logger.info(
                        "job_re_enqueued",
                        job_id=str(job.id),
                        retry_count=job.retry_count,
                        priority=job.priority,
                    )

            await session.commit()
