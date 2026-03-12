"""
Reaper process — detects dead workers and reclaims their jobs.

Runs as an independent process on a configurable timer. Scans for
jobs with expired leases, transitions them to failed, evaluates
retry policy, and reconciles queue state between Postgres and Redis.

Key properties:
    - Stateless: no in-memory state, safe to restart at any time.
    - Idempotent: running twice in succession causes no harm.
    - Conservative: generous lease timeout minimizes false positives.
"""

import asyncio
import random
from datetime import datetime, timezone, timedelta

import redis.asyncio as redis
from sqlalchemy import select, text, update as sa_update, delete
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.core.state_machine import transition_job
from src.db.repositories import attempt_repo
from src.db.session import async_session_factory
from src.models.enums import JobStatus
from src.models.job import Job, JobAttempt
from src.models.worker import WorkerRegistry
from src.queue.publisher import enqueue_job
from src.queue.redis_stream import STREAM_NAMES

import structlog

logger = structlog.get_logger(__name__)


class Reaper:
    """
    Periodic background process that recovers from worker failures.

    Detects stale job leases (worker died mid-execution), reclaims
    those jobs for retry, reconciles Redis queue state with Postgres,
    and cleans up dead worker registry entries.
    """

    def __init__(self) -> None:
        """Initialize the reaper with a Redis connection."""
        self._redis: redis.Redis | None = None
        self._running = False

    async def start(self) -> None:
        """
        Run the reaper loop forever, executing one cycle at each interval.

        Connects to Redis, then enters an infinite loop calling run_cycle()
        followed by sleeping for reaper_interval_seconds.
        """
        self._redis = redis.from_url(settings.redis_url, decode_responses=True)
        self._running = True
        logger.info("reaper_started", interval=settings.reaper_interval_seconds)

        try:
            while self._running:
                try:
                    await self.run_cycle()
                except Exception as e:
                    logger.error("reaper_cycle_error", error=str(e))
                await asyncio.sleep(settings.reaper_interval_seconds)
        finally:
            if self._redis:
                await self._redis.close()

    async def stop(self) -> None:
        """Signal the reaper to stop after the current cycle completes."""
        self._running = False

    async def run_cycle(self) -> None:
        """
        Execute one complete reaper scan cycle.

        Step 1: Reclaim stale running jobs (lease expired).
        Step 2: Reclaim stale pausing jobs (worker died during pause).
        Step 3: Reclaim stale cancelling jobs (worker died during cancel).
        Step 4: Reconcile queue state (re-enqueue orphaned queued jobs).
        Step 5: Clean up dead worker registry entries.
        """
        logger.debug("reaper_cycle_start")

        async with async_session_factory() as session:
            # Step 1 — Reclaim stale running jobs
            await self._reclaim_stale_running(session)

            # Step 2 — Reclaim stale pausing jobs
            await self._reclaim_stale_pausing(session)

            # Step 3 — Reclaim stale cancelling jobs
            await self._reclaim_stale_cancelling(session)

            # Step 4 — Reconcile queue state
            await self._reconcile_queue(session)

            # Step 5 — Clean up dead workers
            await self._cleanup_dead_workers(session)

            await session.commit()

        logger.debug("reaper_cycle_complete")

    async def _reclaim_stale_running(self, session: AsyncSession) -> None:
        """
        Find running jobs with expired leases and transition them to failed.

        Uses FOR UPDATE SKIP LOCKED to prevent deadlocks if multiple
        reaper instances run concurrently.
        """
        now = datetime.now(timezone.utc)
        stmt = (
            select(Job)
            .where(Job.status == "running", Job.lease_expires_at < now)
            .with_for_update(skip_locked=True)
        )
        result = await session.execute(stmt)
        stale_jobs = list(result.scalars().all())

        for job in stale_jobs:
            logger.warning(
                "reclaiming_stale_job",
                job_id=str(job.id),
                worker_id=job.worker_id,
                lease_expired_at=str(job.lease_expires_at),
            )

            # Transition running -> failed
            failed_job = await transition_job(
                session,
                job.id,
                JobStatus.FAILED,
                expected_status=JobStatus.RUNNING,
                error_message="Worker lease expired — reclaimed by reaper",
                error_type="transient",
                triggered_by="reaper",
                metadata={"reason": "lease_expired"},
            )

            if failed_job:
                # Evaluate retry policy
                await self._evaluate_retry(session, failed_job)

                # Remove stale heartbeat key from Redis
                if job.worker_id:
                    heartbeat_key = f"worker:{job.worker_id}:heartbeat"
                    await self._redis.delete(heartbeat_key)

    async def _reclaim_stale_pausing(self, session: AsyncSession) -> None:
        """Reclaim jobs stuck in 'pausing' due to worker death."""
        now = datetime.now(timezone.utc)
        stmt = (
            select(Job)
            .where(Job.status == "pausing", Job.lease_expires_at < now)
            .with_for_update(skip_locked=True)
        )
        result = await session.execute(stmt)
        stale_jobs = list(result.scalars().all())

        for job in stale_jobs:
            logger.warning("reclaiming_stale_pausing", job_id=str(job.id))

            failed_job = await transition_job(
                session,
                job.id,
                JobStatus.FAILED,
                expected_status=JobStatus.PAUSING,
                error_message="Worker died during pause — reclaimed by reaper",
                error_type="transient",
                triggered_by="reaper",
            )

            if failed_job:
                await self._evaluate_retry(session, failed_job)

    async def _reclaim_stale_cancelling(self, session: AsyncSession) -> None:
        """Force-cancel jobs stuck in 'cancelling' due to worker death."""
        now = datetime.now(timezone.utc)
        stmt = (
            select(Job)
            .where(Job.status == "cancelling", Job.lease_expires_at < now)
            .with_for_update(skip_locked=True)
        )
        result = await session.execute(stmt)
        stale_jobs = list(result.scalars().all())

        for job in stale_jobs:
            logger.warning("force_cancelling_stale_job", job_id=str(job.id))

            # Intent was to cancel, so just finish the cancellation
            await transition_job(
                session,
                job.id,
                JobStatus.CANCELLED,
                expected_status=JobStatus.CANCELLING,
                triggered_by="reaper",
                metadata={"reason": "worker_died_during_cancel"},
            )

    async def _reconcile_queue(self, session: AsyncSession) -> None:
        """
        Re-enqueue jobs stuck in 'queued' state that are missing from Redis.

        This handles the case where Redis crashed after the job was
        transitioned to 'queued' in Postgres but before the XADD completed.
        Postgres is truth — if it says 'queued', the job must be in Redis.
        """
        threshold = datetime.now(timezone.utc) - timedelta(
            seconds=settings.reaper_reconcile_threshold_seconds
        )

        stmt = select(Job).where(Job.status == "queued", Job.queued_at < threshold)
        result = await session.execute(stmt)
        orphaned_jobs = list(result.scalars().all())

        for job in orphaned_jobs:
            logger.info("reconciling_orphaned_job", job_id=str(job.id))
            await enqueue_job(self._redis, job.id, job.priority)

    async def _cleanup_dead_workers(self, session: AsyncSession) -> None:
        """
        Remove worker_registry entries that haven't sent a heartbeat
        in over 5 minutes and aren't in 'stopped' status.
        """
        threshold = datetime.now(timezone.utc) - timedelta(minutes=5)
        stmt = (
            delete(WorkerRegistry)
            .where(
                WorkerRegistry.last_heartbeat < threshold,
                WorkerRegistry.status != "stopped",
            )
        )
        result = await session.execute(stmt)
        if result.rowcount > 0:
            logger.info("dead_workers_cleaned", count=result.rowcount)

    async def _evaluate_retry(self, session: AsyncSession, job: Job) -> None:
        """
        Decide whether a failed job should be retried or dead-lettered.

        Args:
            session: Active async database session.
            job: The failed Job to evaluate.
        """
        error_type = job.error_type or "transient"

        if error_type == "transient" and job.retry_count < job.max_retries:
            # Calculate backoff with jitter
            delay = min(
                job.backoff_seconds * (job.backoff_multiplier ** job.retry_count),
                job.max_backoff_seconds,
            )
            jitter = random.uniform(0, delay * 0.25)
            next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay + jitter)

            # Increment retry count
            await session.execute(
                sa_update(Job)
                .where(Job.id == job.id)
                .values(retry_count=job.retry_count + 1, next_retry_at=next_retry_at)
            )

            await transition_job(
                session,
                job.id,
                JobStatus.RETRY_SCHEDULED,
                expected_status=JobStatus.FAILED,
                triggered_by="reaper",
                metadata={"next_retry_at": next_retry_at.isoformat()},
            )

            logger.info("reaper_retry_scheduled", job_id=str(job.id))
        else:
            await transition_job(
                session,
                job.id,
                JobStatus.DEAD_LETTER,
                expected_status=JobStatus.FAILED,
                triggered_by="reaper",
            )
            logger.warning("reaper_dead_lettered", job_id=str(job.id))
