"""
Worker class — the main process that claims and executes simulation jobs.

Each worker runs a claim loop that polls Redis streams in priority order,
atomically claims jobs via a Postgres WHERE guard, and spawns execution
tasks. The worker maintains its own heartbeat and registers itself in
the worker_registry table for monitoring and reaper detection.
"""

import asyncio
import os
import signal
import socket
import uuid
from datetime import datetime, timezone, timedelta

import redis.asyncio as redis
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.core.state_machine import transition_job
from src.db.repositories import attempt_repo
from src.db.session import async_session_factory
from src.models.enums import JobStatus
from src.models.job import Job
from src.models.worker import WorkerRegistry
from src.queue.redis_stream import (
    ensure_consumer_groups,
    xack_job,
    xreadgroup_job,
)
from src.worker.executor import execute_job
from src.worker.heartbeat import refresh_heartbeat, remove_heartbeat

import structlog

logger = structlog.get_logger(__name__)


class Worker:
    """
    Distributed worker process for executing simulation jobs.

    Lifecycle:
        1. start() — register, create consumer groups, enter claim loop
        2. claim_loop() — poll Redis, atomically claim jobs, spawn execution
        3. shutdown() — graceful stop, checkpoint running jobs, deregister
    """

    def __init__(self, config: settings.__class__ = None, worker_id: str | None = None) -> None:
        """
        Initialize the worker.

        Args:
            config: Settings instance (uses global settings if None).
            worker_id: Optional explicit worker ID. If None, generates one
                       from hostname:pid:random_hex.
        """
        self._config = config or settings
        self._worker_id = worker_id or self._generate_worker_id()
        self._shutting_down = False
        self._running_tasks: dict[uuid.UUID, asyncio.Task] = {}
        self._redis: redis.Redis | None = None
        self._log = logger.bind(worker_id=self._worker_id)

    @property
    def worker_id(self) -> str:
        """Return this worker's unique identifier."""
        return self._worker_id

    def _generate_worker_id(self) -> str:
        """Generate a unique worker ID: hostname:pid:random_hex."""
        hostname = socket.gethostname()
        pid = os.getpid()
        rand = uuid.uuid4().hex[:8]
        return f"{hostname}:{pid}:{rand}"

    async def start(self) -> None:
        """
        Start the worker process.

        1. Connect to Redis
        2. Register in worker_registry table
        3. Create consumer groups for all priority streams
        4. Start the heartbeat background loop
        5. Enter the claim loop (runs until shutdown)
        """
        self._log.info("worker_starting")

        # Connect to Redis
        self._redis = redis.from_url(self._config.redis_url, decode_responses=True)

        # Register in worker_registry
        async with async_session_factory() as session:
            worker_row = WorkerRegistry(
                id=self._worker_id,
                hostname=socket.gethostname(),
                pid=os.getpid(),
                status="idle",
            )
            session.add(worker_row)
            await session.commit()

        # Ensure consumer groups exist for all priority streams
        await ensure_consumer_groups(self._redis)

        # Start heartbeat background task
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        self._log.info("worker_started")

        try:
            await self.claim_loop()
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

    async def claim_loop(self) -> None:
        """
        Continuously poll Redis streams for jobs to claim.

        Polls in strict priority order (critical -> high -> normal -> low).
        For each message, attempts an atomic claim in Postgres. If the claim
        succeeds (1 row updated), spawns an execution task. If it fails
        (0 rows — another worker got there first), ACKs the message and moves on.
        """
        self._log.info("claim_loop_started")

        while not self._shutting_down:
            # Check if we have capacity for another job
            if len(self._running_tasks) >= self._config.worker_max_concurrent_jobs:
                await asyncio.sleep(1)
                self._cleanup_finished_tasks()
                continue

            # Try to read a job from Redis streams
            result = await xreadgroup_job(
                self._redis,
                self._worker_id,
                block_ms=self._config.worker_claim_poll_interval_ms,
            )

            if result is None:
                # No messages available — brief sleep to avoid busy loop
                await asyncio.sleep(0.1)
                continue

            stream_name, message_id, job_id = result

            # Attempt atomic claim in Postgres
            async with async_session_factory() as session:
                claimed_job = await self._try_claim(session, job_id)

                if claimed_job is None:
                    # Another worker already claimed it — ACK and move on
                    await xack_job(self._redis, stream_name, message_id)
                    self._log.debug("claim_lost", job_id=str(job_id))
                    continue

                # Create an attempt row for tracking
                attempt = await attempt_repo.create_attempt(
                    session,
                    job_id=job_id,
                    worker_id=self._worker_id,
                    attempt_number=claimed_job.retry_count + 1,
                )

                # Update worker_registry to show we're busy
                await session.execute(
                    update(WorkerRegistry)
                    .where(WorkerRegistry.id == self._worker_id)
                    .values(status="busy", current_job_id=job_id)
                )

                await session.commit()

                self._log.info("job_claimed", job_id=str(job_id), job_type=claimed_job.type)

                # Spawn execution as a background task
                task = asyncio.create_task(
                    self._run_job(claimed_job, attempt.id, stream_name, message_id)
                )
                self._running_tasks[job_id] = task

    async def _try_claim(self, session: AsyncSession, job_id: uuid.UUID) -> Job | None:
        """
        Atomically claim a job by transitioning queued -> running.

        The WHERE guard ensures only one worker can claim a given job.
        Sets worker_id and lease_expires_at on the job row.

        Args:
            session: Active async database session.
            job_id: UUID of the job to claim.

        Returns:
            The claimed Job if successful, or None if already claimed.
        """
        now = datetime.now(timezone.utc)
        lease_expiry = now + timedelta(seconds=self._config.worker_lease_timeout_seconds)

        # Use transition_job for the status change + event logging
        job = await transition_job(
            session,
            job_id,
            JobStatus.RUNNING,
            expected_status=[JobStatus.QUEUED, JobStatus.RESUMING],
            worker_id=self._worker_id,
            triggered_by="worker",
        )

        if job:
            # Set the lease expiration (not handled by transition_job)
            from sqlalchemy import update as sa_update
            await session.execute(
                sa_update(Job)
                .where(Job.id == job_id)
                .values(lease_expires_at=lease_expiry)
            )

        return job

    async def _run_job(
        self,
        job: Job,
        attempt_id: uuid.UUID,
        stream_name: str,
        message_id: str,
    ) -> None:
        """
        Execute a job and clean up worker state when done.

        This is the wrapper that runs as an asyncio task. It calls
        execute_job() and then updates the worker_registry to idle.

        Args:
            job: The claimed Job to execute.
            attempt_id: UUID of the current attempt.
            stream_name: Redis stream for XACK.
            message_id: Redis message ID for XACK.
        """
        try:
            async with async_session_factory() as session:
                await execute_job(
                    job,
                    self._worker_id,
                    attempt_id,
                    session,
                    self._redis,
                    stream_name=stream_name,
                    message_id=message_id,
                )
        except Exception as e:
            self._log.error("job_execution_error", job_id=str(job.id), error=str(e))
        finally:
            # Return worker to idle state
            try:
                async with async_session_factory() as session:
                    await session.execute(
                        update(WorkerRegistry)
                        .where(WorkerRegistry.id == self._worker_id)
                        .values(status="idle", current_job_id=None)
                    )
                    await session.commit()
            except Exception:
                pass

            self._running_tasks.pop(job.id, None)

    async def _heartbeat_loop(self) -> None:
        """
        Periodically refresh the worker's heartbeat in Redis and Postgres.

        Runs as a background task for the lifetime of the worker.
        Updates both the Redis heartbeat key and the worker_registry row.
        """
        while not self._shutting_down:
            try:
                await refresh_heartbeat(self._redis, self._worker_id)

                # Update worker_registry.last_heartbeat in Postgres
                async with async_session_factory() as session:
                    await session.execute(
                        update(WorkerRegistry)
                        .where(WorkerRegistry.id == self._worker_id)
                        .values(last_heartbeat=datetime.now(timezone.utc))
                    )
                    await session.commit()

            except Exception as e:
                self._log.warning("heartbeat_error", error=str(e))

            await asyncio.sleep(self._config.worker_heartbeat_interval_seconds)

    def _cleanup_finished_tasks(self) -> None:
        """Remove completed asyncio tasks from the running set."""
        finished = [jid for jid, task in self._running_tasks.items() if task.done()]
        for jid in finished:
            self._running_tasks.pop(jid, None)

    async def shutdown(self) -> None:
        """
        Graceful shutdown — stop claiming, wait for running jobs, deregister.

        1. Signal the claim loop to stop
        2. Wait for running tasks to finish (with timeout)
        3. Remove heartbeat from Redis
        4. Update worker_registry status to 'stopped'
        5. Close Redis connection
        """
        self._log.info("worker_shutting_down")
        self._shutting_down = True

        # Wait for running tasks with a timeout
        if self._running_tasks:
            self._log.info(
                "waiting_for_running_jobs",
                count=len(self._running_tasks),
            )
            tasks = list(self._running_tasks.values())
            await asyncio.wait(tasks, timeout=30)

        # Clean up Redis heartbeat
        if self._redis:
            await remove_heartbeat(self._redis, self._worker_id)

        # Mark worker as stopped in Postgres
        try:
            async with async_session_factory() as session:
                await session.execute(
                    update(WorkerRegistry)
                    .where(WorkerRegistry.id == self._worker_id)
                    .values(status="stopped", current_job_id=None)
                )
                await session.commit()
        except Exception:
            pass

        # Close Redis connection
        if self._redis:
            await self._redis.close()

        self._log.info("worker_stopped")
