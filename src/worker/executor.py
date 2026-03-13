"""
Job execution logic — the core of what a worker does with a claimed job.

Handles the full execution loop: load checkpoint (if resuming),
run simulation steps, check for control signals (pause/cancel),
refresh heartbeats, update progress, write periodic checkpoints,
and handle completion or failure.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as redis

from src.config import settings
from src.core.state_machine import transition_job
from src.db.repositories import attempt_repo, job_repo
from src.db.session import async_session_factory
from src.models.enums import JobStatus
from src.models.job import Job
from src.core.circuit_breaker import CircuitBreaker
from src.dag.executor import on_node_completed, on_node_failed
from src.worker.checkpoint import write_checkpoint, load_checkpoint
from src.worker.heartbeat import refresh_heartbeat, refresh_job_lease
from src.worker.signal_handler import SignalHandler
from src.worker.simulation_runner import SimulationRunner

import structlog

logger = structlog.get_logger(__name__)

# Error types that are retryable (transient infrastructure failures)
TRANSIENT_ERRORS = (
    TimeoutError,
    ConnectionError,
    OSError,
    MemoryError,
)

# Error types that indicate bad input (permanent, no retry)
PERMANENT_ERRORS = (
    ValueError,
    KeyError,
    TypeError,
)


def classify_error(error: Exception) -> str:
    """
    Classify an exception as transient or permanent.

    Transient errors are retried with exponential backoff.
    Permanent errors go straight to dead_letter.
    Unknown errors default to transient (safer to retry).

    Args:
        error: The exception that caused the job failure.

    Returns:
        'transient' or 'permanent' error type string.
    """
    if isinstance(error, PERMANENT_ERRORS):
        return "permanent"
    if isinstance(error, TRANSIENT_ERRORS):
        return "transient"
    # Default: safer to retry unknown errors
    return "transient"


async def execute_job(
    job: Job,
    worker_id: str,
    attempt_id: uuid.UUID,
    session: AsyncSession,
    redis_client: redis.Redis,
    *,
    stream_name: str | None = None,
    message_id: str | None = None,
) -> None:
    """
    Execute a claimed job through its full simulation lifecycle.

    This is the main execution function called by the worker after
    successfully claiming a job. It runs all simulation steps, checks
    for pause/cancel signals between steps, handles heartbeats and
    progress updates, and transitions the job to completed or failed.

    Args:
        job: The Job ORM object that was claimed.
        worker_id: This worker's unique ID.
        attempt_id: UUID of the current attempt row.
        session: Active async database session.
        redis_client: Active async Redis client.
        stream_name: Redis stream the job was read from (for XACK).
        message_id: Redis message ID (for XACK on completion).
    """
    job_id = job.id
    log = logger.bind(job_id=str(job_id), worker_id=worker_id)

    # Start listening for control signals (pause/cancel) from the API
    signal_handler = SignalHandler(redis_client, job_id)
    await signal_handler.start()

    try:
        # Initialize the simulation runner
        runner = SimulationRunner(job.type, job.params)
        total_steps = runner.total_steps()
        state: dict = {}
        starting_step = 0

        # If resuming from a checkpoint, load the saved state
        checkpoint = await load_checkpoint(session, job_id)
        if checkpoint:
            state = runner.deserialize_state(checkpoint.data)
            starting_step = checkpoint.step_number + 1
            log.info("resuming_from_checkpoint", step=starting_step)

        # ── Main execution loop ──────────────────────────────────
        for step in range(starting_step, total_steps):

            # ── Check control signals before each step ───────────
            if signal_handler.should_cancel:
                log.info("cancel_signal_processing", step=step)
                await _handle_cancel(
                    session, redis_client, job_id, worker_id, attempt_id,
                    stream_name, message_id,
                )
                return

            if signal_handler.should_pause:
                log.info("pause_signal_processing", step=step)
                await _handle_pause(
                    session, job_id, worker_id, attempt_id,
                    step, runner.serialize_state(state),
                )
                return

            # Execute one simulation step
            state = await runner.step(step, state)

            # Refresh heartbeat in Redis and extend lease in Postgres
            await refresh_heartbeat(redis_client, worker_id)
            await refresh_job_lease(session, job_id)

            # Update progress percentage
            progress = ((step + 1) / total_steps) * 100.0
            await job_repo.update_job_progress(
                session, job_id, progress, f"Step {step + 1}/{total_steps}"
            )

            # Periodic checkpoint (every N steps as configured)
            if (step + 1) % settings.checkpoint_interval_steps == 0 and step < total_steps - 1:
                await write_checkpoint(
                    session, job_id, attempt_id, step, runner.serialize_state(state)
                )

            # Flush progress and heartbeat updates
            await session.commit()

        # ── Completion ───────────────────────────────────────────
        final_result = runner.build_result(state)

        updated = await transition_job(
            session,
            job_id,
            JobStatus.COMPLETED,
            expected_status=JobStatus.RUNNING,
            worker_id=worker_id,
            result=final_result,
            triggered_by="worker",
        )

        if updated:
            await attempt_repo.finish_attempt(session, attempt_id, status="completed")

            # Notify circuit breaker of success (closes breaker if half-open)
            cb = CircuitBreaker(redis_client)
            await cb.record_success(job.type)

            # Acknowledge the Redis stream message
            if stream_name and message_id:
                from src.queue.redis_stream import xack_job
                await xack_job(redis_client, stream_name, message_id)

            await session.commit()
            log.info("job_completed", total_steps=total_steps)

            # Notify DAG executor if this job is part of a DAG
            if job.dag_id:
                try:
                    async with async_session_factory() as dag_session:
                        await on_node_completed(dag_session, redis_client, updated)
                        await dag_session.commit()
                except Exception as dag_err:
                    log.warning("dag_completion_notify_error", error=str(dag_err))
        else:
            log.warning("completion_transition_rejected")

    except Exception as e:
        # ── Failure handling ─────────────────────────────────────
        error_type = classify_error(e)
        error_msg = f"{type(e).__name__}: {str(e)}"
        log.error("job_failed", error=error_msg, error_type=error_type)

        try:
            failed_job = await transition_job(
                session,
                job_id,
                JobStatus.FAILED,
                expected_status=[JobStatus.RUNNING, JobStatus.PAUSING, JobStatus.CANCELLING],
                error_message=error_msg,
                error_type=error_type,
                triggered_by="worker",
            )

            await attempt_repo.finish_attempt(
                session, attempt_id, status="failed", error_message=error_msg
            )

            # Notify circuit breaker of failure
            cb = CircuitBreaker(redis_client)
            await cb.record_failure(job.type)

            if failed_job:
                await _evaluate_retry(session, failed_job, error_type, redis_client)

            await session.commit()

        except Exception as inner:
            log.error("failure_handling_error", error=str(inner))
            await session.rollback()

    finally:
        # Always clean up the signal handler
        await signal_handler.stop()


async def _handle_cancel(
    session: AsyncSession,
    redis_client: redis.Redis,
    job_id: uuid.UUID,
    worker_id: str,
    attempt_id: uuid.UUID,
    stream_name: str | None,
    message_id: str | None,
) -> None:
    """
    Process a cancel signal — transition to cancelled and clean up.

    Called when the signal handler detects a cancel signal between steps.
    The job may be in 'running' or 'cancelling' state (API already set
    it to 'cancelling' before publishing the signal).

    Args:
        session: Active async database session.
        redis_client: Active async Redis client.
        job_id: UUID of the job being cancelled.
        worker_id: This worker's unique ID.
        attempt_id: UUID of the current attempt.
        stream_name: Redis stream for XACK.
        message_id: Redis message ID for XACK.
    """
    updated = await transition_job(
        session,
        job_id,
        JobStatus.CANCELLED,
        expected_status=[JobStatus.RUNNING, JobStatus.CANCELLING],
        triggered_by="worker",
        metadata={"reason": "cancel_signal_received"},
    )

    if updated:
        await attempt_repo.finish_attempt(session, attempt_id, status="cancelled")

        if stream_name and message_id:
            from src.queue.redis_stream import xack_job
            await xack_job(redis_client, stream_name, message_id)

        await session.commit()
        logger.info("job_cancelled_by_signal", job_id=str(job_id))


async def _handle_pause(
    session: AsyncSession,
    job_id: uuid.UUID,
    worker_id: str,
    attempt_id: uuid.UUID,
    current_step: int,
    serialized_state: dict,
) -> None:
    """
    Process a pause signal — checkpoint state and transition to paused.

    Called when the signal handler detects a pause signal between steps.
    Writes a checkpoint with the current simulation state so the job
    can be resumed later from exactly this point.

    Args:
        session: Active async database session.
        job_id: UUID of the job being paused.
        worker_id: This worker's unique ID.
        attempt_id: UUID of the current attempt.
        current_step: The last completed step number.
        serialized_state: Serialized simulation state for the checkpoint.
    """
    # Write checkpoint with current state (atomic — invalidates previous)
    checkpoint_id = await write_checkpoint(
        session, job_id, attempt_id, current_step, serialized_state,
    )

    # Transition pausing -> paused (clears worker_id and lease)
    updated = await transition_job(
        session,
        job_id,
        JobStatus.PAUSED,
        expected_status=[JobStatus.RUNNING, JobStatus.PAUSING],
        worker_id=worker_id,
        triggered_by="worker",
        metadata={"checkpoint_id": str(checkpoint_id), "paused_at_step": current_step},
    )

    if updated:
        await attempt_repo.finish_attempt(session, attempt_id, status="paused")
        await session.commit()
        logger.info(
            "job_paused_by_signal",
            job_id=str(job_id),
            step=current_step,
            checkpoint_id=str(checkpoint_id),
        )


async def _evaluate_retry(
    session: AsyncSession,
    job: Job,
    error_type: str,
    redis_client: redis.Redis,
) -> None:
    """
    Decide whether a failed job should be retried or sent to dead_letter.

    Transient errors with remaining retries get scheduled for retry
    with exponential backoff. Permanent errors or exhausted retries
    go to dead_letter.

    Args:
        session: Active async database session.
        job: The failed Job with current retry_count and config.
        error_type: 'transient' or 'permanent'.
        redis_client: Active async Redis client (for DAG notifications).
    """
    import random
    from datetime import timedelta
    from sqlalchemy import update as sa_update
    from src.models.job import Job as JobModel

    if error_type == "transient" and job.retry_count < job.max_retries:
        # Calculate exponential backoff with jitter
        delay = min(
            job.backoff_seconds * (job.backoff_multiplier ** job.retry_count),
            job.max_backoff_seconds,
        )
        jitter = random.uniform(0, delay * 0.25)
        next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay + jitter)

        # Increment retry count and schedule
        stmt = (
            sa_update(JobModel)
            .where(JobModel.id == job.id)
            .values(retry_count=job.retry_count + 1, next_retry_at=next_retry_at)
        )
        await session.execute(stmt)

        await transition_job(
            session,
            job.id,
            JobStatus.RETRY_SCHEDULED,
            expected_status=JobStatus.FAILED,
            triggered_by="worker",
            metadata={"next_retry_at": next_retry_at.isoformat(), "retry_count": job.retry_count + 1},
        )

        logger.info(
            "retry_scheduled",
            job_id=str(job.id),
            retry_count=job.retry_count + 1,
            next_retry_at=next_retry_at.isoformat(),
        )
    else:
        # Permanent error or retries exhausted — send to dead_letter
        await transition_job(
            session,
            job.id,
            JobStatus.DEAD_LETTER,
            expected_status=JobStatus.FAILED,
            triggered_by="worker",
            metadata={"reason": "permanent_error" if error_type == "permanent" else "max_retries_exceeded"},
        )

        logger.warning(
            "job_dead_lettered",
            job_id=str(job.id),
            error_type=error_type,
            retry_count=job.retry_count,
        )

        # Notify DAG executor if this job is part of a DAG
        if job.dag_id:
            await on_node_failed(session, redis_client, job)
