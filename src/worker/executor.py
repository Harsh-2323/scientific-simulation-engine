"""
Job execution logic — the core of what a worker does with a claimed job.

Handles the full execution loop: load checkpoint (if resuming),
run simulation steps, refresh heartbeats, update progress, write
periodic checkpoints, and handle completion or failure.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as redis

from src.config import settings
from src.core.state_machine import transition_job
from src.db.repositories import attempt_repo, job_repo
from src.models.enums import JobStatus, ErrorType
from src.models.job import Job
from src.worker.heartbeat import refresh_heartbeat, refresh_job_lease
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
    successfully claiming a job. It runs all simulation steps, handles
    heartbeats and progress updates, and transitions the job to
    completed or failed when done.

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

    try:
        # Initialize the simulation runner
        runner = SimulationRunner(job.type, job.params)
        total_steps = runner.total_steps()
        state: dict = {}
        starting_step = 0

        # If resuming from a checkpoint, load the saved state
        from src.models.checkpoint import Checkpoint
        from sqlalchemy import select

        checkpoint_stmt = (
            select(Checkpoint)
            .where(Checkpoint.job_id == job_id, Checkpoint.is_valid == True)  # noqa: E712
            .order_by(Checkpoint.step_number.desc())
            .limit(1)
        )
        result = await session.execute(checkpoint_stmt)
        checkpoint = result.scalars().first()

        if checkpoint:
            state = runner.deserialize_state(checkpoint.data)
            starting_step = checkpoint.step_number + 1
            log.info("resuming_from_checkpoint", step=starting_step)

        # ── Main execution loop ──────────────────────────────────
        for step in range(starting_step, total_steps):
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
                await _write_checkpoint(
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
            # Mark the attempt as completed
            await attempt_repo.finish_attempt(session, attempt_id, status="completed")

            # Acknowledge the Redis stream message
            if stream_name and message_id:
                from src.queue.redis_stream import xack_job
                await xack_job(redis_client, stream_name, message_id)

            await session.commit()
            log.info("job_completed", total_steps=total_steps)
        else:
            log.warning("completion_transition_rejected")

    except Exception as e:
        # ── Failure handling ─────────────────────────────────────
        error_type = classify_error(e)
        error_msg = f"{type(e).__name__}: {str(e)}"
        log.error("job_failed", error=error_msg, error_type=error_type)

        try:
            # Transition to failed state
            failed_job = await transition_job(
                session,
                job_id,
                JobStatus.FAILED,
                expected_status=[JobStatus.RUNNING, JobStatus.PAUSING, JobStatus.CANCELLING],
                error_message=error_msg,
                error_type=error_type,
                triggered_by="worker",
            )

            # Mark the attempt as failed
            await attempt_repo.finish_attempt(
                session, attempt_id, status="failed", error_message=error_msg
            )

            # Evaluate retry policy
            if failed_job:
                await _evaluate_retry(session, failed_job, error_type)

            await session.commit()

        except Exception as inner:
            log.error("failure_handling_error", error=str(inner))
            await session.rollback()


async def _evaluate_retry(
    session: AsyncSession,
    job: Job,
    error_type: str,
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


async def _write_checkpoint(
    session: AsyncSession,
    job_id: uuid.UUID,
    attempt_id: uuid.UUID,
    step_number: int,
    data: dict,
) -> uuid.UUID:
    """
    Write an atomic checkpoint — invalidate all previous, insert new.

    Within a single flush, marks all prior checkpoints for this job as
    invalid and inserts the new one as valid. If anything fails, the
    previous checkpoint remains valid (transaction rollback).

    Args:
        session: Active async database session.
        job_id: The job being checkpointed.
        attempt_id: The current attempt.
        step_number: The last completed step number.
        data: Serialized simulation state.

    Returns:
        UUID of the newly created checkpoint.
    """
    from sqlalchemy import update as sa_update
    from src.models.checkpoint import Checkpoint

    # Invalidate all previous checkpoints for this job
    invalidate_stmt = (
        sa_update(Checkpoint)
        .where(Checkpoint.job_id == job_id, Checkpoint.is_valid == True)  # noqa: E712
        .values(is_valid=False)
    )
    await session.execute(invalidate_stmt)

    # Insert the new valid checkpoint
    new_checkpoint = Checkpoint(
        job_id=job_id,
        attempt_id=attempt_id,
        step_number=step_number,
        data=data,
        is_valid=True,
    )
    session.add(new_checkpoint)
    await session.flush()

    logger.info(
        "checkpoint_written",
        job_id=str(job_id),
        step=step_number,
        checkpoint_id=str(new_checkpoint.id),
    )
    return new_checkpoint.id
