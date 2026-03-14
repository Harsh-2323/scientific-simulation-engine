"""
Repository for job attempt tracking.

Each time a worker claims a job, a new attempt row is created.
This tracks per-attempt outcomes — which worker ran it, whether
it succeeded or failed, and links to any checkpoint created.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.job import JobAttempt

import structlog

logger = structlog.get_logger(__name__)


async def create_attempt(
    session: AsyncSession,
    *,
    job_id: uuid.UUID,
    worker_id: str,
    attempt_number: int,
) -> JobAttempt:
    """
    Record a new execution attempt for a job.

    Args:
        session: Active async database session.
        job_id: The job being attempted.
        worker_id: The worker claiming this attempt.
        attempt_number: Sequential attempt number (1-based, = retry_count + 1).

    Returns:
        The newly created JobAttempt row.
    """
    attempt = JobAttempt(
        job_id=job_id,
        worker_id=worker_id,
        attempt_number=attempt_number,
        status="running",
    )
    session.add(attempt)
    await session.flush()

    logger.info(
        "attempt_created",
        job_id=str(job_id),
        worker_id=worker_id,
        attempt_number=attempt_number,
    )
    return attempt


async def finish_attempt(
    session: AsyncSession,
    attempt_id: uuid.UUID,
    *,
    status: str,
    error_message: str | None = None,
) -> None:
    """
    Mark an attempt as finished (completed or failed).

    Args:
        session: Active async database session.
        attempt_id: The attempt to update.
        status: Final status ('completed' or 'failed').
        error_message: Error details if the attempt failed.
    """
    stmt = (
        update(JobAttempt)
        .where(JobAttempt.id == attempt_id)
        .values(
            status=status,
            finished_at=datetime.now(timezone.utc),
            error_message=error_message,
        )
    )
    await session.execute(stmt)

    logger.info("attempt_finished", attempt_id=str(attempt_id), status=status)


async def get_attempts_for_job(
    session: AsyncSession,
    job_id: uuid.UUID,
) -> list[JobAttempt]:
    """
    Fetch all attempts for a job, ordered by attempt number.

    Args:
        session: Active async database session.
        job_id: The job's UUID.

    Returns:
        List of attempts ordered by attempt_number ascending.
    """
    stmt = (
        select(JobAttempt)
        .where(JobAttempt.job_id == job_id)
        .order_by(JobAttempt.attempt_number.asc())
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())
