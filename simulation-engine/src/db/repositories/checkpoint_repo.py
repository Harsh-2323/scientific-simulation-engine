"""
Repository for checkpoint CRUD operations.

Provides database access for listing, loading, and managing
checkpoints. The write logic lives in src/worker/checkpoint.py
because it's tightly coupled with the atomic write protocol.
"""

import uuid

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.checkpoint import Checkpoint

import structlog

logger = structlog.get_logger(__name__)


async def get_latest_valid_checkpoint(
    session: AsyncSession,
    job_id: uuid.UUID,
) -> Checkpoint | None:
    """
    Fetch the most recent valid checkpoint for a job.

    Args:
        session: Active async database session.
        job_id: UUID of the job.

    Returns:
        The latest Checkpoint with is_valid=True, or None.
    """
    stmt = (
        select(Checkpoint)
        .where(Checkpoint.job_id == job_id, Checkpoint.is_valid == True)  # noqa: E712
        .order_by(Checkpoint.step_number.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalars().first()


async def list_checkpoints_for_job(
    session: AsyncSession,
    job_id: uuid.UUID,
) -> tuple[list[Checkpoint], int]:
    """
    Fetch all checkpoints for a job, ordered newest first.

    Args:
        session: Active async database session.
        job_id: UUID of the job.

    Returns:
        Tuple of (list of checkpoints, total count).
    """
    count_stmt = select(func.count(Checkpoint.id)).where(Checkpoint.job_id == job_id)
    total = (await session.execute(count_stmt)).scalar() or 0

    stmt = (
        select(Checkpoint)
        .where(Checkpoint.job_id == job_id)
        .order_by(Checkpoint.step_number.desc())
    )
    result = await session.execute(stmt)
    checkpoints = list(result.scalars().all())

    return checkpoints, total


async def has_valid_checkpoint(
    session: AsyncSession,
    job_id: uuid.UUID,
) -> bool:
    """
    Check whether a job has at least one valid checkpoint.

    Used by the resume endpoint to verify that resuming is possible.

    Args:
        session: Active async database session.
        job_id: UUID of the job.

    Returns:
        True if a valid checkpoint exists, False otherwise.
    """
    stmt = (
        select(func.count(Checkpoint.id))
        .where(Checkpoint.job_id == job_id, Checkpoint.is_valid == True)  # noqa: E712
    )
    count = (await session.execute(stmt)).scalar() or 0
    return count > 0
