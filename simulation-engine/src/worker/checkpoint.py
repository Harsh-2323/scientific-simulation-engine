"""
Checkpoint write and read with atomic protocol.

Checkpoints capture serialized simulation state at a specific step,
enabling pause/resume without losing intermediate work. The atomic
write protocol ensures that at most one valid checkpoint exists per
job at any time — if the write fails, the previous checkpoint remains.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.checkpoint import Checkpoint

import structlog

logger = structlog.get_logger(__name__)


async def write_checkpoint(
    session: AsyncSession,
    job_id: uuid.UUID,
    attempt_id: uuid.UUID,
    step_number: int,
    data: dict,
) -> uuid.UUID:
    """
    Atomically write a checkpoint — invalidate all previous, insert new.

    The protocol ensures exactly one valid checkpoint per job:
        1. Serialize data to JSON (Pydantic/dict already serializable)
        2. Validate that the data can round-trip (deserialize check)
        3. Within a single transaction:
           a. Mark all previous checkpoints for this job as is_valid=False
           b. INSERT new checkpoint with is_valid=True
        4. Return the new checkpoint's UUID

    If any step fails, the transaction rolls back and the previous
    valid checkpoint is preserved. This guarantees we never have a
    corrupt or partial checkpoint.

    Args:
        session: Active async database session (caller manages transaction).
        job_id: UUID of the job being checkpointed.
        attempt_id: UUID of the current execution attempt.
        step_number: The last completed simulation step number.
        data: Serialized simulation state (must be JSON-serializable).

    Returns:
        UUID of the newly created checkpoint.

    Raises:
        CheckpointError: If serialization validation fails.
    """
    import json
    from src.core.exceptions import CheckpointError

    from src.config import settings

    # Validate the data can round-trip through JSON serialization
    try:
        serialized = json.dumps(data)
        json.loads(serialized)
    except (TypeError, ValueError) as e:
        raise CheckpointError(f"Checkpoint data is not JSON-serializable: {e}")

    # Guard against oversized checkpoints bloating the DB
    max_bytes = int(settings.max_checkpoint_bytes)
    if len(serialized.encode("utf-8")) > max_bytes:
        raise CheckpointError(
            f"Checkpoint data exceeds max size of {max_bytes} bytes "
            f"({len(serialized.encode('utf-8'))} bytes)"
        )

    # Step 1: Invalidate all previous checkpoints for this job
    invalidate_stmt = (
        sa_update(Checkpoint)
        .where(Checkpoint.job_id == job_id, Checkpoint.is_valid == True)  # noqa: E712
        .values(is_valid=False)
    )
    await session.execute(invalidate_stmt)

    # Step 2: Insert the new valid checkpoint
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


async def load_checkpoint(
    session: AsyncSession,
    job_id: uuid.UUID,
) -> Checkpoint | None:
    """
    Load the latest valid checkpoint for a job.

    Used when resuming a paused job — the worker starts execution
    from checkpoint.step_number + 1 with the restored state.

    Args:
        session: Active async database session.
        job_id: UUID of the job to load the checkpoint for.

    Returns:
        The latest valid Checkpoint, or None if no valid checkpoint exists.
    """
    stmt = (
        select(Checkpoint)
        .where(Checkpoint.job_id == job_id, Checkpoint.is_valid == True)  # noqa: E712
        .order_by(Checkpoint.step_number.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    checkpoint = result.scalars().first()

    if checkpoint:
        logger.info(
            "checkpoint_loaded",
            job_id=str(job_id),
            step=checkpoint.step_number,
            checkpoint_id=str(checkpoint.id),
        )

    return checkpoint
