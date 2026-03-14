"""
Repository for job event (audit log) operations.

Every state transition writes an event row. This repo provides
read access for the /jobs/{id}/events API endpoint.
"""

import uuid

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.job import JobEvent


async def list_events_for_job(
    session: AsyncSession,
    job_id: uuid.UUID,
) -> tuple[list[JobEvent], int]:
    """
    Fetch all events for a specific job, ordered by timestamp ascending.

    Args:
        session: Active async database session.
        job_id: The job's UUID.

    Returns:
        Tuple of (list of events, total count).
    """
    # Get total count
    count_stmt = select(func.count(JobEvent.id)).where(JobEvent.job_id == job_id)
    total = (await session.execute(count_stmt)).scalar() or 0

    # Get events ordered chronologically
    stmt = (
        select(JobEvent)
        .where(JobEvent.job_id == job_id)
        .order_by(JobEvent.timestamp.asc())
    )
    result = await session.execute(stmt)
    events = list(result.scalars().all())

    return events, total
