"""
Repository for Job CRUD operations.

Provides async database access for creating, reading, listing, and
updating jobs. All status changes go through the state machine —
this repo only handles non-status mutations and queries.
"""

import uuid
from datetime import datetime

from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.models.job import Job, JobAttempt
from src.models.enums import JobStatus, JobPriority

import structlog

logger = structlog.get_logger(__name__)


async def create_job(
    session: AsyncSession,
    *,
    job_type: str,
    params: dict,
    priority: str = "normal",
    max_retries: int = 3,
    idempotency_key: str | None = None,
    metadata: dict | None = None,
    dag_id: uuid.UUID | None = None,
    dag_node_id: str | None = None,
    backoff_seconds: float = 5.0,
    backoff_multiplier: float = 2.0,
    max_backoff_seconds: float = 300.0,
) -> Job:
    """
    Insert a new job into the database with status='pending'.

    Args:
        session: Active async database session.
        job_type: The simulation type (e.g., 'cfd_simulation').
        params: Simulation parameters as a JSON-serializable dict.
        priority: One of 'critical', 'high', 'normal', 'low'.
        max_retries: Maximum retry attempts before dead_letter.
        idempotency_key: Optional dedup key — unique across all jobs.
        metadata: Arbitrary key-value pairs for extensibility.
        dag_id: Parent DAG ID if this job is part of a DAG.
        dag_node_id: Node identifier within the DAG.
        backoff_seconds: Base backoff for retry scheduling.
        backoff_multiplier: Exponential multiplier for backoff.
        max_backoff_seconds: Cap on backoff duration.

    Returns:
        The newly created Job with status='pending'.
    """
    job = Job(
        type=job_type,
        status="pending",
        priority=priority,
        params=params,
        max_retries=max_retries,
        idempotency_key=idempotency_key,
        metadata_=metadata or {},
        dag_id=dag_id,
        dag_node_id=dag_node_id,
        backoff_seconds=backoff_seconds,
        backoff_multiplier=backoff_multiplier,
        max_backoff_seconds=max_backoff_seconds,
    )
    session.add(job)
    await session.flush()

    logger.info("job_created", job_id=str(job.id), job_type=job_type, priority=priority)
    return job


async def get_job_by_id(
    session: AsyncSession,
    job_id: uuid.UUID,
    *,
    load_attempts: bool = False,
) -> Job | None:
    """
    Fetch a single job by its UUID.

    Args:
        session: Active async database session.
        job_id: The job's UUID.
        load_attempts: If True, eagerly load the job's attempts.

    Returns:
        The Job if found, or None.
    """
    stmt = select(Job).where(Job.id == job_id)
    if load_attempts:
        stmt = stmt.options(selectinload(Job.attempts))
    result = await session.execute(stmt)
    return result.scalars().first()


async def get_job_by_idempotency_key(
    session: AsyncSession,
    idempotency_key: str,
) -> Job | None:
    """
    Look up a job by its idempotency key for deduplication.

    Args:
        session: Active async database session.
        idempotency_key: The unique idempotency key to search for.

    Returns:
        The existing Job if found, or None.
    """
    stmt = select(Job).where(Job.idempotency_key == idempotency_key)
    result = await session.execute(stmt)
    return result.scalars().first()


async def list_jobs(
    session: AsyncSession,
    *,
    status: str | None = None,
    priority: str | None = None,
    job_type: str | None = None,
    dag_id: uuid.UUID | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    limit: int = 50,
    offset: int = 0,
    sort_by: str = "created_at",
    sort_order: str = "desc",
) -> tuple[list[Job], int]:
    """
    List jobs with optional filters, pagination, and sorting.

    Args:
        session: Active async database session.
        status: Filter by job status.
        priority: Filter by priority level.
        job_type: Filter by simulation type.
        dag_id: Filter by parent DAG.
        created_after: Only jobs created after this timestamp.
        created_before: Only jobs created before this timestamp.
        limit: Max results per page (capped at 200).
        offset: Number of results to skip.
        sort_by: Column to sort by ('created_at', 'updated_at', 'priority').
        sort_order: 'asc' or 'desc'.

    Returns:
        Tuple of (list of jobs, total count matching filters).
    """
    # Build filter conditions
    conditions = []
    if status:
        conditions.append(Job.status == status)
    if priority:
        conditions.append(Job.priority == priority)
    if job_type:
        conditions.append(Job.type == job_type)
    if dag_id:
        conditions.append(Job.dag_id == dag_id)
    if created_after:
        conditions.append(Job.created_at >= created_after)
    if created_before:
        conditions.append(Job.created_at <= created_before)

    # Count total matching results
    count_stmt = select(func.count(Job.id))
    if conditions:
        count_stmt = count_stmt.where(and_(*conditions))
    total = (await session.execute(count_stmt)).scalar() or 0

    # Build the query with sorting
    sort_column = getattr(Job, sort_by, Job.created_at)
    if sort_order == "asc":
        order = sort_column.asc()
    else:
        order = sort_column.desc()

    query = select(Job).order_by(order).limit(limit).offset(offset)
    if conditions:
        query = query.where(and_(*conditions))

    result = await session.execute(query)
    jobs = list(result.scalars().all())

    return jobs, total


async def update_job_progress(
    session: AsyncSession,
    job_id: uuid.UUID,
    progress_percent: float,
    progress_message: str | None = None,
) -> None:
    """
    Update a job's progress indicators during execution.

    This is a lightweight update that doesn't go through the state machine
    because it doesn't change the job's status.

    Args:
        session: Active async database session.
        job_id: The job's UUID.
        progress_percent: Completion percentage (0.0 to 100.0).
        progress_message: Optional human-readable progress description.
    """
    from sqlalchemy import update
    stmt = (
        update(Job)
        .where(Job.id == job_id)
        .values(progress_percent=progress_percent, progress_message=progress_message)
    )
    await session.execute(stmt)


async def update_job_lease(
    session: AsyncSession,
    job_id: uuid.UUID,
    lease_expires_at: datetime,
) -> None:
    """
    Refresh a job's lease expiration timestamp.

    Called by the worker's heartbeat loop to extend the lease,
    preventing the reaper from reclaiming the job while it's still alive.

    Args:
        session: Active async database session.
        job_id: The job's UUID.
        lease_expires_at: New lease expiration timestamp.
    """
    from sqlalchemy import update
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status == "running")
        .values(lease_expires_at=lease_expires_at)
    )
    await session.execute(stmt)
