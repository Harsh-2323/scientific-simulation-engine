"""
Job API routes — submit, list, get, and view events/checkpoints.

These endpoints are the primary interface for clients submitting
simulation jobs and monitoring their progress. All state mutations
go through the state machine to ensure consistency.
"""

import uuid
from datetime import datetime

import redis.asyncio as redis
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import get_db, get_redis
from src.core.exceptions import BackpressureError, ConflictError
from src.core.state_machine import transition_job
from src.db.repositories import job_repo
from src.db.repositories import event_repo
from src.models.checkpoint import Checkpoint
from src.models.enums import JobStatus, JobPriority
from src.queue.publisher import enqueue_job
from src.schemas.job import (
    CheckpointListResponse,
    CheckpointResponse,
    EventListResponse,
    EventResponse,
    JobDetailResponse,
    JobListParams,
    JobListResponse,
    JobResponse,
    JobSubmitRequest,
)
from src.config import settings

import structlog

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("", response_model=JobResponse, status_code=201)
async def submit_job(
    request: JobSubmitRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis),
) -> JobResponse:
    """
    Submit a new simulation job for execution.

    The job is created with status='pending', immediately transitioned
    to 'queued', and enqueued into the appropriate Redis priority stream.

    Supports idempotency: if a job with the same idempotency_key already
    exists and has identical params, returns the existing job. If params
    differ, returns 409 Conflict.

    Returns 429 Too Many Requests if the total queue depth exceeds
    the backpressure limit.
    """
    # Handle idempotency key deduplication
    if request.idempotency_key:
        existing = await job_repo.get_job_by_idempotency_key(db, request.idempotency_key)
        if existing:
            # Same key exists — check if params match
            if existing.params == request.params:
                return JobResponse.from_job(existing)
            else:
                raise HTTPException(
                    status_code=409,
                    detail="Idempotency key already exists with different parameters",
                )

    # Check backpressure — reject if queue is overloaded
    try:
        total_depth = await _get_total_queue_depth(redis_client)
        if total_depth > settings.backpressure_queue_depth_limit:
            raise BackpressureError(total_depth, settings.backpressure_queue_depth_limit)
    except BackpressureError:
        raise HTTPException(
            status_code=429,
            detail="System is under heavy load. Please retry later.",
            headers={"Retry-After": "30"},
        )

    # Create the job in 'pending' state
    job = await job_repo.create_job(
        db,
        job_type=request.type,
        params=request.params,
        priority=request.priority.value,
        max_retries=request.max_retries,
        idempotency_key=request.idempotency_key,
        metadata=request.metadata,
    )

    # Transition pending -> queued (writes event log)
    job = await transition_job(
        db,
        job.id,
        JobStatus.QUEUED,
        expected_status=JobStatus.PENDING,
        triggered_by="api",
    )

    # Flush so the job row is visible to workers reading from Postgres
    await db.flush()

    # Enqueue into the Redis stream for the job's priority level
    await enqueue_job(redis_client, job.id, job.priority)

    logger.info("job_submitted", job_id=str(job.id), job_type=request.type)
    return JobResponse.from_job(job)


@router.get("/{job_id}", response_model=JobDetailResponse)
async def get_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> JobDetailResponse:
    """
    Get a single job by ID, including its execution attempts.

    Returns 404 if the job doesn't exist.
    """
    job = await job_repo.get_job_by_id(db, job_id, load_attempts=True)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    return JobDetailResponse.from_job_with_attempts(job, job.attempts)


@router.get("", response_model=JobListResponse)
async def list_jobs(
    status: str | None = Query(None),
    priority: str | None = Query(None),
    type: str | None = Query(None, alias="type"),
    dag_id: uuid.UUID | None = Query(None),
    created_after: datetime | None = Query(None),
    created_before: datetime | None = Query(None),
    limit: int = Query(50, le=200, ge=1),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("created_at"),
    sort_order: str = Query("desc"),
    db: AsyncSession = Depends(get_db),
) -> JobListResponse:
    """
    List jobs with optional filters, pagination, and sorting.

    Supports filtering by status, priority, type, DAG membership,
    and creation time range.
    """
    jobs, total = await job_repo.list_jobs(
        db,
        status=status,
        priority=priority,
        job_type=type,
        dag_id=dag_id,
        created_after=created_after,
        created_before=created_before,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_order=sort_order,
    )

    return JobListResponse(
        items=[JobResponse.from_job(j) for j in jobs],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{job_id}/events", response_model=EventListResponse)
async def get_job_events(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> EventListResponse:
    """
    Get the audit log of state transitions for a specific job.

    Returns events in chronological order. Useful for debugging
    job lifecycle issues and understanding what happened.
    """
    # Verify job exists
    job = await job_repo.get_job_by_id(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    events, total = await event_repo.list_events_for_job(db, job_id)

    return EventListResponse(
        items=[EventResponse.from_event(e) for e in events],
        total=total,
    )


@router.get("/{job_id}/checkpoints", response_model=CheckpointListResponse)
async def get_job_checkpoints(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> CheckpointListResponse:
    """
    List all checkpoints for a job, with a data preview for each.

    Only shows the first 3 keys of checkpoint data to keep responses
    manageable — full checkpoint data is internal to the worker.
    """
    # Verify job exists
    job = await job_repo.get_job_by_id(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    # Query checkpoints ordered by step number descending (newest first)
    stmt = (
        select(Checkpoint)
        .where(Checkpoint.job_id == job_id)
        .order_by(Checkpoint.step_number.desc())
    )
    result = await db.execute(stmt)
    checkpoints = list(result.scalars().all())

    items = []
    for cp in checkpoints:
        # Build a preview of the first 3 keys in the checkpoint data
        preview_keys = list(cp.data.keys())[:3]
        data_preview = {k: cp.data[k] for k in preview_keys}

        items.append(CheckpointResponse(
            id=cp.id,
            step_number=cp.step_number,
            created_at=cp.created_at,
            is_valid=cp.is_valid,
            data_preview=data_preview,
        ))

    return CheckpointListResponse(items=items, total=len(items))


async def _get_total_queue_depth(redis_client: redis.Redis) -> int:
    """
    Sum the length of all 4 priority Redis streams.

    Used for backpressure checks before accepting new job submissions.
    """
    total = 0
    for priority in ["critical", "high", "normal", "low"]:
        stream_name = f"jobs:priority:{priority}"
        try:
            length = await redis_client.xlen(stream_name)
            total += length
        except Exception:
            # Stream may not exist yet — that's fine, count as 0
            pass
    return total
