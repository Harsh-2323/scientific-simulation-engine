"""
Job control endpoints — pause, resume, and cancel running jobs.

These endpoints modify job state through the state machine and
publish control signals via Redis Pub/Sub to notify workers in
real-time. Workers check for signals between simulation steps.
"""

import uuid

import redis.asyncio as redis
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import get_db, get_redis
from src.core.state_machine import transition_job
from src.db.repositories import job_repo, checkpoint_repo
from src.models.enums import JobStatus
from src.queue.publisher import enqueue_job
from src.schemas.job import JobResponse
from src.worker.signal_handler import publish_control_signal

import structlog

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/jobs", tags=["control"])


@router.post("/{job_id}/cancel", response_model=JobResponse)
async def cancel_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis),
) -> JobResponse:
    """
    Cancel a job that is queued, running, or paused.

    Behavior depends on current status:
      - queued: Immediately transition to cancelled.
      - running: Transition to cancelling, publish cancel signal to worker.
                 Worker will stop at next step boundary and confirm cancellation.
      - paused: Immediately transition to cancelled.

    Returns 409 if the job is in a status that cannot be cancelled.
    """
    job = await job_repo.get_job_by_id(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    if job.status == "queued":
        # Direct cancellation — no worker involved
        updated = await transition_job(
            db, job_id, JobStatus.CANCELLED,
            expected_status=JobStatus.QUEUED,
            triggered_by="api",
        )
        if not updated:
            raise HTTPException(status_code=409, detail="Job status changed during cancellation")
        await db.flush()

        logger.info("queued_job_cancelled", job_id=str(job_id))
        return JobResponse.from_job(updated)

    elif job.status == "running":
        # Transition to cancelling and notify the worker via Pub/Sub
        updated = await transition_job(
            db, job_id, JobStatus.CANCELLING,
            expected_status=JobStatus.RUNNING,
            triggered_by="api",
        )
        if not updated:
            raise HTTPException(status_code=409, detail="Job status changed during cancellation")
        await db.flush()

        # Publish cancel signal — worker picks this up between steps
        await publish_control_signal(redis_client, job_id, "cancel")

        logger.info("running_job_cancelling", job_id=str(job_id))
        return JobResponse.from_job(updated)

    elif job.status == "paused":
        # Direct cancellation — job is idle, no worker involved
        updated = await transition_job(
            db, job_id, JobStatus.CANCELLED,
            expected_status=JobStatus.PAUSED,
            triggered_by="api",
        )
        if not updated:
            raise HTTPException(status_code=409, detail="Job status changed during cancellation")
        await db.flush()

        logger.info("paused_job_cancelled", job_id=str(job_id))
        return JobResponse.from_job(updated)

    else:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel job in status '{job.status}'",
        )


@router.post("/{job_id}/pause", response_model=JobResponse)
async def pause_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis),
) -> JobResponse:
    """
    Pause a running job.

    Transitions to 'pausing' and publishes a pause signal. The worker
    completes its current step, writes a checkpoint with the full
    simulation state, and transitions to 'paused'. The job can then
    be resumed later from that checkpoint.

    Returns 409 if the job is not currently running.
    """
    job = await job_repo.get_job_by_id(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    if job.status != "running":
        raise HTTPException(
            status_code=409,
            detail=f"Cannot pause job in status '{job.status}'. Only running jobs can be paused.",
        )

    # Transition running -> pausing
    updated = await transition_job(
        db, job_id, JobStatus.PAUSING,
        expected_status=JobStatus.RUNNING,
        triggered_by="api",
    )
    if not updated:
        raise HTTPException(status_code=409, detail="Job status changed during pause request")
    await db.flush()

    # Publish pause signal — worker saves checkpoint and yields
    await publish_control_signal(redis_client, job_id, "pause")

    logger.info("job_pausing", job_id=str(job_id))
    return JobResponse.from_job(updated)


@router.post("/{job_id}/resume", response_model=JobResponse)
async def resume_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis),
) -> JobResponse:
    """
    Resume a paused job from its last checkpoint.

    Verifies a valid checkpoint exists, transitions to 'resuming',
    and re-enqueues the job into the Redis stream. A worker will
    claim it, load the checkpoint, and continue from the saved step.

    Returns 409 if the job is not paused or has no valid checkpoint.
    """
    job = await job_repo.get_job_by_id(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    if job.status != "paused":
        raise HTTPException(
            status_code=409,
            detail=f"Cannot resume job in status '{job.status}'. Only paused jobs can be resumed.",
        )

    # Verify a valid checkpoint exists for this job
    has_checkpoint = await checkpoint_repo.has_valid_checkpoint(db, job_id)
    if not has_checkpoint:
        raise HTTPException(
            status_code=409,
            detail="No valid checkpoint to resume from",
        )

    # Transition paused -> resuming
    updated = await transition_job(
        db, job_id, JobStatus.RESUMING,
        expected_status=JobStatus.PAUSED,
        triggered_by="api",
    )
    if not updated:
        raise HTTPException(status_code=409, detail="Job status changed during resume request")
    await db.flush()

    # Re-enqueue to Redis stream at the same priority level
    await enqueue_job(redis_client, job_id, job.priority)

    logger.info("job_resuming", job_id=str(job_id))
    return JobResponse.from_job(updated)
