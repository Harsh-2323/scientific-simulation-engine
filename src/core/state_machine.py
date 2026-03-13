"""
Job state machine — the single authority for all status transitions.

Every job status change in the system MUST go through transition_job().
Direct assignment of job.status is forbidden because it bypasses the
atomic WHERE-guard that prevents race conditions between concurrent
workers, the reaper, and the API server.

The WHERE guard works like this:
    UPDATE jobs SET status = 'running'
    WHERE id = ? AND status = 'queued'
    RETURNING id;

If another worker already claimed the job (status is no longer 'queued'),
the UPDATE affects 0 rows and we know the transition was rejected.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import update, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.job import Job, JobEvent
from src.models.enums import JobStatus
from src.core.exceptions import InvalidTransitionError

import structlog

logger = structlog.get_logger(__name__)

# Defines every legal state transition in the system.
# If a (from_status, to_status) pair is not in here, it's a bug.
VALID_TRANSITIONS: dict[str, list[str]] = {
    "pending":          ["queued", "cancelled"],
    "queued":           ["running", "cancelled"],
    "running":          ["completed", "failed", "cancelling", "pausing", "queued"],
    "pausing":          ["paused", "failed"],
    "paused":           ["resuming", "cancelled"],
    "resuming":         ["running", "failed"],
    "cancelling":       ["cancelled", "failed"],
    "failed":           ["retry_scheduled", "dead_letter"],
    "retry_scheduled":  ["queued"],
    "completed":        [],       # Terminal — no transitions out
    "cancelled":        [],       # Terminal
    "dead_letter":      [],       # Terminal
}


def is_valid_transition(from_status: str, to_status: str) -> bool:
    """
    Check whether a state transition is permitted by the state machine.

    Args:
        from_status: The current job status.
        to_status: The desired target status.

    Returns:
        True if the transition is allowed, False otherwise.
    """
    return to_status in VALID_TRANSITIONS.get(from_status, [])


async def transition_job(
    session: AsyncSession,
    job_id: uuid.UUID,
    new_status: JobStatus,
    *,
    expected_status: JobStatus | list[JobStatus],
    worker_id: str | None = None,
    error_message: str | None = None,
    error_type: str | None = None,
    result: dict | None = None,
    triggered_by: str,
    metadata: dict | None = None,
) -> Job | None:
    """
    Atomically transition a job to a new status.

    Uses a WHERE guard on the expected current status to prevent race
    conditions. If the job's status has already changed (another process
    got there first), the UPDATE affects 0 rows and we return None.

    Args:
        session: Active async database session.
        job_id: UUID of the job to transition.
        new_status: The target status.
        expected_status: The status(es) the job must currently be in.
        worker_id: Worker ID to set (for claim transitions) or match (for worker-guarded transitions).
        error_message: Error details (for failure transitions).
        error_type: 'transient' or 'permanent' (for failure transitions).
        result: Final result data (for completion transitions).
        triggered_by: Who initiated this transition ('api', 'worker', 'reaper', 'scheduler', 'dag_executor').
        metadata: Additional context to store in the event log.

    Returns:
        The updated Job if the transition succeeded (1 row affected),
        or None if the transition was rejected (0 rows — someone else got there first).

    Raises:
        InvalidTransitionError: If the transition is not in VALID_TRANSITIONS.
    """
    # Normalize expected_status to a list for uniform handling
    # Accept both JobStatus enums and plain strings
    if isinstance(expected_status, (str, JobStatus)):
        expected_statuses = [expected_status]
    else:
        expected_statuses = list(expected_status)

    new_status_value = new_status.value if isinstance(new_status, JobStatus) else new_status

    # Validate that every expected_status -> new_status is a legal transition
    for exp in expected_statuses:
        exp_value = exp.value if isinstance(exp, JobStatus) else exp
        if not is_valid_transition(exp_value, new_status_value):
            raise InvalidTransitionError(
                job_id=str(job_id),
                current_status=exp_value,
                target_status=new_status_value,
            )

    # Build the atomic UPDATE with WHERE guard
    now = datetime.now(timezone.utc)
    expected_values = [e.value if isinstance(e, JobStatus) else e for e in expected_statuses]

    update_values: dict = {
        "status": new_status_value,
        "updated_at": now,
    }

    # Set transition-specific fields based on the target status
    if new_status_value == "queued":
        update_values["queued_at"] = now
        update_values["error_message"] = None
        update_values["error_type"] = None

    if new_status_value == "running":
        update_values["started_at"] = now
        if worker_id:
            update_values["worker_id"] = worker_id
            # Lease timeout is set by the caller via a separate heartbeat update

    if new_status_value in ("completed", "cancelled", "dead_letter"):
        update_values["completed_at"] = now

    if new_status_value == "completed" and result is not None:
        update_values["result"] = result
        update_values["progress_percent"] = 100.0

    if new_status_value == "failed":
        if error_message:
            update_values["error_message"] = error_message
        if error_type:
            update_values["error_type"] = error_type

    if new_status_value == "paused":
        update_values["worker_id"] = None
        update_values["lease_expires_at"] = None

    if new_status_value == "cancelled":
        update_values["worker_id"] = None

    # Build WHERE conditions
    conditions = [
        Job.id == job_id,
        Job.status.in_(expected_values),
    ]

    # Execute the atomic UPDATE
    stmt = (
        update(Job)
        .where(and_(*conditions))
        .values(**update_values)
        .returning(Job)
    )

    row = await session.execute(stmt)
    updated_job = row.scalars().first()

    if updated_job is None:
        # Transition rejected — another process changed the status first
        logger.warning(
            "transition_rejected",
            job_id=str(job_id),
            expected_status=expected_values,
            target_status=new_status_value,
            triggered_by=triggered_by,
        )
        return None

    # Record the state transition in the audit log.
    # Always use the first expected status for old_status — when multiple
    # expected statuses are passed (e.g., ['pending', 'queued']), we can't
    # know which one the job was actually in, but the first is representative
    # and must be a valid enum value for the job_status column.
    old_status = expected_values[0]
    event = JobEvent(
        job_id=job_id,
        event_type="status_change",
        old_status=old_status,
        new_status=new_status_value,
        triggered_by=triggered_by,
        timestamp=now,
        metadata_=metadata or {},
    )
    session.add(event)
    # Flush the event INSERT immediately so the session is clean for
    # the next operation. Without this, asyncpg can fail with "another
    # operation is in progress" when multiple transitions run in sequence
    # on the same session (the pending INSERT collides with the next UPDATE).
    await session.flush()

    logger.info(
        "job_transition",
        job_id=str(job_id),
        old_status=old_status,
        new_status=new_status_value,
        triggered_by=triggered_by,
    )

    return updated_job
