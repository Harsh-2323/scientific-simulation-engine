"""
DAG executor — manages node lifecycle within a running DAG.

Handles the progression of a DAG as individual jobs complete or fail:
enqueuing child nodes when parents finish, applying failure policies
when nodes die, and detecting when the overall DAG is done.
"""

import uuid

import redis.asyncio as redis
from sqlalchemy import select, update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.state_machine import transition_job
from src.models.dag import DagGraph, DagEdge
from src.models.enums import JobStatus, DagStatus
from src.models.job import Job
from src.queue.publisher import enqueue_job

import structlog

logger = structlog.get_logger(__name__)

# Terminal states — a job in one of these states will not change further
TERMINAL_STATUSES = {"completed", "cancelled", "dead_letter"}


async def on_node_completed(
    session: AsyncSession,
    redis_client: redis.Redis,
    job: Job,
) -> None:
    """
    Called when a DAG node job transitions to 'completed'.

    Checks if the completed node's children are now unblocked
    (all parents completed), and if so, enqueues them. Also
    checks if the entire DAG is finished.

    Args:
        session: Active async database session.
        redis_client: Active async Redis client.
        job: The job that just completed (must have dag_id and dag_node_id set).
    """
    if not job.dag_id or not job.dag_node_id:
        return

    dag_id = job.dag_id
    node_id = job.dag_node_id

    logger.info(
        "dag_node_completed",
        dag_id=str(dag_id),
        node_id=node_id,
        job_id=str(job.id),
    )

    # Find all child nodes that depend on the completed node
    child_edges = (await session.execute(
        select(DagEdge.child_node_id).where(
            DagEdge.dag_id == dag_id,
            DagEdge.parent_node_id == node_id,
        )
    )).scalars().all()

    for child_node_id in child_edges:
        # Check if ALL parents of this child are completed
        parent_edges = (await session.execute(
            select(DagEdge.parent_node_id).where(
                DagEdge.dag_id == dag_id,
                DagEdge.child_node_id == child_node_id,
            )
        )).scalars().all()

        # Get the status of all parent jobs
        parent_jobs = (await session.execute(
            select(Job.status).where(
                Job.dag_id == dag_id,
                Job.dag_node_id.in_(parent_edges),
            )
        )).scalars().all()

        # Only enqueue the child if every parent has completed
        all_parents_completed = all(s == "completed" for s in parent_jobs)

        if all_parents_completed:
            # Find the child job and enqueue it
            child_job = (await session.execute(
                select(Job).where(
                    Job.dag_id == dag_id,
                    Job.dag_node_id == child_node_id,
                )
            )).scalar_one_or_none()

            if child_job and child_job.status == "pending":
                updated = await transition_job(
                    session,
                    child_job.id,
                    JobStatus.QUEUED,
                    expected_status=JobStatus.PENDING,
                    triggered_by="dag_executor",
                    metadata={"dag_id": str(dag_id), "triggered_by_node": node_id},
                )

                if updated:
                    await enqueue_job(redis_client, child_job.id, child_job.priority)
                    logger.info(
                        "dag_child_enqueued",
                        dag_id=str(dag_id),
                        child_node_id=child_node_id,
                        job_id=str(child_job.id),
                    )

    # Check if the entire DAG is complete
    await _check_dag_completion(session, dag_id)


async def on_node_failed(
    session: AsyncSession,
    redis_client: redis.Redis,
    job: Job,
) -> None:
    """
    Called when a DAG node job transitions to 'dead_letter'.

    Applies the DAG's failure policy to handle downstream nodes
    and update the overall DAG status.

    Args:
        session: Active async database session.
        redis_client: Active async Redis client.
        job: The job that reached dead_letter (must have dag_id set).
    """
    if not job.dag_id or not job.dag_node_id:
        return

    dag_id = job.dag_id
    node_id = job.dag_node_id

    # Load the DAG to get the failure policy
    dag = (await session.execute(
        select(DagGraph).where(DagGraph.id == dag_id)
    )).scalar_one_or_none()

    if not dag:
        return

    logger.warning(
        "dag_node_dead_lettered",
        dag_id=str(dag_id),
        node_id=node_id,
        failure_policy=dag.failure_policy,
    )

    if dag.failure_policy == "fail_fast":
        # fail_fast already sets the DAG to 'failed' — don't recheck,
        # because _check_dag_completion would soften it to 'partially_failed'
        # when some nodes completed before the failure.
        await _apply_fail_fast(session, dag_id)
    elif dag.failure_policy == "skip_downstream":
        await _apply_skip_downstream(session, dag_id, node_id)
        await _check_dag_completion(session, dag_id)
    elif dag.failure_policy == "retry_node":
        # retry_node falls back to skip_downstream when a node hits dead_letter
        await _apply_skip_downstream(session, dag_id, node_id)
        await _check_dag_completion(session, dag_id)


async def _apply_fail_fast(session: AsyncSession, dag_id: uuid.UUID) -> None:
    """
    Cancel ALL non-terminal nodes in the DAG and mark DAG as failed.

    Used with the fail_fast failure policy — any node failure
    cancels everything else immediately.

    Args:
        session: Active async database session.
        dag_id: UUID of the DAG to fail.
    """
    # Find all non-terminal jobs in this DAG
    non_terminal_jobs = (await session.execute(
        select(Job).where(
            Job.dag_id == dag_id,
            Job.status.notin_(TERMINAL_STATUSES),
        )
    )).scalars().all()

    for job in non_terminal_jobs:
        try:
            # Cancel from whatever state the job is in
            if job.status in ("pending", "queued"):
                target_status = JobStatus.CANCELLED
                expected = [JobStatus.PENDING, JobStatus.QUEUED]
            elif job.status == "running":
                target_status = JobStatus.CANCELLING
                expected = [JobStatus.RUNNING]
            elif job.status == "paused":
                target_status = JobStatus.CANCELLED
                expected = [JobStatus.PAUSED]
            else:
                continue

            await transition_job(
                session, job.id, target_status,
                expected_status=expected,
                triggered_by="dag_executor",
                metadata={"reason": "fail_fast_policy"},
            )
        except Exception as e:
            logger.warning("fail_fast_cancel_error", job_id=str(job.id), error=str(e))

    # Mark the DAG as failed
    await session.execute(
        sa_update(DagGraph)
        .where(DagGraph.id == dag_id)
        .values(status="failed")
    )

    logger.info("dag_failed_fast", dag_id=str(dag_id))


async def _apply_skip_downstream(
    session: AsyncSession,
    dag_id: uuid.UUID,
    failed_node_id: str,
) -> None:
    """
    Cancel only downstream dependents of the failed node.

    Uses a recursive walk through the dependency graph to find
    all transitive descendants, then cancels them. Other branches
    continue to run normally.

    Args:
        session: Active async database session.
        dag_id: UUID of the DAG.
        failed_node_id: The node that failed (dead_letter).
    """
    # Find all transitive descendants using BFS
    descendants = set()
    queue = [failed_node_id]

    while queue:
        current = queue.pop(0)
        child_edges = (await session.execute(
            select(DagEdge.child_node_id).where(
                DagEdge.dag_id == dag_id,
                DagEdge.parent_node_id == current,
            )
        )).scalars().all()

        for child_id in child_edges:
            if child_id not in descendants:
                descendants.add(child_id)
                queue.append(child_id)

    # Cancel all descendant jobs that haven't completed
    for desc_node_id in descendants:
        desc_job = (await session.execute(
            select(Job).where(
                Job.dag_id == dag_id,
                Job.dag_node_id == desc_node_id,
            )
        )).scalar_one_or_none()

        if desc_job and desc_job.status not in TERMINAL_STATUSES:
            try:
                if desc_job.status in ("pending", "queued"):
                    await transition_job(
                        session, desc_job.id, JobStatus.CANCELLED,
                        expected_status=[JobStatus.PENDING, JobStatus.QUEUED],
                        triggered_by="dag_executor",
                        metadata={"reason": "skip_downstream", "failed_node": failed_node_id},
                    )
                elif desc_job.status == "paused":
                    await transition_job(
                        session, desc_job.id, JobStatus.CANCELLED,
                        expected_status=JobStatus.PAUSED,
                        triggered_by="dag_executor",
                        metadata={"reason": "skip_downstream", "failed_node": failed_node_id},
                    )
            except Exception as e:
                logger.warning(
                    "skip_downstream_cancel_error",
                    job_id=str(desc_job.id), error=str(e),
                )

    logger.info(
        "dag_skip_downstream",
        dag_id=str(dag_id),
        failed_node=failed_node_id,
        cancelled_descendants=len(descendants),
    )


async def _check_dag_completion(session: AsyncSession, dag_id: uuid.UUID) -> None:
    """
    Check if all jobs in the DAG have reached a terminal state.

    If all are terminal, determine the overall DAG status:
    - All completed → 'completed'
    - Any cancelled/dead_letter but some completed → 'partially_failed'
    - All cancelled/dead_letter → 'failed'

    Args:
        session: Active async database session.
        dag_id: UUID of the DAG to check.
    """
    # Get all job statuses for this DAG
    statuses = (await session.execute(
        select(Job.status).where(Job.dag_id == dag_id)
    )).scalars().all()

    if not statuses:
        return

    # Check if all jobs are in a terminal state
    all_terminal = all(s in TERMINAL_STATUSES for s in statuses)

    if not all_terminal:
        return

    # Determine overall DAG status
    all_completed = all(s == "completed" for s in statuses)
    any_completed = any(s == "completed" for s in statuses)

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    if all_completed:
        dag_status = "completed"
    elif any_completed:
        dag_status = "partially_failed"
    else:
        dag_status = "failed"

    await session.execute(
        sa_update(DagGraph)
        .where(DagGraph.id == dag_id)
        .values(status=dag_status, completed_at=now)
    )

    logger.info("dag_completed", dag_id=str(dag_id), status=dag_status)
