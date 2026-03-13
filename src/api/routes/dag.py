"""
DAG API routes — submit, query, decompose, and cancel DAGs.

These endpoints handle the full DAG lifecycle: manual submission,
LLM-powered decomposition, status queries, and cancellation.
"""

import uuid

import redis.asyncio as redis
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import get_db, get_redis
from src.core.exceptions import LLMError, LLMParseError, LLMValidationError
from src.core.state_machine import transition_job
from src.dag.validator import validate_dag, topological_sort, get_root_nodes
from src.dag.llm_planner import decompose_instruction
from src.db.repositories import dag_repo, job_repo
from src.models.dag import DagGraph
from src.models.enums import JobStatus
from src.models.job import Job
from src.queue.publisher import enqueue_job
from src.schemas.dag import (
    DagEdgeResponse,
    DagListResponse,
    DagNodeResponse,
    DagResponse,
    DagSubmitRequest,
    ValidationResult,
)
from src.schemas.llm import DecomposeRequest, DecomposeResponse

import structlog

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/dags", tags=["dags"])


@router.post("", response_model=DagResponse, status_code=201)
async def submit_dag(
    request: DagSubmitRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis),
) -> DagResponse:
    """
    Submit a DAG of simulation jobs for execution.

    Validates the graph structure (cycles, references, limits),
    creates all job rows and edges in a single transaction, then
    enqueues root nodes to start execution.

    Returns 422 if validation fails.
    """
    # Validate the DAG structure
    validation = validate_dag(request)
    if not validation.valid:
        raise HTTPException(status_code=422, detail={
            "message": "DAG validation failed",
            "errors": validation.errors,
            "warnings": validation.warnings,
        })

    # Create the DAG within a transaction
    dag_response = await _create_dag_from_spec(db, redis_client, request)
    return dag_response


@router.get("/{dag_id}", response_model=DagResponse)
async def get_dag(
    dag_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> DagResponse:
    """
    Get a DAG by ID with its nodes and edges.

    Returns 404 if the DAG doesn't exist.
    """
    dag = await dag_repo.get_dag_by_id(db, dag_id)
    if not dag:
        raise HTTPException(status_code=404, detail=f"DAG not found: {dag_id}")

    jobs = await dag_repo.get_dag_jobs(db, dag_id)
    return _build_dag_response(dag, jobs)


@router.get("", response_model=DagListResponse)
async def list_dags(
    limit: int = Query(50, le=200, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> DagListResponse:
    """List all DAGs with pagination."""
    dags, total = await dag_repo.list_dags(db, limit=limit, offset=offset)

    items = []
    for dag in dags:
        jobs = await dag_repo.get_dag_jobs(db, dag.id)
        items.append(_build_dag_response(dag, jobs))

    return DagListResponse(items=items, total=total)


@router.post("/{dag_id}/cancel", response_model=DagResponse)
async def cancel_dag(
    dag_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis),
) -> DagResponse:
    """
    Cancel an entire DAG and all its non-terminal jobs.

    Returns 404 if the DAG doesn't exist.
    Returns 409 if the DAG is already in a terminal state.
    """
    dag = await dag_repo.get_dag_by_id(db, dag_id)
    if not dag:
        raise HTTPException(status_code=404, detail=f"DAG not found: {dag_id}")

    if dag.status in ("completed", "failed", "cancelled", "partially_failed"):
        raise HTTPException(
            status_code=409,
            detail=f"DAG is already in terminal state: {dag.status}",
        )

    # Cancel all non-terminal jobs
    jobs = await dag_repo.get_dag_jobs(db, dag_id)
    terminal = {"completed", "cancelled", "dead_letter"}

    for job in jobs:
        if job.status in terminal:
            continue

        try:
            if job.status in ("pending", "queued"):
                await transition_job(
                    db, job.id, JobStatus.CANCELLED,
                    expected_status=[JobStatus.PENDING, JobStatus.QUEUED],
                    triggered_by="api",
                    metadata={"reason": "dag_cancelled"},
                )
            elif job.status == "running":
                await transition_job(
                    db, job.id, JobStatus.CANCELLING,
                    expected_status=JobStatus.RUNNING,
                    triggered_by="api",
                    metadata={"reason": "dag_cancelled"},
                )
                # Publish cancel signal to the worker
                from src.worker.signal_handler import publish_control_signal
                await publish_control_signal(redis_client, job.id, "cancel")
            elif job.status == "paused":
                await transition_job(
                    db, job.id, JobStatus.CANCELLED,
                    expected_status=JobStatus.PAUSED,
                    triggered_by="api",
                    metadata={"reason": "dag_cancelled"},
                )
        except Exception as e:
            logger.warning("dag_cancel_job_error", job_id=str(job.id), error=str(e))

    # Update DAG status
    from sqlalchemy import update
    from datetime import datetime, timezone
    await db.execute(
        update(DagGraph)
        .where(DagGraph.id == dag_id)
        .values(status="cancelled", completed_at=datetime.now(timezone.utc))
    )
    await db.commit()

    # Reload for response
    dag = await dag_repo.get_dag_by_id(db, dag_id)
    jobs = await dag_repo.get_dag_jobs(db, dag_id)
    return _build_dag_response(dag, jobs)


@router.post("/decompose", response_model=DecomposeResponse, status_code=201)
async def decompose_dag(
    request: DecomposeRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis),
) -> DecomposeResponse:
    """
    Use an LLM to decompose a natural language instruction into a DAG.

    Calls the Anthropic API to generate a simulation plan, validates
    the structure, and optionally creates the DAG (unless dry_run=True).

    Returns 422 if the LLM output fails validation.
    Returns 502 if the LLM API call fails.
    """
    # Call the LLM to generate a DAG spec
    try:
        dag_spec, model_used, latency_ms = await decompose_instruction(
            request.instruction,
            available_types=request.available_types,
            failure_policy=request.failure_policy,
        )
    except LLMError as e:
        raise HTTPException(status_code=502, detail=f"LLM API error: {str(e)}")
    except LLMParseError as e:
        raise HTTPException(status_code=422, detail=f"Failed to parse LLM response: {str(e)}")
    except LLMValidationError as e:
        raise HTTPException(status_code=422, detail=f"Invalid LLM output: {str(e)}")

    # Validate the generated spec
    validation = validate_dag(dag_spec)

    # Build response
    dag_response = None

    if not request.dry_run and validation.valid:
        dag_response = await _create_dag_from_spec(
            db, redis_client, dag_spec,
            natural_language_input=request.instruction,
            raw_llm_output={"model": model_used, "spec": dag_spec.model_dump()},
        )

    return DecomposeResponse(
        dag=dag_response,
        dag_spec=dag_spec,
        validation_result=validation,
        llm_model=model_used,
        llm_latency_ms=latency_ms,
    )


async def _create_dag_from_spec(
    db: AsyncSession,
    redis_client: redis.Redis,
    spec: DagSubmitRequest,
    *,
    natural_language_input: str | None = None,
    raw_llm_output: dict | None = None,
) -> DagResponse:
    """
    Create a DAG and all its jobs/edges from a validated specification.

    Steps:
        1. Insert the dag_graphs row
        2. Insert job rows for each node (in topological order)
        3. Insert dag_edges rows
        4. Enqueue root nodes
        5. Set DAG status to 'running'

    Args:
        db: Active async database session.
        redis_client: Active async Redis client.
        spec: Validated DAG specification.
        natural_language_input: Original LLM input (if applicable).
        raw_llm_output: Raw LLM response (if applicable).

    Returns:
        Complete DagResponse with all nodes and edges.
    """
    # Create the DAG row
    dag = await dag_repo.create_dag(
        db,
        name=spec.name,
        description=spec.description,
        failure_policy=spec.failure_policy,
        metadata=spec.metadata,
        natural_language_input=natural_language_input,
        raw_llm_output=raw_llm_output,
    )

    # Build a lookup for node specs
    node_map = {n.node_id: n for n in spec.nodes}

    # Create jobs in topological order
    sorted_node_ids = topological_sort(spec.nodes)
    job_map: dict[str, Job] = {}

    for node_id in sorted_node_ids:
        node = node_map[node_id]
        job = await job_repo.create_job(
            db,
            job_type=node.job_type,
            params=node.params,
            priority=node.priority,
            max_retries=node.max_retries,
            dag_id=dag.id,
            dag_node_id=node.node_id,
        )
        job_map[node_id] = job

    # Create edges
    for node in spec.nodes:
        for dep in node.depends_on:
            await dag_repo.create_dag_edge(
                db,
                dag_id=dag.id,
                parent_node_id=dep,
                child_node_id=node.node_id,
            )

    # Enqueue root nodes (those with no dependencies)
    root_nodes = get_root_nodes(spec.nodes)
    for root in root_nodes:
        root_job = job_map[root.node_id]
        await transition_job(
            db, root_job.id, JobStatus.QUEUED,
            expected_status=JobStatus.PENDING,
            triggered_by="dag_executor",
            metadata={"dag_id": str(dag.id)},
        )
        await enqueue_job(redis_client, root_job.id, root_job.priority)

    # Set DAG status to running
    from sqlalchemy import update
    await db.execute(
        update(DagGraph)
        .where(DagGraph.id == dag.id)
        .values(status="running")
    )

    await db.commit()

    # Reload DAG and jobs for response
    dag = await dag_repo.get_dag_by_id(db, dag.id)
    jobs = list(job_map.values())

    # Refresh jobs to get updated statuses
    refreshed_jobs = await dag_repo.get_dag_jobs(db, dag.id)

    logger.info(
        "dag_submitted",
        dag_id=str(dag.id),
        node_count=len(spec.nodes),
        root_count=len(root_nodes),
    )

    return _build_dag_response(dag, refreshed_jobs)


def _build_dag_response(dag: DagGraph, jobs: list[Job]) -> DagResponse:
    """
    Build a DagResponse from a DagGraph and its associated jobs.

    Args:
        dag: The DagGraph instance (with edges loaded).
        jobs: List of jobs belonging to this DAG.

    Returns:
        Complete DagResponse with nodes and edges.
    """
    nodes = [
        DagNodeResponse(
            node_id=job.dag_node_id or str(job.id),
            job_id=job.id,
            job_type=job.type,
            job_status=job.status,
        )
        for job in jobs
    ]

    edges = [
        DagEdgeResponse(
            parent_node_id=edge.parent_node_id,
            child_node_id=edge.child_node_id,
        )
        for edge in (dag.edges or [])
    ]

    return DagResponse(
        id=dag.id,
        name=dag.name,
        description=dag.description,
        status=dag.status,
        failure_policy=dag.failure_policy,
        created_at=dag.created_at,
        nodes=nodes,
        edges=edges,
        metadata=dag.metadata_,
    )
