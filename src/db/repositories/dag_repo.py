"""
Repository functions for DAG CRUD operations.

Handles creation of DAG graphs with their associated jobs and edges,
and querying DAGs with their full node/edge structure.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.models.dag import DagGraph, DagEdge
from src.models.job import Job

import structlog

logger = structlog.get_logger(__name__)


async def create_dag(
    session: AsyncSession,
    *,
    name: str,
    description: str | None = None,
    failure_policy: str = "skip_downstream",
    metadata: dict | None = None,
    natural_language_input: str | None = None,
    raw_llm_output: dict | None = None,
) -> DagGraph:
    """
    Create a new DAG graph row.

    Args:
        session: Active async database session.
        name: Human-readable DAG name.
        description: Optional description.
        failure_policy: How to handle node failures.
        metadata: Arbitrary metadata dict.
        natural_language_input: LLM input text (if created via decompose).
        raw_llm_output: Raw LLM response JSON (if created via decompose).

    Returns:
        The created DagGraph instance.
    """
    dag = DagGraph(
        name=name,
        description=description,
        failure_policy=failure_policy,
        metadata_=metadata or {},
        natural_language_input=natural_language_input,
        raw_llm_output=raw_llm_output,
    )
    session.add(dag)
    await session.flush()

    logger.info("dag_created", dag_id=str(dag.id), name=name)
    return dag


async def create_dag_edge(
    session: AsyncSession,
    *,
    dag_id: uuid.UUID,
    parent_node_id: str,
    child_node_id: str,
) -> DagEdge:
    """
    Create a dependency edge between two nodes in a DAG.

    Args:
        session: Active async database session.
        dag_id: UUID of the parent DAG.
        parent_node_id: The node that must complete first.
        child_node_id: The node that depends on parent_node_id.

    Returns:
        The created DagEdge instance.
    """
    edge = DagEdge(
        dag_id=dag_id,
        parent_node_id=parent_node_id,
        child_node_id=child_node_id,
    )
    session.add(edge)
    return edge


async def get_dag_by_id(
    session: AsyncSession,
    dag_id: uuid.UUID,
) -> DagGraph | None:
    """
    Fetch a DAG by its ID, including its edges.

    Args:
        session: Active async database session.
        dag_id: UUID of the DAG to fetch.

    Returns:
        The DagGraph instance with edges loaded, or None if not found.
    """
    result = await session.execute(
        select(DagGraph)
        .options(selectinload(DagGraph.edges))
        .where(DagGraph.id == dag_id)
    )
    return result.scalar_one_or_none()


async def get_dag_jobs(
    session: AsyncSession,
    dag_id: uuid.UUID,
) -> list[Job]:
    """
    Fetch all jobs belonging to a DAG.

    Args:
        session: Active async database session.
        dag_id: UUID of the DAG.

    Returns:
        List of Job instances linked to this DAG.
    """
    result = await session.execute(
        select(Job)
        .where(Job.dag_id == dag_id)
        .order_by(Job.created_at)
    )
    return list(result.scalars().all())


async def list_dags(
    session: AsyncSession,
    *,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[DagGraph], int]:
    """
    List DAGs with pagination.

    Args:
        session: Active async database session.
        limit: Maximum number of DAGs to return.
        offset: Number of DAGs to skip.

    Returns:
        Tuple of (list of DagGraph instances, total count).
    """
    from sqlalchemy import func

    total_result = await session.execute(
        select(func.count(DagGraph.id))
    )
    total = total_result.scalar() or 0

    result = await session.execute(
        select(DagGraph)
        .options(selectinload(DagGraph.edges))
        .order_by(DagGraph.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    dags = list(result.scalars().all())

    return dags, total
