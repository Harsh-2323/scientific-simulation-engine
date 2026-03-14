"""
Shared test fixtures for all test categories.

Uses the running Docker Compose services (Postgres 16 and Redis 7).
Each test gets its own fresh session and Redis client.
"""

import uuid

import pytest
import pytest_asyncio
import redis.asyncio as redis
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool

from src.config import settings
from src.models.job import Job
from src.models.dag import DagGraph, DagEdge


@pytest_asyncio.fixture
async def db_session():
    """
    Fresh async database session per test.

    Creates a new engine connection for each test to avoid
    asyncpg connection state leaking between tests.
    """
    # NullPool disables connection pooling — each test gets a truly fresh
    # connection, preventing asyncpg state from leaking between tests.
    engine = create_async_engine(
        settings.database_url,
        echo=False,
        poolclass=NullPool,
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
    await engine.dispose()


@pytest_asyncio.fixture
async def redis_client():
    """Provide a clean Redis client per test. Flushes after use."""
    client = redis.from_url(settings.redis_url, decode_responses=True)
    yield client
    await client.flushdb()
    await client.aclose()


@pytest_asyncio.fixture
async def api_client(db_session, redis_client):
    """HTTP test client with DB and Redis overrides."""
    from src.main import app
    from src.api.dependencies import get_redis, get_db

    # Override both dependencies so API routes use the test-scoped
    # session and Redis client instead of the module-level engine
    # (which gets stale across test event loops).
    async def override_get_db():
        yield db_session

    app.dependency_overrides[get_redis] = lambda: redis_client
    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client

    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def job_factory(db_session):
    """Factory to create and persist test jobs."""
    async def _create(
        *,
        job_type: str = "simulation",
        status: str = "pending",
        priority: str = "normal",
        params: dict | None = None,
        max_retries: int = 3,
        dag_id: uuid.UUID | None = None,
        dag_node_id: str | None = None,
    ) -> Job:
        job = Job(
            type=job_type,
            status=status,
            priority=priority,
            params=params or {"steps": 10},
            max_retries=max_retries,
            dag_id=dag_id,
            dag_node_id=dag_node_id,
        )
        db_session.add(job)
        await db_session.commit()
        await db_session.refresh(job)
        return job

    return _create


@pytest_asyncio.fixture
async def dag_factory(db_session, redis_client):
    """Factory to create test DAGs with jobs and edges."""
    async def _create(
        *,
        name: str = "test_dag",
        failure_policy: str = "skip_downstream",
        nodes: list[dict] | None = None,
    ) -> tuple[DagGraph, dict[str, Job]]:
        if nodes is None:
            nodes = [
                {"node_id": "a", "job_type": "sim", "params": {"steps": 5}},
                {"node_id": "b", "job_type": "sim", "params": {"steps": 5}, "depends_on": ["a"]},
            ]

        dag = DagGraph(name=name, failure_policy=failure_policy)
        db_session.add(dag)
        await db_session.commit()
        await db_session.refresh(dag)

        job_map: dict[str, Job] = {}
        for node in nodes:
            job = Job(
                type=node["job_type"],
                params=node["params"],
                priority=node.get("priority", "normal"),
                dag_id=dag.id,
                dag_node_id=node["node_id"],
            )
            db_session.add(job)
            await db_session.commit()
            await db_session.refresh(job)
            job_map[node["node_id"]] = job

        for node in nodes:
            for dep in node.get("depends_on", []):
                edge = DagEdge(
                    dag_id=dag.id,
                    parent_node_id=dep,
                    child_node_id=node["node_id"],
                )
                db_session.add(edge)

        await db_session.commit()
        return dag, job_map

    return _create
