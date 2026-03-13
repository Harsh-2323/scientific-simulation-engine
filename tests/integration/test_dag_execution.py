"""
Integration tests for DAG execution and failure policies.
"""

import pytest

from src.core.state_machine import transition_job
from src.dag.executor import on_node_completed, on_node_failed
from src.models.enums import JobStatus
from src.models.dag import DagGraph
from sqlalchemy import select


@pytest.mark.asyncio
class TestDagExecution:
    """Test DAG orchestration with real Postgres + Redis."""

    async def test_diamond_dag_execution(self, db_session, redis_client, dag_factory):
        """Diamond DAG: A -> B, A -> C, B -> D, C -> D. All complete in order."""
        dag, jobs = await dag_factory(
            name="diamond",
            nodes=[
                {"node_id": "a", "job_type": "sim", "params": {"steps": 5}},
                {"node_id": "b", "job_type": "sim", "params": {"steps": 5}, "depends_on": ["a"]},
                {"node_id": "c", "job_type": "sim", "params": {"steps": 5}, "depends_on": ["a"]},
                {"node_id": "d", "job_type": "sim", "params": {"steps": 5}, "depends_on": ["b", "c"]},
            ],
        )

        # Enqueue and complete root node A
        await transition_job(db_session, jobs["a"].id, "queued", expected_status="pending", triggered_by="t")
        await transition_job(db_session, jobs["a"].id, "running", expected_status="queued", triggered_by="t")
        await transition_job(db_session, jobs["a"].id, "completed", expected_status="running", triggered_by="t")
        await db_session.flush()
        await db_session.refresh(jobs["a"])

        # Trigger DAG executor — should enqueue B and C
        await on_node_completed(db_session, redis_client, jobs["a"])
        await db_session.flush()

        await db_session.refresh(jobs["b"])
        await db_session.refresh(jobs["c"])
        assert jobs["b"].status == "queued"
        assert jobs["c"].status == "queued"

        # Complete B
        await transition_job(db_session, jobs["b"].id, "running", expected_status="queued", triggered_by="t")
        await transition_job(db_session, jobs["b"].id, "completed", expected_status="running", triggered_by="t")
        await db_session.flush()
        await db_session.refresh(jobs["b"])
        await on_node_completed(db_session, redis_client, jobs["b"])
        await db_session.flush()

        # D should still be pending — C hasn't completed yet
        await db_session.refresh(jobs["d"])
        assert jobs["d"].status == "pending"

        # Complete C
        await transition_job(db_session, jobs["c"].id, "running", expected_status="queued", triggered_by="t")
        await transition_job(db_session, jobs["c"].id, "completed", expected_status="running", triggered_by="t")
        await db_session.flush()
        await db_session.refresh(jobs["c"])
        await on_node_completed(db_session, redis_client, jobs["c"])
        await db_session.flush()

        # NOW D should be enqueued
        await db_session.refresh(jobs["d"])
        assert jobs["d"].status == "queued"

        # Complete D
        await transition_job(db_session, jobs["d"].id, "running", expected_status="queued", triggered_by="t")
        await transition_job(db_session, jobs["d"].id, "completed", expected_status="running", triggered_by="t")
        await db_session.flush()
        await db_session.refresh(jobs["d"])
        await on_node_completed(db_session, redis_client, jobs["d"])
        await db_session.flush()

        # DAG should be completed
        dag_row = (await db_session.execute(
            select(DagGraph).where(DagGraph.id == dag.id)
        )).scalar_one()
        assert dag_row.status == "completed"

    async def test_linear_dag_progression(self, db_session, redis_client, dag_factory):
        """Linear DAG: A -> B. A completes, then B starts."""
        dag, jobs = await dag_factory(
            name="linear",
            nodes=[
                {"node_id": "a", "job_type": "sim", "params": {"steps": 5}},
                {"node_id": "b", "job_type": "sim", "params": {"steps": 5}, "depends_on": ["a"]},
            ],
        )

        # Complete A
        await transition_job(db_session, jobs["a"].id, "queued", expected_status="pending", triggered_by="t")
        await transition_job(db_session, jobs["a"].id, "running", expected_status="queued", triggered_by="t")
        await transition_job(db_session, jobs["a"].id, "completed", expected_status="running", triggered_by="t")
        await db_session.flush()
        await db_session.refresh(jobs["a"])

        await on_node_completed(db_session, redis_client, jobs["a"])
        await db_session.flush()

        await db_session.refresh(jobs["b"])
        assert jobs["b"].status == "queued"


@pytest.mark.asyncio
class TestDagFailurePolicies:
    """Test failure policy behavior in DAGs."""

    async def test_skip_downstream_cancels_descendants(self, db_session, redis_client, dag_factory):
        """skip_downstream: failed node's descendants are cancelled, others continue."""
        dag, jobs = await dag_factory(
            name="skip_test",
            failure_policy="skip_downstream",
            nodes=[
                {"node_id": "a", "job_type": "sim", "params": {}},
                {"node_id": "b", "job_type": "sim", "params": {}, "depends_on": ["a"]},
                {"node_id": "c", "job_type": "sim", "params": {}},  # Independent branch
            ],
        )

        # Fail node A to dead_letter
        await transition_job(db_session, jobs["a"].id, "queued", expected_status="pending", triggered_by="t")
        await transition_job(db_session, jobs["a"].id, "running", expected_status="queued", triggered_by="t")
        await transition_job(db_session, jobs["a"].id, "failed", expected_status="running", triggered_by="t")
        await transition_job(db_session, jobs["a"].id, "dead_letter", expected_status="failed", triggered_by="t")
        await db_session.flush()
        await db_session.refresh(jobs["a"])

        await on_node_failed(db_session, redis_client, jobs["a"])
        await db_session.flush()

        # B (downstream of A) should be cancelled
        await db_session.refresh(jobs["b"])
        assert jobs["b"].status == "cancelled"

        # C (independent) should still be pending
        await db_session.refresh(jobs["c"])
        assert jobs["c"].status == "pending"

    async def test_fail_fast_cancels_all(self, db_session, redis_client, dag_factory):
        """fail_fast: any node failure cancels all non-terminal nodes."""
        dag, jobs = await dag_factory(
            name="failfast_test",
            failure_policy="fail_fast",
            nodes=[
                {"node_id": "a", "job_type": "sim", "params": {}},
                {"node_id": "b", "job_type": "sim", "params": {}},
                {"node_id": "c", "job_type": "sim", "params": {}, "depends_on": ["a", "b"]},
            ],
        )

        # Complete B (so it's in terminal state)
        await transition_job(db_session, jobs["b"].id, "queued", expected_status="pending", triggered_by="t")
        await transition_job(db_session, jobs["b"].id, "running", expected_status="queued", triggered_by="t")
        await transition_job(db_session, jobs["b"].id, "completed", expected_status="running", triggered_by="t")

        # Fail A to dead_letter
        await transition_job(db_session, jobs["a"].id, "queued", expected_status="pending", triggered_by="t")
        await transition_job(db_session, jobs["a"].id, "running", expected_status="queued", triggered_by="t")
        await transition_job(db_session, jobs["a"].id, "failed", expected_status="running", triggered_by="t")
        await transition_job(db_session, jobs["a"].id, "dead_letter", expected_status="failed", triggered_by="t")
        await db_session.flush()
        await db_session.refresh(jobs["a"])

        await on_node_failed(db_session, redis_client, jobs["a"])
        await db_session.flush()

        # C should be cancelled (non-terminal)
        await db_session.refresh(jobs["c"])
        assert jobs["c"].status == "cancelled"

        # B should still be completed (already terminal)
        await db_session.refresh(jobs["b"])
        assert jobs["b"].status == "completed"

        # DAG should be failed
        dag_row = (await db_session.execute(
            select(DagGraph).where(DagGraph.id == dag.id)
        )).scalar_one()
        assert dag_row.status == "failed"
