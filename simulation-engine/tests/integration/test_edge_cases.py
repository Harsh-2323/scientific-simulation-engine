"""
Edge case and hardening tests.

Covers: crash recovery, checkpoint resume, concurrent workers,
priority ordering, backpressure, circuit breaker end-to-end,
LLM dry run, and DAG dependency ordering.
"""

import asyncio
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from src.config import settings
from src.core.circuit_breaker import CircuitBreaker
from src.core.state_machine import transition_job
from src.models.enums import JobStatus
from src.queue.publisher import enqueue_job
from src.worker.checkpoint import write_checkpoint, load_checkpoint


# ── Crash Recovery ────────────────────────────────────────────────────────────

class TestCrashRecovery:

    async def test_expired_lease_job_can_be_reclaimed(self, db_session, job_factory):
        """
        A running job with an expired lease should be transitionable to failed
        — simulating what the reaper does when it reclaims a dead worker's job.
        """
        job = await job_factory(status="running", params={"steps": 10})
        job.worker_id = "dead-worker:99999:abc"
        job.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=60)
        job.retry_count = 0
        job.max_retries = 3
        await db_session.commit()

        # Reaper would call transition_job to move it to failed
        failed = await transition_job(
            db_session, job.id, JobStatus.FAILED,
            expected_status=JobStatus.RUNNING,
            error_message="Worker lease expired — reclaimed by reaper",
            error_type="transient",
            triggered_by="reaper",
        )

        assert failed is not None
        assert failed.status == "failed"
        assert failed.error_message == "Worker lease expired — reclaimed by reaper"

    async def test_stale_pausing_job_can_be_reclaimed(self, db_session, job_factory):
        """
        A job stuck in 'pausing' with an expired lease can be transitioned to failed.
        """
        job = await job_factory(status="pausing", params={"steps": 10})
        job.worker_id = "dead-worker:99999:xyz"
        job.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=60)
        await db_session.commit()

        failed = await transition_job(
            db_session, job.id, JobStatus.FAILED,
            expected_status=JobStatus.PAUSING,
            error_message="Worker lease expired during pause",
            error_type="transient",
            triggered_by="reaper",
        )

        assert failed is not None
        assert failed.status == "failed"

    async def test_failed_job_with_retries_goes_to_retry_scheduled(self, db_session, job_factory):
        """
        A failed job with remaining retries should transition to retry_scheduled.
        """
        job = await job_factory(status="failed", params={"steps": 10})
        job.retry_count = 1
        job.max_retries = 3
        await db_session.commit()

        retrying = await transition_job(
            db_session, job.id, JobStatus.RETRY_SCHEDULED,
            expected_status=JobStatus.FAILED,
            triggered_by="worker",
        )

        assert retrying is not None
        assert retrying.status == "retry_scheduled"

    async def test_failed_job_exhausted_retries_goes_to_dead_letter(self, db_session, job_factory):
        """
        A failed job with no retries remaining goes to dead_letter.
        """
        job = await job_factory(status="failed", params={"steps": 10})
        job.retry_count = 3
        job.max_retries = 3
        await db_session.commit()

        dead = await transition_job(
            db_session, job.id, JobStatus.DEAD_LETTER,
            expected_status=JobStatus.FAILED,
            triggered_by="worker",
        )

        assert dead is not None
        assert dead.status == "dead_letter"


# ── Checkpoint Resume ─────────────────────────────────────────────────────────

class TestCheckpointResume:

    async def _create_attempt(self, db_session, job_id: uuid.UUID) -> uuid.UUID:
        """Helper: create a real job_attempts row and return its ID."""
        from src.db.repositories import attempt_repo
        attempt = await attempt_repo.create_attempt(
            db_session,
            job_id=job_id,
            worker_id="test-worker:0:abc",
            attempt_number=1,
        )
        await db_session.commit()
        return attempt.id

    async def test_resume_continues_from_checkpoint_step(self, db_session, job_factory):
        """
        When a job is resumed, the worker starts from checkpoint.step_number + 1.
        """
        job = await job_factory(status="paused", params={"steps": 50})
        attempt_id = await self._create_attempt(db_session, job.id)

        checkpoint_data = {
            "accumulated_value": 12.5,
            "last_completed_step": 25,
            "step_results": [{"step": i, "value": i * 0.5} for i in range(25)],
        }
        await write_checkpoint(db_session, job.id, attempt_id, 25, checkpoint_data)
        await db_session.commit()

        checkpoint = await load_checkpoint(db_session, job.id)

        assert checkpoint is not None
        assert checkpoint.step_number == 25
        assert checkpoint.is_valid is True
        assert checkpoint.data["last_completed_step"] == 25

        # Worker should resume from step 26
        resume_from = checkpoint.step_number + 1
        assert resume_from == 26

    async def test_checkpoint_atomic_write_invalidates_previous(self, db_session, job_factory):
        """
        Writing a new checkpoint invalidates all previous ones.
        Only one valid checkpoint exists per job at any time.
        """
        job = await job_factory(status="running", params={"steps": 100})
        attempt_id = await self._create_attempt(db_session, job.id)

        await write_checkpoint(db_session, job.id, attempt_id, 10, {"last_completed_step": 10})
        await db_session.commit()

        await write_checkpoint(db_session, job.id, attempt_id, 20, {"last_completed_step": 20})
        await db_session.commit()

        # Latest checkpoint should be at step 20
        checkpoint = await load_checkpoint(db_session, job.id)
        assert checkpoint is not None
        assert checkpoint.step_number == 20

        # Only one valid checkpoint should exist
        from sqlalchemy import select
        from src.models.checkpoint import Checkpoint
        result = await db_session.execute(
            select(Checkpoint).where(
                Checkpoint.job_id == job.id,
                Checkpoint.is_valid == True,  # noqa: E712
            )
        )
        assert len(result.scalars().all()) == 1

    async def test_checkpoint_size_guard(self, db_session, job_factory):
        """
        Writing a checkpoint exceeding max_checkpoint_bytes raises CheckpointError.
        """
        from src.core.exceptions import CheckpointError

        job = await job_factory(status="running", params={"steps": 10})
        attempt_id = uuid.uuid4()
        oversized_data = {"big_field": "x" * (11 * 1024 * 1024)}

        with pytest.raises(CheckpointError, match="exceeds max size"):
            await write_checkpoint(db_session, job.id, attempt_id, 1, oversized_data)

    async def test_load_checkpoint_returns_none_when_none_exists(self, db_session, job_factory):
        """
        Loading a checkpoint for a job with no checkpoints returns None.
        """
        job = await job_factory(status="running", params={"steps": 10})
        checkpoint = await load_checkpoint(db_session, job.id)
        assert checkpoint is None


# ── Concurrent Workers ────────────────────────────────────────────────────────

class TestConcurrentWorkers:

    async def test_only_one_worker_claims_a_job(self, db_session, job_factory):
        """
        Two workers racing to claim the same queued job —
        only one succeeds via the WHERE status='queued' guard.
        """
        job = await job_factory(status="queued", params={"steps": 5})

        result1 = await transition_job(
            db_session, job.id, "running",
            expected_status="queued",
            triggered_by="worker-1",
            worker_id="worker-1",
        )
        result2 = await transition_job(
            db_session, job.id, "running",
            expected_status="queued",
            triggered_by="worker-2",
            worker_id="worker-2",
        )

        winners = [r for r in [result1, result2] if r is not None]
        losers = [r for r in [result1, result2] if r is None]

        assert len(winners) == 1
        assert len(losers) == 1
        assert winners[0].worker_id in ("worker-1", "worker-2")

    async def test_three_separate_jobs_each_claimed_once(self, db_session, job_factory):
        """
        Three different queued jobs — each claimed by a different worker,
        all succeed (no shared state conflict since they're different jobs).
        """
        jobs = [await job_factory(status="queued", params={"steps": 5}) for _ in range(3)]
        worker_ids = ["worker-a", "worker-b", "worker-c"]

        results = []
        for job, worker_id in zip(jobs, worker_ids):
            result = await transition_job(
                db_session, job.id, "running",
                expected_status="queued",
                triggered_by=worker_id,
                worker_id=worker_id,
            )
            results.append(result)

        assert all(r is not None for r in results)
        claimed_workers = {r.worker_id for r in results}
        assert claimed_workers == {"worker-a", "worker-b", "worker-c"}


# ── Priority Ordering ─────────────────────────────────────────────────────────

class TestPriorityOrdering:

    async def test_critical_job_enqueued_to_critical_stream(self, redis_client, job_factory):
        """
        A critical priority job lands in the critical Redis stream.
        """
        job = await job_factory(status="queued", priority="critical", params={"steps": 5})
        await enqueue_job(redis_client, job.id, "critical")

        messages = await redis_client.xrange("jobs:priority:critical")
        job_ids = [m[1].get("job_id") for m in messages]
        assert str(job.id) in job_ids

    async def test_low_priority_job_goes_to_low_stream(self, redis_client, job_factory):
        """
        A low priority job lands in the low Redis stream.
        """
        job = await job_factory(status="queued", priority="low", params={"steps": 5})
        await enqueue_job(redis_client, job.id, "low")

        messages = await redis_client.xrange("jobs:priority:low")
        job_ids = [m[1].get("job_id") for m in messages]
        assert str(job.id) in job_ids

    async def test_priority_aging_bumps_normal_to_high(self):
        """
        A normal priority job waiting 2x the aging threshold gets bumped to high.
        """
        from src.core.scheduler import calculate_effective_priority

        old_queued_at = datetime.now(timezone.utc) - timedelta(seconds=600)
        aged = calculate_effective_priority("normal", old_queued_at)
        assert aged in ("high", "critical")

    async def test_priority_no_aging_below_threshold(self):
        """
        A recently queued job keeps its original priority.
        """
        from src.core.scheduler import calculate_effective_priority

        recent = datetime.now(timezone.utc) - timedelta(seconds=60)
        priority = calculate_effective_priority("normal", recent)
        assert priority == "normal"


# ── Backpressure ──────────────────────────────────────────────────────────────

class TestBackpressure:

    async def test_submit_returns_429_when_queue_full(self, api_client):
        """
        When queue depth exceeds limit, POST /jobs returns 429 with Retry-After.
        """
        with patch(
            "src.api.routes.jobs._get_total_queue_depth",
            new_callable=AsyncMock,
            return_value=settings.backpressure_queue_depth_limit + 1,
        ):
            response = await api_client.post("/api/v1/jobs", json={
                "type": "simulation",
                "priority": "normal",
                "params": {"steps": 10},
            })

        assert response.status_code == 429
        assert "Retry-After" in response.headers

    async def test_submit_succeeds_when_queue_not_full(self, api_client):
        """
        Normal queue depth — job submission succeeds with 201.
        """
        response = await api_client.post("/api/v1/jobs", json={
            "type": "simulation",
            "priority": "normal",
            "params": {"steps": 5},
        })
        assert response.status_code == 201


# ── Circuit Breaker End-to-End ────────────────────────────────────────────────

class TestCircuitBreakerEndToEnd:

    async def test_circuit_opens_after_threshold_failures(self, redis_client):
        """
        After threshold failures of the same job type, is_allowed returns False.
        """
        cb = CircuitBreaker(redis_client)
        job_type = "cfd_test_open"
        await cb.reset(job_type)

        for _ in range(settings.circuit_breaker_failure_threshold):
            await cb.record_failure(job_type)

        assert await cb.is_allowed(job_type) is False

    async def test_circuit_stays_closed_below_threshold(self, redis_client):
        """
        Fewer than threshold failures — circuit stays closed.
        """
        cb = CircuitBreaker(redis_client)
        job_type = "mesh_test_closed"
        await cb.reset(job_type)

        for _ in range(settings.circuit_breaker_failure_threshold - 1):
            await cb.record_failure(job_type)

        assert await cb.is_allowed(job_type) is True

    async def test_circuit_independent_per_job_type(self, redis_client):
        """
        Circuit open for one job type doesn't affect another.
        """
        cb = CircuitBreaker(redis_client)
        broken = "broken_type_isolated"
        healthy = "healthy_type_isolated"
        await cb.reset(broken)
        await cb.reset(healthy)

        for _ in range(settings.circuit_breaker_failure_threshold):
            await cb.record_failure(broken)

        assert await cb.is_allowed(broken) is False
        assert await cb.is_allowed(healthy) is True

    async def test_circuit_resets_on_success_from_half_open(self, redis_client):
        """
        A success in half_open state closes the circuit.
        record_success only acts on half_open (not open directly).
        """
        cb = CircuitBreaker(redis_client)
        job_type = "recovery_test"
        await cb.reset(job_type)

        # Trip to open
        for _ in range(settings.circuit_breaker_failure_threshold):
            await cb.record_failure(job_type)

        # Simulate cooldown elapsed — manually set to half_open
        await redis_client.set(cb._key(job_type, "state"), "half_open")

        # Success from half_open → should close
        await cb.record_success(job_type)
        state = await cb.get_state(job_type)
        assert state == "closed"


# ── LLM Dry Run ───────────────────────────────────────────────────────────────

class TestLLMDryRun:

    async def test_dry_run_returns_spec_without_creating_dag(self, api_client):
        """
        dry_run=True returns the DAG spec but creates no DB records.
        """
        with patch("src.api.routes.dag.decompose_instruction", new_callable=AsyncMock) as mock:
            from src.schemas.dag import DagSubmitRequest, DagNodeSpec
            mock.return_value = (
                DagSubmitRequest(
                    name="dry_run_dag",
                    failure_policy="skip_downstream",
                    nodes=[DagNodeSpec(node_id="sim_a", job_type="simulation", params={"steps": 5})],
                ),
                "moonshotai/kimi-k2-instruct",
                500,
                [],
            )
            response = await api_client.post("/api/v1/dags/decompose", json={
                "instruction": "Run a simple simulation",
                "dry_run": True,
            })

        assert response.status_code == 201
        data = response.json()
        assert data["dag"] is None
        assert data["dag_spec"]["name"] == "dry_run_dag"

    async def test_dry_run_false_creates_dag(self, api_client):
        """
        dry_run=False creates and returns the DAG.
        """
        with patch("src.api.routes.dag.decompose_instruction", new_callable=AsyncMock) as mock:
            from src.schemas.dag import DagSubmitRequest, DagNodeSpec
            mock.return_value = (
                DagSubmitRequest(
                    name="real_dag",
                    failure_policy="skip_downstream",
                    nodes=[DagNodeSpec(node_id="sim_b", job_type="simulation", params={"steps": 5})],
                ),
                "moonshotai/kimi-k2-instruct",
                400,
                [],
            )
            response = await api_client.post("/api/v1/dags/decompose", json={
                "instruction": "Run a quick simulation",
                "dry_run": False,
            })

        assert response.status_code == 201
        data = response.json()
        assert data["dag"] is not None
        assert data["dag"]["name"] == "real_dag"


# ── DAG Dependency Ordering ───────────────────────────────────────────────────

class TestDagDependencyOrdering:

    async def test_child_only_starts_after_parent_completes(self, db_session, redis_client, dag_factory):
        """
        In A→B, B stays pending until A completes.
        """
        from src.dag.executor import on_node_completed

        dag, job_map = await dag_factory(nodes=[
            {"node_id": "a", "job_type": "simulation", "params": {"steps": 5}},
            {"node_id": "b", "job_type": "simulation", "params": {"steps": 5}, "depends_on": ["a"]},
        ])

        job_a = job_map["a"]
        job_b = job_map["b"]

        assert job_b.status == "pending"

        # Complete A
        await transition_job(db_session, job_a.id, "queued", expected_status="pending", triggered_by="test")
        await transition_job(db_session, job_a.id, "running", expected_status="queued", triggered_by="test")
        completed_a = await transition_job(db_session, job_a.id, "completed", expected_status="running", triggered_by="test")
        await db_session.commit()

        await on_node_completed(db_session, redis_client, completed_a)

        await db_session.refresh(job_b)
        assert job_b.status == "queued"

    async def test_child_with_two_parents_waits_for_both(self, db_session, redis_client, dag_factory):
        """
        C only starts after BOTH A and B complete (diamond pattern).
        """
        from src.dag.executor import on_node_completed

        dag, job_map = await dag_factory(nodes=[
            {"node_id": "a", "job_type": "simulation", "params": {"steps": 5}},
            {"node_id": "b", "job_type": "simulation", "params": {"steps": 5}},
            {"node_id": "c", "job_type": "simulation", "params": {"steps": 5}, "depends_on": ["a", "b"]},
        ])

        job_a, job_b, job_c = job_map["a"], job_map["b"], job_map["c"]

        # Complete A — C should still be pending
        await transition_job(db_session, job_a.id, "queued", expected_status="pending", triggered_by="test")
        await transition_job(db_session, job_a.id, "running", expected_status="queued", triggered_by="test")
        completed_a = await transition_job(db_session, job_a.id, "completed", expected_status="running", triggered_by="test")
        await db_session.commit()
        await on_node_completed(db_session, redis_client, completed_a)

        await db_session.refresh(job_c)
        assert job_c.status == "pending"

        # Complete B — now C should be queued
        await transition_job(db_session, job_b.id, "queued", expected_status="pending", triggered_by="test")
        await transition_job(db_session, job_b.id, "running", expected_status="queued", triggered_by="test")
        completed_b = await transition_job(db_session, job_b.id, "completed", expected_status="running", triggered_by="test")
        await db_session.commit()
        await on_node_completed(db_session, redis_client, completed_b)

        await db_session.refresh(job_c)
        assert job_c.status == "queued"
