"""
Integration tests for the full job lifecycle.

Tests end-to-end flows from submission through completion,
including idempotency, backpressure, and the API layer.
"""

import pytest
import pytest_asyncio

from src.core.state_machine import transition_job
from src.models.enums import JobStatus
from src.models.job import JobEvent
from sqlalchemy import select


@pytest.mark.asyncio
class TestJobLifecycle:
    """Test jobs through their full lifecycle against real Postgres + Redis."""

    async def test_full_lifecycle(self, db_session, job_factory):
        """Submit -> queued -> running -> completed with events and result."""
        job = await job_factory()

        # pending -> queued
        job = await transition_job(
            db_session, job.id, "queued",
            expected_status="pending", triggered_by="api",
        )
        assert job.status == "queued"
        assert job.queued_at is not None

        # queued -> running
        job = await transition_job(
            db_session, job.id, "running",
            expected_status="queued", triggered_by="worker",
            worker_id="w1",
        )
        assert job.status == "running"
        assert job.started_at is not None
        assert job.worker_id == "w1"

        # running -> completed
        job = await transition_job(
            db_session, job.id, "completed",
            expected_status="running", triggered_by="worker",
            result={"accuracy": 0.99},
        )
        assert job.status == "completed"
        assert job.completed_at is not None
        assert job.result == {"accuracy": 0.99}
        assert job.progress_percent == 100.0

        # Verify 3 events were created
        events = (await db_session.execute(
            select(JobEvent)
            .where(JobEvent.job_id == job.id)
            .order_by(JobEvent.timestamp)
        )).scalars().all()
        assert len(events) == 3
        assert events[0].new_status == "queued"
        assert events[1].new_status == "running"
        assert events[2].new_status == "completed"

    async def test_failure_to_retry_lifecycle(self, db_session, job_factory):
        """failed -> retry_scheduled -> queued lifecycle."""
        job = await job_factory()

        # Move through to running
        await transition_job(db_session, job.id, "queued", expected_status="pending", triggered_by="t")
        await transition_job(db_session, job.id, "running", expected_status="queued", triggered_by="t")

        # Fail the job
        job = await transition_job(
            db_session, job.id, "failed",
            expected_status="running", triggered_by="worker",
            error_message="timeout", error_type="transient",
        )
        assert job.status == "failed"
        assert job.error_message == "timeout"
        assert job.error_type == "transient"

        # Schedule retry
        job = await transition_job(
            db_session, job.id, "retry_scheduled",
            expected_status="failed", triggered_by="worker",
        )
        assert job.status == "retry_scheduled"

        # Re-queue
        job = await transition_job(
            db_session, job.id, "queued",
            expected_status="retry_scheduled", triggered_by="scheduler",
        )
        assert job.status == "queued"

    async def test_dead_letter_lifecycle(self, db_session, job_factory):
        """Permanent error goes directly to dead_letter."""
        job = await job_factory()

        await transition_job(db_session, job.id, "queued", expected_status="pending", triggered_by="t")
        await transition_job(db_session, job.id, "running", expected_status="queued", triggered_by="t")
        await transition_job(
            db_session, job.id, "failed",
            expected_status="running", triggered_by="worker",
            error_type="permanent",
        )

        job = await transition_job(
            db_session, job.id, "dead_letter",
            expected_status="failed", triggered_by="worker",
        )
        assert job.status == "dead_letter"
        assert job.completed_at is not None


@pytest.mark.asyncio
class TestApiEndpoints:
    """Test the FastAPI endpoints via the test client."""

    async def test_submit_job(self, api_client):
        """POST /api/v1/jobs creates a job and returns 201."""
        response = await api_client.post("/api/v1/jobs", json={
            "type": "simulation",
            "priority": "normal",
            "params": {"steps": 20},
        })
        assert response.status_code == 201
        data = response.json()
        assert data["status"] == "queued"
        assert data["type"] == "simulation"
        assert data["params"] == {"steps": 20}

    async def test_get_job(self, api_client):
        """GET /api/v1/jobs/{id} returns the job details."""
        # First create a job
        submit = await api_client.post("/api/v1/jobs", json={
            "type": "simulation",
            "params": {"steps": 10},
        })
        job_id = submit.json()["id"]

        # Then fetch it
        response = await api_client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200
        assert response.json()["id"] == job_id

    async def test_list_jobs(self, api_client):
        """GET /api/v1/jobs returns paginated job list."""
        # Create a few jobs
        for _ in range(3):
            await api_client.post("/api/v1/jobs", json={
                "type": "simulation", "params": {},
            })

        response = await api_client.get("/api/v1/jobs?limit=10")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] >= 3
        assert len(data["items"]) >= 3

    async def test_health_endpoint(self, api_client):
        """GET /api/v1/health returns healthy status."""
        response = await api_client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    async def test_metrics_endpoint(self, api_client):
        """GET /api/v1/metrics returns system metrics."""
        response = await api_client.get("/api/v1/metrics")
        assert response.status_code == 200
        data = response.json()
        assert "queue_depth" in data
        assert "total_queue_depth" in data
        assert "running_jobs" in data

    async def test_idempotency_same_params(self, api_client):
        """Same idempotency key + same params returns the original job."""
        payload = {
            "type": "simulation",
            "params": {"steps": 10},
            "idempotency_key": "test-idem-001",
        }
        r1 = await api_client.post("/api/v1/jobs", json=payload)
        r2 = await api_client.post("/api/v1/jobs", json=payload)

        assert r1.status_code == 201
        assert r2.status_code == 201  # Returns existing, not 409
        assert r1.json()["id"] == r2.json()["id"]

    async def test_idempotency_different_params_409(self, api_client):
        """Same idempotency key + different params returns 409."""
        await api_client.post("/api/v1/jobs", json={
            "type": "simulation",
            "params": {"steps": 10},
            "idempotency_key": "test-conflict",
        })

        response = await api_client.post("/api/v1/jobs", json={
            "type": "simulation",
            "params": {"steps": 999},  # Different params
            "idempotency_key": "test-conflict",
        })
        assert response.status_code == 409
