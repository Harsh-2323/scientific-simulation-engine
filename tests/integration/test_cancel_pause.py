"""
Integration tests for cancel and pause/resume flows.
"""

import pytest

from src.core.state_machine import transition_job
from src.models.enums import JobStatus


@pytest.mark.asyncio
class TestCancel:
    """Test job cancellation from various states."""

    async def test_cancel_queued_job(self, db_session, job_factory):
        """A queued job can be directly cancelled."""
        job = await job_factory()
        await transition_job(db_session, job.id, "queued", expected_status="pending", triggered_by="t")

        result = await transition_job(
            db_session, job.id, "cancelled",
            expected_status="queued", triggered_by="api",
        )
        assert result is not None
        assert result.status == "cancelled"
        assert result.completed_at is not None

    async def test_cancel_running_job_goes_to_cancelling(self, db_session, job_factory):
        """A running job transitions to cancelling first (waiting for worker ack)."""
        job = await job_factory()
        await transition_job(db_session, job.id, "queued", expected_status="pending", triggered_by="t")
        await transition_job(db_session, job.id, "running", expected_status="queued", triggered_by="t")

        result = await transition_job(
            db_session, job.id, "cancelling",
            expected_status="running", triggered_by="api",
        )
        assert result.status == "cancelling"

        # Worker acknowledges
        result = await transition_job(
            db_session, job.id, "cancelled",
            expected_status="cancelling", triggered_by="worker",
        )
        assert result.status == "cancelled"

    async def test_cancel_paused_job(self, db_session, job_factory):
        """A paused job can be directly cancelled."""
        job = await job_factory()
        await transition_job(db_session, job.id, "queued", expected_status="pending", triggered_by="t")
        await transition_job(db_session, job.id, "running", expected_status="queued", triggered_by="t")
        await transition_job(db_session, job.id, "pausing", expected_status="running", triggered_by="t")
        await transition_job(db_session, job.id, "paused", expected_status="pausing", triggered_by="t")

        result = await transition_job(
            db_session, job.id, "cancelled",
            expected_status="paused", triggered_by="api",
        )
        assert result.status == "cancelled"

    async def test_cancel_pending_job(self, db_session, job_factory):
        """A pending job (not yet queued) can be cancelled."""
        job = await job_factory()

        result = await transition_job(
            db_session, job.id, "cancelled",
            expected_status="pending", triggered_by="api",
        )
        assert result.status == "cancelled"


@pytest.mark.asyncio
class TestPauseResume:
    """Test pause and resume flows."""

    async def test_pause_running_job(self, db_session, job_factory):
        """A running job transitions through pausing to paused."""
        job = await job_factory()
        await transition_job(db_session, job.id, "queued", expected_status="pending", triggered_by="t")
        await transition_job(db_session, job.id, "running", expected_status="queued", triggered_by="t")

        # API signals pause
        result = await transition_job(
            db_session, job.id, "pausing",
            expected_status="running", triggered_by="api",
        )
        assert result.status == "pausing"

        # Worker checkpoints and pauses
        result = await transition_job(
            db_session, job.id, "paused",
            expected_status="pausing", triggered_by="worker",
        )
        assert result.status == "paused"
        assert result.worker_id is None  # Worker released
        assert result.lease_expires_at is None

    async def test_resume_paused_job(self, db_session, job_factory):
        """A paused job can be resumed back to running."""
        job = await job_factory()
        await transition_job(db_session, job.id, "queued", expected_status="pending", triggered_by="t")
        await transition_job(db_session, job.id, "running", expected_status="queued", triggered_by="t")
        await transition_job(db_session, job.id, "pausing", expected_status="running", triggered_by="t")
        await transition_job(db_session, job.id, "paused", expected_status="pausing", triggered_by="t")

        # Resume
        result = await transition_job(
            db_session, job.id, "resuming",
            expected_status="paused", triggered_by="api",
        )
        assert result.status == "resuming"

        # Worker picks it up
        result = await transition_job(
            db_session, job.id, "running",
            expected_status="resuming", triggered_by="worker",
        )
        assert result.status == "running"
