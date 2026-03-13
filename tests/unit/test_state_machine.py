"""
Unit tests for the job state machine.

Tests valid/invalid transitions, event logging, optimistic locking,
and terminal state enforcement.
"""

import uuid
import pytest
import pytest_asyncio

from src.core.state_machine import VALID_TRANSITIONS, is_valid_transition, transition_job
from src.core.exceptions import InvalidTransitionError
from src.models.enums import JobStatus
from src.models.job import Job, JobEvent
from sqlalchemy import select


# All 12 states in the system
ALL_STATES = list(VALID_TRANSITIONS.keys())


class TestValidTransitions:
    """Verify the state machine transition table."""

    def test_all_valid_transitions_succeed(self):
        """Every (from, to) pair in VALID_TRANSITIONS should be valid."""
        for from_status, targets in VALID_TRANSITIONS.items():
            for to_status in targets:
                assert is_valid_transition(from_status, to_status), (
                    f"{from_status} -> {to_status} should be valid"
                )

    def test_all_invalid_transitions_rejected(self):
        """Transitions not in VALID_TRANSITIONS should be invalid."""
        # Test a few known-invalid transitions
        invalid_pairs = [
            ("completed", "running"),
            ("cancelled", "queued"),
            ("dead_letter", "pending"),
            ("pending", "completed"),
            ("queued", "completed"),
        ]
        for from_status, to_status in invalid_pairs:
            assert not is_valid_transition(from_status, to_status), (
                f"{from_status} -> {to_status} should be invalid"
            )

    def test_terminal_states_have_no_transitions(self):
        """Completed, cancelled, and dead_letter should have no outgoing transitions."""
        terminal = ["completed", "cancelled", "dead_letter"]
        for state in terminal:
            assert VALID_TRANSITIONS[state] == [], (
                f"Terminal state '{state}' should have no transitions"
            )


@pytest.mark.asyncio
class TestTransitionJob:
    """Test the atomic transition_job function against a real database."""

    async def test_valid_transition_succeeds(self, db_session, job_factory):
        """A valid pending->queued transition should update the job and return it."""
        job = await job_factory(status="pending")

        result = await transition_job(
            db_session, job.id, "queued",
            expected_status="pending", triggered_by="test",
        )

        assert result is not None
        assert result.status == "queued"
        assert result.queued_at is not None

    async def test_transition_writes_event(self, db_session, job_factory):
        """Every transition should create a JobEvent audit record."""
        job = await job_factory(status="pending")

        await transition_job(
            db_session, job.id, "queued",
            expected_status="pending", triggered_by="test",
        )

        events = (await db_session.execute(
            select(JobEvent).where(JobEvent.job_id == job.id)
        )).scalars().all()

        assert len(events) == 1
        event = events[0]
        assert event.old_status == "pending"
        assert event.new_status == "queued"
        assert event.triggered_by == "test"
        assert event.event_type == "status_change"

    async def test_wrong_expected_status_returns_none(self, db_session, job_factory):
        """If the job's current status doesn't match expected, transition is rejected."""
        job = await job_factory(status="pending")

        # Try to transition with wrong expected status
        result = await transition_job(
            db_session, job.id, "running",
            expected_status="queued", triggered_by="test",
        )

        # Should return None (0 rows updated)
        assert result is None

    async def test_invalid_transition_raises(self, db_session, job_factory):
        """Invalid transitions should raise InvalidTransitionError."""
        job = await job_factory(status="completed")

        with pytest.raises(InvalidTransitionError):
            await transition_job(
                db_session, job.id, "running",
                expected_status="completed", triggered_by="test",
            )

    async def test_concurrent_claims_one_wins(self, db_session, job_factory):
        """Two transitions from the same state — only one should succeed."""
        job = await job_factory(status="pending")
        await transition_job(
            db_session, job.id, "queued",
            expected_status="pending", triggered_by="test",
        )

        # First claim
        result1 = await transition_job(
            db_session, job.id, "running",
            expected_status="queued", triggered_by="worker_1",
        )
        assert result1 is not None

        # Second claim — should fail because status is now 'running'
        result2 = await transition_job(
            db_session, job.id, "running",
            expected_status="queued", triggered_by="worker_2",
        )
        assert result2 is None

    async def test_completed_sets_result(self, db_session, job_factory):
        """Completing a job should store the result and set progress to 100%."""
        job = await job_factory(status="pending")
        await transition_job(db_session, job.id, "queued", expected_status="pending", triggered_by="t")
        await transition_job(db_session, job.id, "running", expected_status="queued", triggered_by="t")

        result = await transition_job(
            db_session, job.id, "completed",
            expected_status="running", triggered_by="worker",
            result={"output": 42},
        )

        assert result.result == {"output": 42}
        assert result.progress_percent == 100.0
        assert result.completed_at is not None
