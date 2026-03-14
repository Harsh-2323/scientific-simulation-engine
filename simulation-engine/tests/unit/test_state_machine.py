"""
Unit tests for the job state machine transition table.

Tests valid/invalid transitions and terminal state enforcement.
Database-dependent tests (transition_job with real SQL) live in
tests/integration/test_job_lifecycle.py.
"""

from src.core.state_machine import VALID_TRANSITIONS, is_valid_transition


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

    def test_every_non_terminal_state_has_outbound(self):
        """Non-terminal states must have at least one outgoing transition."""
        terminal = {"completed", "cancelled", "dead_letter"}
        for state, targets in VALID_TRANSITIONS.items():
            if state not in terminal:
                assert len(targets) > 0, (
                    f"Non-terminal state '{state}' must have outbound transitions"
                )

    def test_pending_to_queued(self):
        """Core transition: pending -> queued."""
        assert is_valid_transition("pending", "queued")

    def test_running_to_completed(self):
        """Core transition: running -> completed."""
        assert is_valid_transition("running", "completed")

    def test_failed_to_retry_scheduled(self):
        """Retry path: failed -> retry_scheduled."""
        assert is_valid_transition("failed", "retry_scheduled")

    def test_failed_to_dead_letter(self):
        """Terminal failure path: failed -> dead_letter."""
        assert is_valid_transition("failed", "dead_letter")

    def test_running_to_pausing(self):
        """Pause control: running -> pausing."""
        assert is_valid_transition("running", "pausing")

    def test_paused_to_resuming(self):
        """Resume control: paused -> resuming."""
        assert is_valid_transition("paused", "resuming")
