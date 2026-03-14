"""
Unit tests for the priority aging scheduler.

Tests effective priority calculation with various aging scenarios.
"""

from datetime import datetime, timezone, timedelta

import pytest

from src.core.scheduler import calculate_effective_priority


class TestEffectivePriority:
    """Verify priority aging calculations."""

    def test_no_aging_below_threshold(self):
        """A job queued less than the threshold keeps its base priority."""
        recent = datetime.now(timezone.utc) - timedelta(seconds=10)
        assert calculate_effective_priority("low", recent) == "low"

    def test_no_aging_for_none_queued_at(self):
        """If queued_at is None, return the base priority unchanged."""
        assert calculate_effective_priority("low", None) == "low"

    def test_one_bump_at_threshold(self):
        """A low job queued for exactly one threshold period bumps to normal."""
        # 300s (default threshold) in queue
        old = datetime.now(timezone.utc) - timedelta(seconds=300)
        result = calculate_effective_priority("low", old)
        assert result == "normal"

    def test_two_bumps(self):
        """A low job queued for 2x threshold bumps up 2 levels to high."""
        old = datetime.now(timezone.utc) - timedelta(seconds=600)
        result = calculate_effective_priority("low", old)
        assert result == "high"

    def test_caps_at_critical(self):
        """No matter how long the wait, priority never goes below 0 (critical)."""
        ancient = datetime.now(timezone.utc) - timedelta(seconds=10000)
        result = calculate_effective_priority("low", ancient)
        assert result == "critical"

    def test_critical_stays_critical(self):
        """Critical priority cannot be bumped further."""
        old = datetime.now(timezone.utc) - timedelta(seconds=600)
        assert calculate_effective_priority("critical", old) == "critical"

    def test_normal_bumps_to_high(self):
        """Normal (level 2) bumps to high (level 1) after one threshold."""
        old = datetime.now(timezone.utc) - timedelta(seconds=300)
        assert calculate_effective_priority("normal", old) == "high"

    def test_handles_naive_datetime(self):
        """Timezone-naive datetimes should be handled gracefully."""
        old = datetime.utcnow() - timedelta(seconds=600)
        result = calculate_effective_priority("low", old)
        # Should not raise, and should return a valid priority
        assert result in ("critical", "high", "normal", "low")
