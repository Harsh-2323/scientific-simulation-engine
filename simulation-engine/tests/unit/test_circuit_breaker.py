"""
Unit tests for the per job-type circuit breaker.

Tests state transitions: CLOSED -> OPEN -> HALF_OPEN -> CLOSED,
threshold enforcement, cooldown, and type independence.
"""

import pytest
import pytest_asyncio

from src.core.circuit_breaker import CircuitBreaker


@pytest.mark.asyncio
class TestCircuitBreaker:
    """Test circuit breaker state machine with real Redis."""

    async def test_starts_closed(self, redis_client):
        """A fresh circuit breaker should be in the CLOSED state."""
        cb = CircuitBreaker(redis_client)
        state = await cb.get_state("new_type")
        assert state == "closed"
        assert await cb.is_allowed("new_type") is True

    async def test_stays_closed_below_threshold(self, redis_client):
        """Failures below the threshold keep the breaker closed."""
        cb = CircuitBreaker(redis_client)
        for _ in range(4):  # threshold is 5
            await cb.record_failure("test_type")

        assert await cb.get_state("test_type") == "closed"
        assert await cb.is_allowed("test_type") is True

    async def test_opens_on_threshold(self, redis_client):
        """Reaching the failure threshold trips the breaker to OPEN."""
        cb = CircuitBreaker(redis_client)
        for _ in range(5):
            await cb.record_failure("test_type")

        assert await cb.get_state("test_type") == "open"
        assert await cb.is_allowed("test_type") is False

    async def test_half_open_allows_probe(self, redis_client):
        """After cooldown, the breaker transitions to HALF_OPEN and allows one probe."""
        cb = CircuitBreaker(redis_client)

        # Trip the breaker
        for _ in range(5):
            await cb.record_failure("test_type")
        assert await cb.get_state("test_type") == "open"

        # Manually set to half_open (simulating cooldown expiry)
        await redis_client.set("circuit:test_type:state", "half_open")

        assert await cb.get_state("test_type") == "half_open"
        # First caller gets through as the probe
        assert await cb.is_allowed("test_type") is True

    async def test_success_closes_half_open(self, redis_client):
        """A successful probe in HALF_OPEN closes the breaker."""
        cb = CircuitBreaker(redis_client)

        # Set to half_open directly
        await redis_client.set("circuit:test_type:state", "half_open")
        assert await cb.get_state("test_type") == "half_open"

        await cb.record_success("test_type")
        assert await cb.get_state("test_type") == "closed"

    async def test_failure_reopens_from_half_open(self, redis_client):
        """A failed probe in HALF_OPEN reopens the breaker."""
        cb = CircuitBreaker(redis_client)

        # Set to half_open with enough failures already recorded
        for _ in range(5):
            await cb.record_failure("test_type")
        await redis_client.set("circuit:test_type:state", "half_open")

        # Another failure while half_open
        await cb.record_failure("test_type")
        assert await cb.get_state("test_type") == "open"

    async def test_independent_per_type(self, redis_client):
        """Different job types have independent circuit breakers."""
        cb = CircuitBreaker(redis_client)

        # Trip breaker for type_a
        for _ in range(5):
            await cb.record_failure("type_a")

        assert await cb.get_state("type_a") == "open"
        assert await cb.is_allowed("type_a") is False

        # type_b should be unaffected
        assert await cb.get_state("type_b") == "closed"
        assert await cb.is_allowed("type_b") is True

    async def test_reset_clears_state(self, redis_client):
        """Manual reset brings the breaker back to closed."""
        cb = CircuitBreaker(redis_client)

        for _ in range(5):
            await cb.record_failure("test_type")
        assert await cb.get_state("test_type") == "open"

        await cb.reset("test_type")
        assert await cb.get_state("test_type") == "closed"
        assert await cb.is_allowed("test_type") is True
