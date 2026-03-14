"""
Circuit breaker — per job-type protection against systemic failures.

Prevents a broken job type from burning through retries when the
underlying issue is systemic (e.g., a simulation binary is broken).

Uses Redis for state storage so it works across multiple worker processes.

States:
    CLOSED  — normal operation, jobs are allowed to run.
    OPEN    — too many recent failures, jobs of this type are blocked.
    HALF_OPEN — cooldown elapsed, allowing one probe job to test recovery.
"""

import time

import redis.asyncio as redis

from src.config import settings

import structlog

logger = structlog.get_logger(__name__)


class CircuitBreaker:
    """
    Per job-type circuit breaker backed by Redis.

    Tracks failure timestamps in a sorted set per job type. When failures
    within the configured window exceed the threshold, the breaker opens
    and blocks all jobs of that type until the cooldown expires.

    Redis keys used:
        circuit:{job_type}:failures  — sorted set of failure timestamps
        circuit:{job_type}:state     — current state string
        circuit:{job_type}:opened_at — timestamp when the breaker opened
    """

    def __init__(self, redis_client: redis.Redis) -> None:
        """
        Initialize the circuit breaker.

        Args:
            redis_client: Active async Redis client for state storage.
        """
        self._redis = redis_client
        self._failure_threshold = settings.circuit_breaker_failure_threshold
        self._window_seconds = settings.circuit_breaker_window_seconds
        self._cooldown_seconds = settings.circuit_breaker_cooldown_seconds

    def _key(self, job_type: str, suffix: str) -> str:
        """Build a Redis key for the given job type and suffix."""
        return f"circuit:{job_type}:{suffix}"

    async def record_failure(self, job_type: str) -> None:
        """
        Record a job failure for circuit breaker evaluation.

        Adds the current timestamp to the failure sorted set, prunes
        old entries outside the window, then checks whether the
        threshold has been exceeded to trip the breaker.

        Args:
            job_type: The type of job that failed.
        """
        now = time.time()
        failures_key = self._key(job_type, "failures")

        # Add this failure timestamp to the sorted set
        await self._redis.zadd(failures_key, {str(now): now})

        # Remove failures older than the window
        cutoff = now - self._window_seconds
        await self._redis.zremrangebyscore(failures_key, "-inf", cutoff)

        # Count recent failures within the window
        failure_count = await self._redis.zcard(failures_key)

        if failure_count >= self._failure_threshold:
            current_state = await self.get_state(job_type)
            if current_state == "closed":
                # Trip the breaker: CLOSED -> OPEN
                await self._redis.set(self._key(job_type, "state"), "open")
                await self._redis.set(self._key(job_type, "opened_at"), str(now))

                logger.warning(
                    "circuit_breaker_opened",
                    job_type=job_type,
                    failure_count=failure_count,
                    threshold=self._failure_threshold,
                    window_seconds=self._window_seconds,
                )
            elif current_state == "half_open":
                # Probe failed: HALF_OPEN -> OPEN (reset cooldown)
                await self._redis.set(self._key(job_type, "state"), "open")
                await self._redis.set(self._key(job_type, "opened_at"), str(now))

                logger.warning(
                    "circuit_breaker_probe_failed",
                    job_type=job_type,
                )

    async def record_success(self, job_type: str) -> None:
        """
        Record a job success for circuit breaker evaluation.

        If the breaker is in HALF_OPEN state, a success means the
        underlying issue is resolved — close the breaker.

        Args:
            job_type: The type of job that succeeded.
        """
        current_state = await self.get_state(job_type)

        if current_state == "half_open":
            # Probe succeeded: HALF_OPEN -> CLOSED
            await self._redis.set(self._key(job_type, "state"), "closed")

            # Clear failure history since the issue is resolved
            await self._redis.delete(self._key(job_type, "failures"))
            await self._redis.delete(self._key(job_type, "opened_at"))

            logger.info(
                "circuit_breaker_closed",
                job_type=job_type,
                reason="probe_succeeded",
            )

    async def is_allowed(self, job_type: str) -> bool:
        """
        Check whether a job of the given type is allowed to execute.

        Returns True for CLOSED state. For OPEN state, checks whether
        the cooldown has elapsed and transitions to HALF_OPEN if so.
        For HALF_OPEN, allows exactly one probe job through.

        Args:
            job_type: The type of job to check.

        Returns:
            True if the job is allowed, False if blocked by the breaker.
        """
        state = await self.get_state(job_type)

        if state == "closed":
            return True

        if state == "open":
            # Check if cooldown has elapsed
            opened_at_str = await self._redis.get(self._key(job_type, "opened_at"))
            if opened_at_str:
                opened_at = float(opened_at_str)
                elapsed = time.time() - opened_at

                if elapsed >= self._cooldown_seconds:
                    # Cooldown expired: OPEN -> HALF_OPEN
                    await self._redis.set(self._key(job_type, "state"), "half_open")
                    logger.info(
                        "circuit_breaker_half_open",
                        job_type=job_type,
                        elapsed_seconds=elapsed,
                    )
                    return True  # Allow the probe job

            return False

        if state == "half_open":
            # In half-open, allow one probe (first caller wins).
            # Use SETNX pattern: try to set a probe-in-progress flag.
            probe_key = self._key(job_type, "probe_active")
            set_result = await self._redis.set(
                probe_key, "1", nx=True, ex=self._cooldown_seconds
            )
            if set_result:
                return True  # This caller gets to be the probe
            return False  # Another probe is already running

        return True  # Unknown state defaults to allowing

    async def get_state(self, job_type: str) -> str:
        """
        Get the current circuit breaker state for a job type.

        Args:
            job_type: The type of job to check.

        Returns:
            'closed', 'open', or 'half_open'. Defaults to 'closed'
            if no state has been set (breaker never tripped).
        """
        state = await self._redis.get(self._key(job_type, "state"))
        return state or "closed"

    async def reset(self, job_type: str) -> None:
        """
        Manually reset a circuit breaker to the closed state.

        Clears all failure history and state keys. Useful for
        administrative intervention when the underlying issue is
        known to be fixed.

        Args:
            job_type: The type of job whose breaker should be reset.
        """
        await self._redis.delete(
            self._key(job_type, "failures"),
            self._key(job_type, "state"),
            self._key(job_type, "opened_at"),
            self._key(job_type, "probe_active"),
        )
        logger.info("circuit_breaker_reset", job_type=job_type)
