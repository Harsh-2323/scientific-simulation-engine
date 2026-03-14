"""
Priority aging scheduler — prevents low-priority job starvation.

When a job sits in the queue longer than the configured threshold,
its effective priority is bumped up by one level. This ensures that
even low-priority jobs eventually get executed, preventing indefinite
starvation by a steady stream of higher-priority work.

The effective priority is calculated at claim time and when re-enqueuing
retried jobs, determining which Redis stream the job should be placed in.
"""

from datetime import datetime, timezone

from src.config import settings
from src.models.enums import JobPriority

import structlog

logger = structlog.get_logger(__name__)


def calculate_effective_priority(base_priority: str, queued_at: datetime | None) -> str:
    """
    Calculate the effective priority of a job, accounting for queue aging.

    For every aging_threshold_seconds the job has been waiting in the queue,
    its effective priority is bumped by one level (lower number = higher
    priority). The effective priority is capped at 'critical' (level 0).

    Args:
        base_priority: The job's original priority ('critical', 'high', 'normal', 'low').
        queued_at: When the job entered the queue. If None, returns base_priority.

    Returns:
        The effective priority string after applying aging bumps.

    Examples:
        - 'low' job queued for 600s with 300s threshold → 'normal' (bumped 1 level)
        - 'low' job queued for 900s with 300s threshold → 'high' (bumped 2 levels)
        - 'normal' job queued for 900s → 'critical' (capped at highest)
    """
    if queued_at is None:
        return base_priority

    now = datetime.now(timezone.utc)
    # Handle timezone-naive timestamps by assuming UTC
    if queued_at.tzinfo is None:
        from datetime import timezone as tz
        queued_at = queued_at.replace(tzinfo=tz.utc)

    wait_seconds = (now - queued_at).total_seconds()

    # No aging if the job hasn't waited long enough
    if wait_seconds < settings.aging_threshold_seconds:
        return base_priority

    # Calculate how many priority levels to bump
    age_bumps = int(wait_seconds / settings.aging_threshold_seconds)

    # Convert priority to numeric: critical=0, high=1, normal=2, low=3
    try:
        priority_enum = JobPriority(base_priority)
        base_numeric = priority_enum.numeric_value
    except ValueError:
        return base_priority

    # Bump priority (lower number = higher priority), capped at 0
    effective_numeric = max(0, base_numeric - age_bumps)

    effective_priority = JobPriority.from_numeric(effective_numeric)

    if effective_priority.value != base_priority:
        logger.debug(
            "priority_aged",
            base_priority=base_priority,
            effective_priority=effective_priority.value,
            wait_seconds=int(wait_seconds),
            age_bumps=age_bumps,
        )

    return effective_priority.value
