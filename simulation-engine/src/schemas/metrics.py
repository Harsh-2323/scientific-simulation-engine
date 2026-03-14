"""
Pydantic schemas for the metrics endpoint response.

Provides a snapshot of system health: queue depths, worker counts,
job throughput, and circuit breaker states.
"""

from pydantic import BaseModel, Field


class CircuitBreakerStatus(BaseModel):
    """Status of a single job type's circuit breaker."""

    job_type: str = Field(description="The job type this breaker protects")
    state: str = Field(description="Current state: closed, open, or half_open")


class MetricsResponse(BaseModel):
    """
    System-wide operational metrics.

    Provides enough information for operators to understand system
    health at a glance: queue pressure, worker capacity, throughput,
    and failure rates.
    """

    queue_depth: dict[str, int] = Field(
        description="Message count per priority stream"
    )
    total_queue_depth: int = Field(
        description="Sum of all priority stream lengths"
    )
    active_workers: int = Field(
        description="Workers currently executing jobs"
    )
    running_jobs: int = Field(
        description="Jobs with status='running'"
    )
    completed_last_hour: int = Field(
        description="Jobs completed in the last 60 minutes"
    )
    failed_last_hour: int = Field(
        description="Jobs failed in the last 60 minutes"
    )
    dead_letter_count: int = Field(
        description="Total jobs in dead_letter (exhausted retries)"
    )
