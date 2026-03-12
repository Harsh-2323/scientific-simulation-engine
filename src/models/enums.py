"""
Enumeration types mirroring the PostgreSQL ENUM definitions.

These enums are used throughout the application for type-safe status
tracking and are mapped to database columns via SQLAlchemy.
"""

import enum


class JobStatus(str, enum.Enum):
    """
    All possible states in the job lifecycle state machine.

    Terminal states: COMPLETED, CANCELLED, DEAD_LETTER
    See docs/STATE_MACHINE.md for the full transition diagram.
    """

    PENDING = "pending"              # Job created, not yet queued
    QUEUED = "queued"                # In Redis stream, waiting for a worker
    RUNNING = "running"              # Worker has claimed and is executing
    COMPLETED = "completed"          # Terminal: finished successfully
    FAILED = "failed"                # Execution failed (may be retried)
    CANCELLING = "cancelling"        # Cancel signal sent, awaiting worker ack
    CANCELLED = "cancelled"          # Terminal: job was cancelled
    PAUSING = "pausing"              # Pause signal sent, awaiting worker checkpoint
    PAUSED = "paused"                # Worker checkpointed and yielded the job
    RESUMING = "resuming"            # Resume requested, job being re-enqueued
    RETRY_SCHEDULED = "retry_scheduled"  # Waiting for backoff timer before re-queue
    DEAD_LETTER = "dead_letter"      # Terminal: max retries exceeded


class JobPriority(str, enum.Enum):
    """
    Priority levels controlling worker claim order.

    Workers poll Redis streams in strict priority order:
    critical -> high -> normal -> low.
    """

    CRITICAL = "critical"   # Numeric value 0 — highest priority
    HIGH = "high"           # Numeric value 1
    NORMAL = "normal"       # Numeric value 2 — default
    LOW = "low"             # Numeric value 3 — lowest priority

    @property
    def numeric_value(self) -> int:
        """Return the numeric priority for comparison and aging calculations."""
        return {"critical": 0, "high": 1, "normal": 2, "low": 3}[self.value]

    @classmethod
    def from_numeric(cls, value: int) -> "JobPriority":
        """Convert a numeric priority back to the enum, clamping to valid range."""
        clamped = max(0, min(3, value))
        return {0: cls.CRITICAL, 1: cls.HIGH, 2: cls.NORMAL, 3: cls.LOW}[clamped]


class ErrorType(str, enum.Enum):
    """
    Classification of job errors, determining retry behavior.

    Transient errors are retried with exponential backoff.
    Permanent errors go straight to dead_letter.
    """

    TRANSIENT = "transient"
    PERMANENT = "permanent"


class DagStatus(str, enum.Enum):
    """
    Lifecycle status of a DAG graph.

    A DAG moves from pending -> running as root nodes are enqueued,
    and reaches a terminal state when all nodes finish.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PARTIALLY_FAILED = "partially_failed"


class DagFailurePolicy(str, enum.Enum):
    """
    How a DAG responds when one of its nodes reaches dead_letter.

    FAIL_FAST: Cancel all remaining nodes immediately.
    SKIP_DOWNSTREAM: Cancel only downstream dependents, let other branches continue.
    RETRY_NODE: Let the node's own retry policy handle it; fall back to SKIP_DOWNSTREAM on dead_letter.
    """

    FAIL_FAST = "fail_fast"
    SKIP_DOWNSTREAM = "skip_downstream"
    RETRY_NODE = "retry_node"
