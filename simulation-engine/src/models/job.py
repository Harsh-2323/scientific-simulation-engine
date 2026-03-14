"""
SQLAlchemy models for jobs, job attempts, and job events.

The Job model is the central entity — it tracks the full lifecycle
of a simulation workload from submission through completion or failure.
JobAttempt records each execution attempt by a worker.
JobEvent provides an append-only audit log of every state transition.
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base

# PostgreSQL ENUM types — create_type=False because the migration creates them
_job_status_enum = Enum(
    'pending', 'queued', 'running', 'completed', 'failed',
    'cancelling', 'cancelled', 'pausing', 'paused', 'resuming',
    'retry_scheduled', 'dead_letter',
    name='job_status', create_type=False,
)
_job_priority_enum = Enum(
    'critical', 'high', 'normal', 'low',
    name='job_priority', create_type=False,
)
_error_type_enum = Enum(
    'transient', 'permanent',
    name='error_type', create_type=False,
)


class Job(Base):
    """
    Core job table. Single source of truth for job state.

    Every status change must go through the state machine's transition_job()
    function — never assign job.status directly.
    """

    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    type: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(_job_status_enum, nullable=False, default="pending")
    priority: Mapped[str] = mapped_column(_job_priority_enum, nullable=False, default="normal")
    params: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Timestamps tracking the job lifecycle
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Worker assignment and lease tracking
    worker_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Retry configuration (set at submission, with defaults from config)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    backoff_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=5.0)
    backoff_multiplier: Mapped[float] = mapped_column(Float, nullable=False, default=2.0)
    max_backoff_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=300.0)

    # Idempotency key for deduplication of submissions
    idempotency_key: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True)

    # Error tracking
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_type: Mapped[str | None] = mapped_column(_error_type_enum, nullable=True)

    # Progress reporting during execution
    progress_percent: Mapped[float] = mapped_column(Float, default=0.0)
    progress_message: Mapped[str | None] = mapped_column(String(1024), nullable=True)

    # DAG membership (NULL if standalone job)
    dag_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dag_graphs.id", ondelete="SET NULL"), nullable=True
    )
    dag_node_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Arbitrary metadata for extensibility
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, default=dict)

    # Relationships
    attempts: Mapped[list["JobAttempt"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )
    events: Mapped[list["JobEvent"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )
    checkpoints: Mapped[list["Checkpoint"]] = relationship(  # noqa: F821
        back_populates="job", cascade="all, delete-orphan"
    )

    # Indexes matching the spec's exact index definitions
    __table_args__ = (
        Index("idx_jobs_status", "status"),
        Index("idx_jobs_priority_status", "priority", "status"),
        Index("idx_jobs_status_lease", "status", "lease_expires_at"),
        Index("idx_jobs_retry_scheduled", "next_retry_at"),
        Index("idx_jobs_dag_id", "dag_id"),
        Index("idx_jobs_idempotency_key", "idempotency_key"),
        Index("idx_jobs_worker_id", "worker_id"),
        Index("idx_jobs_created_at", "created_at"),
    )


class JobAttempt(Base):
    """
    One row per execution attempt of a job.

    Tracks which worker ran each attempt, when it started/finished,
    and whether it succeeded or failed. Links to the checkpoint
    that was active during this attempt (if any).
    """

    __tablename__ = "job_attempts"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    worker_id: Mapped[str] = mapped_column(String(255), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="running")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    checkpoint_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("checkpoints.id", ondelete="SET NULL"), nullable=True
    )

    # Relationships
    job: Mapped["Job"] = relationship(back_populates="attempts")

    __table_args__ = (
        UniqueConstraint("job_id", "attempt_number", name="uq_job_attempt_number"),
        Index("idx_attempts_job_id", "job_id"),
    )


class JobEvent(Base):
    """
    Append-only audit log of every state transition.

    Every call to transition_job() writes a row here, recording
    who triggered the transition and any associated metadata.
    """

    __tablename__ = "job_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    old_status: Mapped[str | None] = mapped_column(_job_status_enum, nullable=True)
    new_status: Mapped[str | None] = mapped_column(_job_status_enum, nullable=True)
    triggered_by: Mapped[str] = mapped_column(String(100), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, default=dict)

    # Relationships
    job: Mapped["Job"] = relationship(back_populates="events")

    __table_args__ = (
        Index("idx_events_job_id", "job_id"),
        Index("idx_events_timestamp", "timestamp"),
    )


# Avoid circular import — Checkpoint is defined in checkpoint.py
from src.models.checkpoint import Checkpoint  # noqa: E402, F401
