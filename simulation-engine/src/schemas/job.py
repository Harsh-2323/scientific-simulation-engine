"""
Pydantic schemas for job-related API requests and responses.

These schemas handle validation of incoming requests (JobSubmitRequest),
serialization of database models to JSON (JobResponse), and query
parameter parsing (JobListParams).
"""

import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from src.models.enums import JobStatus, JobPriority


# ── Request schemas ─────────────────────────────────────────────


class JobSubmitRequest(BaseModel):
    """Request body for POST /api/v1/jobs — submit a new simulation job."""

    type: str = Field(description="Simulation type, e.g. 'cfd_simulation', 'mesh_generation'")
    params: dict = Field(default={}, description="Simulation parameters")
    priority: JobPriority = Field(default=JobPriority.NORMAL, description="Job priority level")
    max_retries: int = Field(default=3, ge=0, description="Max retry attempts before dead_letter")
    idempotency_key: str | None = Field(default=None, description="Optional dedup key")
    metadata: dict = Field(default={}, description="Arbitrary metadata")


class JobListParams(BaseModel):
    """Query parameters for GET /api/v1/jobs — list jobs with filters."""

    status: JobStatus | None = None
    priority: JobPriority | None = None
    type: str | None = None
    dag_id: uuid.UUID | None = None
    created_after: datetime | None = None
    created_before: datetime | None = None
    limit: int = Field(default=50, le=200, ge=1)
    offset: int = Field(default=0, ge=0)
    sort_by: str = Field(default="created_at", pattern="^(created_at|updated_at|priority)$")
    sort_order: str = Field(default="desc", pattern="^(asc|desc)$")


# ── Response schemas ────────────────────────────────────────────


class AttemptResponse(BaseModel):
    """Serialized view of a single execution attempt."""

    id: uuid.UUID
    worker_id: str
    attempt_number: int
    started_at: datetime
    finished_at: datetime | None
    status: str
    error_message: str | None

    model_config = {"from_attributes": True}


class JobResponse(BaseModel):
    """Full job representation returned by all job endpoints."""

    id: uuid.UUID
    type: str
    status: str
    priority: str
    params: dict
    result: dict | None
    created_at: datetime
    updated_at: datetime
    queued_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    worker_id: str | None
    retry_count: int
    max_retries: int
    progress_percent: float
    progress_message: str | None
    error_message: str | None
    error_type: str | None
    dag_id: uuid.UUID | None
    dag_node_id: str | None
    metadata: dict

    model_config = {"from_attributes": True}

    @classmethod
    def from_job(cls, job) -> "JobResponse":
        """
        Construct a JobResponse from a Job ORM model.

        Handles the metadata_ -> metadata field name mapping that exists
        because 'metadata' is a reserved attribute in SQLAlchemy.
        """
        return cls(
            id=job.id,
            type=job.type,
            status=job.status,
            priority=job.priority,
            params=job.params,
            result=job.result,
            created_at=job.created_at,
            updated_at=job.updated_at,
            queued_at=job.queued_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            worker_id=job.worker_id,
            retry_count=job.retry_count,
            max_retries=job.max_retries,
            progress_percent=job.progress_percent,
            progress_message=job.progress_message,
            error_message=job.error_message,
            error_type=job.error_type,
            dag_id=job.dag_id,
            dag_node_id=job.dag_node_id,
            metadata=job.metadata_,
        )


class JobDetailResponse(JobResponse):
    """Extended job response that includes the list of execution attempts."""

    attempts: list[AttemptResponse] = []

    @classmethod
    def from_job_with_attempts(cls, job, attempts=None) -> "JobDetailResponse":
        """Build a detail response with nested attempts."""
        base = JobResponse.from_job(job)
        attempt_list = []
        if attempts:
            attempt_list = [AttemptResponse.model_validate(a) for a in attempts]
        return cls(**base.model_dump(), attempts=attempt_list)


class JobListResponse(BaseModel):
    """Paginated list of jobs."""

    items: list[JobResponse]
    total: int
    limit: int
    offset: int


class EventResponse(BaseModel):
    """Serialized view of a single job event (audit log entry)."""

    id: uuid.UUID
    event_type: str
    old_status: str | None
    new_status: str | None
    triggered_by: str
    timestamp: datetime
    metadata: dict

    model_config = {"from_attributes": True}

    @classmethod
    def from_event(cls, event) -> "EventResponse":
        """Construct from a JobEvent ORM model."""
        return cls(
            id=event.id,
            event_type=event.event_type,
            old_status=event.old_status,
            new_status=event.new_status,
            triggered_by=event.triggered_by,
            timestamp=event.timestamp,
            metadata=event.metadata_,
        )


class EventListResponse(BaseModel):
    """List of events for a specific job."""

    items: list[EventResponse]
    total: int


class CheckpointResponse(BaseModel):
    """Serialized view of a checkpoint (preview only — not the full data)."""

    id: uuid.UUID
    step_number: int
    created_at: datetime
    is_valid: bool
    data_preview: dict = Field(description="First 3 keys of checkpoint data, for inspection")

    model_config = {"from_attributes": True}


class CheckpointListResponse(BaseModel):
    """List of checkpoints for a specific job."""

    items: list[CheckpointResponse]
    total: int
