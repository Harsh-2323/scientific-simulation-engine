"""
SQLAlchemy model for the worker registry.

Tracks active workers so the reaper can detect dead ones
and the metrics endpoint can report worker counts.
Workers self-register on startup and update their heartbeat
on a configurable interval.
"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class WorkerRegistry(Base):
    """
    Active worker tracking.

    Each worker inserts a row on startup and updates last_heartbeat
    periodically. The reaper deletes rows whose heartbeat has gone
    stale (>5 minutes without update and status != 'stopped').
    """

    __tablename__ = "worker_registry"

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    hostname: Mapped[str] = mapped_column(String(512), nullable=False)
    pid: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_heartbeat: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="idle")
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    current_job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True
    )
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, default=dict)

    __table_args__ = (
        Index("idx_worker_heartbeat", "last_heartbeat"),
    )
