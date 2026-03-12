"""
SQLAlchemy model for simulation checkpoints.

Checkpoints capture serialized intermediate state during job execution,
enabling pause/resume without losing work. The atomic write protocol
ensures a job always has at most one valid checkpoint.
"""

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base


class Checkpoint(Base):
    """
    Serialized intermediate state for pause/resume.

    Only one checkpoint per job should have is_valid=True at any time.
    When a new checkpoint is written, all previous checkpoints for
    that job are marked is_valid=False within the same transaction.
    """

    __tablename__ = "checkpoints"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    attempt_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("job_attempts.id", ondelete="SET NULL"), nullable=True
    )
    step_number: Mapped[int] = mapped_column(Integer, nullable=False)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    is_valid: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # Relationships
    job: Mapped["Job"] = relationship(back_populates="checkpoints")  # noqa: F821

    __table_args__ = (
        Index("idx_checkpoints_job_id", "job_id"),
        Index("idx_checkpoints_job_valid", "job_id", "is_valid", step_number.desc()),
    )
