"""
SQLAlchemy models for DAG (Directed Acyclic Graph) orchestration.

A DAG represents a dependency graph of jobs — each node is a job,
and edges define execution order. The DAG executor enqueues root
nodes first, then progressively unlocks child nodes as their
parents complete.
"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base


class DagGraph(Base):
    """
    Top-level DAG container.

    Holds the DAG name, description, failure policy, and overall status.
    Individual nodes are stored as Job rows linked via dag_id.
    Dependencies between nodes are stored as DagEdge rows.
    """

    __tablename__ = "dag_graphs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(512), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="pending")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # LLM decomposition metadata (populated when DAG was created via /dags/decompose)
    natural_language_input: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_llm_output: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # How to handle node failures within this DAG
    failure_policy: Mapped[str] = mapped_column(
        String(50), nullable=False, default="skip_downstream"
    )

    # Arbitrary metadata for extensibility
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, default=dict)

    # Relationships
    edges: Mapped[list["DagEdge"]] = relationship(
        back_populates="dag", cascade="all, delete-orphan"
    )


class DagEdge(Base):
    """
    Dependency edge between two jobs within a DAG.

    parent_node_id must complete before child_node_id can be enqueued.
    Node IDs reference the dag_node_id column on the Job model.
    """

    __tablename__ = "dag_edges"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    dag_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dag_graphs.id", ondelete="CASCADE"), nullable=False
    )
    parent_node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    child_node_id: Mapped[str] = mapped_column(String(255), nullable=False)

    # Relationships
    dag: Mapped["DagGraph"] = relationship(back_populates="edges")

    __table_args__ = (
        UniqueConstraint("dag_id", "parent_node_id", "child_node_id", name="uq_dag_edge"),
        Index("idx_dag_edges_dag_id", "dag_id"),
        Index("idx_dag_edges_parent", "dag_id", "parent_node_id"),
        Index("idx_dag_edges_child", "dag_id", "child_node_id"),
    )
