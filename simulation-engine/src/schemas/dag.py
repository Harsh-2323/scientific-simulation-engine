"""
Pydantic schemas for DAG submission, response, and node specification.

These schemas define the API contract for creating and viewing
directed acyclic graphs of simulation jobs.
"""

import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from src.models.enums import DagFailurePolicy, JobPriority


class DagNodeSpec(BaseModel):
    """
    Specification for a single node in a DAG submission.

    Each node becomes a Job row linked to the DAG via dag_id.
    Dependencies define execution ordering — a node only runs
    after all its parent nodes have completed.
    """

    node_id: str = Field(
        description="Unique identifier within this DAG (snake_case)",
        pattern=r"^[a-z0-9_]+$",
    )
    job_type: str = Field(description="Simulation type to execute")
    params: dict = Field(default_factory=dict, description="Job parameters")
    depends_on: list[str] = Field(
        default_factory=list,
        description="List of node_ids that must complete before this node runs",
    )
    priority: str = Field(default="normal", description="Job priority level")
    max_retries: int = Field(default=3, ge=0, description="Max retry attempts")


class DagSubmitRequest(BaseModel):
    """
    Request body for POST /api/v1/dags.

    Defines the full structure of a DAG: its nodes, their dependencies,
    failure handling policy, and optional metadata.
    """

    name: str = Field(description="Human-readable name for this DAG")
    description: str | None = Field(default=None, description="Optional description")
    failure_policy: str = Field(
        default="skip_downstream",
        description="How to handle node failures: fail_fast, skip_downstream, retry_node",
    )
    nodes: list[DagNodeSpec] = Field(
        min_length=1, description="List of DAG nodes to create"
    )
    metadata: dict = Field(default_factory=dict, description="Arbitrary metadata")


class DagNodeResponse(BaseModel):
    """Response representation of a DAG node and its associated job."""

    node_id: str
    job_id: uuid.UUID
    job_type: str
    job_status: str


class DagEdgeResponse(BaseModel):
    """Response representation of a dependency edge between two nodes."""

    parent_node_id: str
    child_node_id: str


class DagResponse(BaseModel):
    """
    Full DAG response including nodes, edges, and overall status.

    Returned after DAG creation and when querying DAG details.
    """

    id: uuid.UUID
    name: str
    description: str | None
    status: str
    failure_policy: str
    created_at: datetime
    nodes: list[DagNodeResponse]
    edges: list[DagEdgeResponse]
    metadata: dict = Field(default_factory=dict)


class DagListResponse(BaseModel):
    """Paginated list of DAGs."""

    items: list[DagResponse]
    total: int


class ValidationResult(BaseModel):
    """
    Result of DAG validation checks.

    Separates blocking errors (invalid graph structure) from
    informational warnings (suspicious but allowed parameters).
    """

    valid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
