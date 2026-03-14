"""
Pydantic schemas for the LLM decomposition endpoint.

Defines the request and response models for converting
natural language instructions into DAG specifications.
"""

from pydantic import BaseModel, Field

from src.schemas.dag import DagResponse, DagSubmitRequest, ValidationResult


class DecomposeRequest(BaseModel):
    """
    Request body for POST /api/v1/dags/decompose.

    Provides a natural language instruction that the LLM will
    decompose into a structured DAG of simulation sub-jobs.
    """

    instruction: str = Field(
        description="Natural language description of the research goal"
    )
    available_types: list[str] | None = Field(
        default=None,
        description="Optional filter — only use these simulation types",
    )
    failure_policy: str = Field(
        default="skip_downstream",
        description="Failure policy for the generated DAG",
    )
    dry_run: bool = Field(
        default=False,
        description="If True, return the spec without creating the DAG",
    )


class DecomposeResponse(BaseModel):
    """
    Response from the LLM decomposition endpoint.

    Always includes the generated spec and validation result.
    The dag field is populated only when dry_run=False and
    validation passes.
    """

    dag: DagResponse | None = Field(
        default=None,
        description="The created DAG (None if dry_run or validation failed)",
    )
    dag_spec: DagSubmitRequest = Field(
        description="The generated DAG specification",
    )
    validation_result: ValidationResult = Field(
        description="Results of all validation checks",
    )
    llm_model: str = Field(description="Model used for decomposition")
    llm_latency_ms: int = Field(description="Time spent on LLM call in ms")
    quality_warnings: list[str] = Field(
        default_factory=list,
        description="Heuristic quality warnings about the decomposition",
    )
