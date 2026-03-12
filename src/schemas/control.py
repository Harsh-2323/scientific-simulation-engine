"""
Pydantic schemas for job control operations (pause, resume, cancel).

These schemas define the response format for control endpoints.
Request bodies are empty — the action is implied by the endpoint path.
"""

from pydantic import BaseModel


class ControlSignal(BaseModel):
    """Internal representation of a control signal published via Redis Pub/Sub."""

    action: str     # "pause" or "cancel"


class ControlResponse(BaseModel):
    """Response returned by pause/resume/cancel endpoints."""

    job_id: str
    action: str
    previous_status: str
    new_status: str
    message: str
