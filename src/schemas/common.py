"""
Shared Pydantic schemas used across multiple API endpoints.

These base schemas provide common patterns like paginated responses
and timestamp formatting that all resource schemas build on.
"""

from datetime import datetime

from pydantic import BaseModel, Field


class PaginatedParams(BaseModel):
    """Common query parameters for paginated list endpoints."""

    limit: int = Field(default=50, le=200, ge=1, description="Results per page")
    offset: int = Field(default=0, ge=0, description="Number of results to skip")
