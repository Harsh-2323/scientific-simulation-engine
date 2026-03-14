"""
Custom exception classes for the simulation engine.

These exceptions provide structured error handling across all layers
of the application — from state machine validation to LLM integration.
"""


class SimulationEngineError(Exception):
    """Base exception for all simulation engine errors."""

    pass


class InvalidTransitionError(SimulationEngineError):
    """
    Raised when a job state transition is not permitted by the state machine.

    Contains the current status, the requested target status, and the job ID
    for diagnostic logging.
    """

    def __init__(self, job_id: str, current_status: str, target_status: str) -> None:
        self.job_id = job_id
        self.current_status = current_status
        self.target_status = target_status
        super().__init__(
            f"Invalid transition for job {job_id}: "
            f"{current_status} -> {target_status} is not allowed"
        )


class JobNotFoundError(SimulationEngineError):
    """Raised when a job ID does not exist in the database."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"Job not found: {job_id}")


class DagNotFoundError(SimulationEngineError):
    """Raised when a DAG ID does not exist in the database."""

    def __init__(self, dag_id: str) -> None:
        self.dag_id = dag_id
        super().__init__(f"DAG not found: {dag_id}")


class DagValidationError(SimulationEngineError):
    """
    Raised when a DAG specification fails validation.

    Contains a list of specific errors (cycles, missing refs, etc.)
    so the API can return them all to the caller at once.
    """

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__(f"DAG validation failed: {'; '.join(errors)}")


class CheckpointError(SimulationEngineError):
    """Raised when checkpoint write or load fails."""

    pass


class CheckpointNotFoundError(SimulationEngineError):
    """Raised when no valid checkpoint exists for a job that requires one."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"No valid checkpoint found for job {job_id}")


class CircuitBreakerOpenError(SimulationEngineError):
    """
    Raised when a job type's circuit breaker is in the OPEN state.

    Workers should skip jobs of this type until the breaker transitions
    to HALF_OPEN after the cooldown period.
    """

    def __init__(self, job_type: str) -> None:
        self.job_type = job_type
        super().__init__(f"Circuit breaker is open for job type: {job_type}")


class BackpressureError(SimulationEngineError):
    """
    Raised when total queue depth exceeds the backpressure limit.

    The API returns 429 Too Many Requests when this is raised.
    """

    def __init__(self, queue_depth: int, limit: int) -> None:
        self.queue_depth = queue_depth
        self.limit = limit
        super().__init__(
            f"Backpressure active: queue depth {queue_depth} exceeds limit {limit}"
        )


class LLMError(SimulationEngineError):
    """Raised when the Anthropic API call fails (network, auth, rate limit)."""

    pass


class LLMParseError(SimulationEngineError):
    """Raised when the LLM response is not valid JSON."""

    def __init__(self, raw_output: str, parse_error: str) -> None:
        self.raw_output = raw_output
        self.parse_error = parse_error
        super().__init__(f"Failed to parse LLM output as JSON: {parse_error}")


class LLMValidationError(SimulationEngineError):
    """Raised when the LLM response doesn't match the expected DagSubmitRequest schema."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__(f"LLM output validation failed: {'; '.join(errors)}")


class ConflictError(SimulationEngineError):
    """
    Raised on idempotency key conflicts or invalid control operations.

    Maps to HTTP 409 Conflict in the API layer.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
