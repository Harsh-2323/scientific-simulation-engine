"""
Application configuration using Pydantic Settings.

All values are overridable via environment variables (flat namespace, UPPER_CASE).
Reads from .env file if present.
"""

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Central configuration for the simulation engine.

    Controls database connections, worker behavior, retry policies,
    priority scheduling, circuit breaker thresholds, LLM integration,
    and logging output.
    """

    # === Database ===
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/simengine",
        description="Async SQLAlchemy connection URL for PostgreSQL",
    )
    db_pool_size: int = Field(default=10, description="SQLAlchemy connection pool size")
    db_pool_max_overflow: int = Field(default=20, description="Max overflow connections")

    # === Redis ===
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL",
    )

    # === Worker ===
    worker_heartbeat_interval_seconds: int = Field(
        default=5, description="How often workers refresh their heartbeat in Redis"
    )
    worker_lease_timeout_seconds: int = Field(
        default=30, description="Lease duration. Must be > heartbeat_interval * 3"
    )
    worker_max_concurrent_jobs: int = Field(
        default=1, description="Max jobs a single worker runs concurrently"
    )
    worker_claim_poll_interval_ms: int = Field(
        default=500, description="Redis XREADGROUP BLOCK timeout in milliseconds"
    )

    # === Reaper ===
    reaper_interval_seconds: int = Field(
        default=30, description="How often the reaper scans for stale jobs"
    )
    reaper_reconcile_threshold_seconds: int = Field(
        default=300, description="Re-enqueue queued jobs not in Redis after this many seconds"
    )

    # === Retry Scheduler ===
    retry_scheduler_interval_seconds: int = Field(
        default=10, description="How often the retry scheduler checks for due retries"
    )

    # === Retry Policy (defaults, overridable per job) ===
    default_max_retries: int = Field(default=3, description="Default max retry attempts")
    default_base_backoff_seconds: float = Field(default=5.0, description="Base backoff duration")
    default_max_backoff_seconds: float = Field(default=300.0, description="Max backoff cap")
    default_backoff_multiplier: float = Field(default=2.0, description="Exponential multiplier")

    # === Priority & Scheduling ===
    aging_threshold_seconds: int = Field(
        default=300, description="Queue time before priority is bumped by 1 level"
    )
    backpressure_queue_depth_limit: int = Field(
        default=10000, description="Total queue depth triggering 429 backpressure"
    )

    # === Circuit Breaker ===
    circuit_breaker_failure_threshold: int = Field(
        default=5, description="Failures of same job_type to trip circuit"
    )
    circuit_breaker_window_seconds: int = Field(
        default=300, description="Time window for counting failures"
    )
    circuit_breaker_cooldown_seconds: int = Field(
        default=600, description="Wait time before half-open probe"
    )

    # === LLM ===
    anthropic_api_key: str = Field(
        default="", description="Anthropic API key. Set via ANTHROPIC_API_KEY env var."
    )
    llm_model: str = Field(
        default="claude-sonnet-4-20250514", description="Anthropic model for decomposition"
    )
    llm_max_tokens: int = Field(default=4096, description="Max tokens for LLM response")

    # === DAG ===
    max_dag_nodes: int = Field(default=50, description="Maximum nodes per DAG")
    default_dag_failure_policy: str = Field(
        default="skip_downstream", description="Default failure policy for DAGs"
    )

    # === Simulation Runner ===
    default_simulation_steps: int = Field(
        default=100, description="Default steps for simulated work"
    )
    default_step_duration_seconds: float = Field(
        default=0.1, description="Default duration per simulation step (for testing)"
    )
    checkpoint_interval_steps: int = Field(
        default=10, description="Checkpoint every N steps"
    )

    # === Logging ===
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(default="json", description="Log format: 'json' or 'console'")

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


# Singleton settings instance, loaded once at import time
settings = Settings()
