"""
FastAPI application entry point.

Assembles the API server by including all route modules, setting up
database and Redis connections on startup, and tearing them down on
shutdown. Run with: uvicorn src.main:app --host 0.0.0.0 --port 8000
"""

from contextlib import asynccontextmanager

import logging

import structlog
from fastapi import FastAPI

from src.api.dependencies import init_redis, close_redis
from src.api.routes import health, jobs, control, metrics, dag
from src.config import settings

# Map string log level to numeric value for structlog's filtering bound logger
_LOG_LEVEL = getattr(logging, settings.log_level.upper(), logging.INFO)

# Configure structlog for consistent JSON or console output
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()
        if settings.log_format == "console"
        else structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(_LOG_LEVEL),
)

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle — startup and shutdown hooks.

    On startup: initialize Redis connection pool.
    On shutdown: close Redis connections and release resources.
    """
    logger.info("app_starting", log_level=settings.log_level)

    # Initialize Redis client for the application
    await init_redis()
    logger.info("redis_connected", url=settings.redis_url)

    yield

    # Cleanup on shutdown
    await close_redis()
    logger.info("app_shutdown")


app = FastAPI(
    title="Scientific Simulation Engine",
    description=(
        "Distributed job execution engine for long-running scientific simulations "
        "with checkpointing, DAG orchestration, and LLM-powered decomposition."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# Include route modules under the /api/v1 prefix
app.include_router(health.router, prefix="/api/v1")
app.include_router(jobs.router, prefix="/api/v1")
app.include_router(control.router, prefix="/api/v1")
app.include_router(metrics.router, prefix="/api/v1")
app.include_router(dag.router, prefix="/api/v1")
