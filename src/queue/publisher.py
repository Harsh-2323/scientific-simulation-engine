"""
Job enqueue helper.

Provides a high-level function for adding jobs to the Redis priority
streams. Used by the API server when submitting jobs, the retry scheduler
when re-enqueuing, and the DAG executor when unlocking child nodes.
"""

import uuid

import redis.asyncio as redis

from src.queue.redis_stream import xadd_job

import structlog

logger = structlog.get_logger(__name__)


async def enqueue_job(
    redis_client: redis.Redis,
    job_id: uuid.UUID,
    priority: str,
) -> str:
    """
    Enqueue a job into the Redis stream matching its priority level.

    This is the single entry point for putting jobs into the queue.
    All components (API, retry scheduler, DAG executor) use this
    function to ensure consistent enqueue behavior.

    Args:
        redis_client: Active async Redis client.
        job_id: UUID of the job to enqueue.
        priority: Priority level string ('critical', 'high', 'normal', 'low').

    Returns:
        The Redis stream message ID assigned to this entry.
    """
    message_id = await xadd_job(redis_client, job_id, priority)
    logger.info("job_published", job_id=str(job_id), priority=priority)
    return message_id
