"""
Redis Streams wrapper for job queue operations.

Abstracts the Redis Streams commands (XADD, XREADGROUP, XACK, XLEN)
behind a clean async interface. Each priority level has its own stream,
and workers are organized into a consumer group called 'workers'.
"""

import uuid

import redis.asyncio as redis

import structlog

logger = structlog.get_logger(__name__)

# Consumer group name shared by all workers
CONSUMER_GROUP = "workers"

# Stream names per priority level
STREAM_NAMES = {
    "critical": "jobs:priority:critical",
    "high": "jobs:priority:high",
    "normal": "jobs:priority:normal",
    "low": "jobs:priority:low",
}

# Workers poll streams in this strict priority order
PRIORITY_ORDER = ["critical", "high", "normal", "low"]


async def ensure_consumer_groups(redis_client: redis.Redis) -> None:
    """
    Create consumer groups for all priority streams if they don't exist.

    Called on worker startup. Uses MKSTREAM to auto-create the stream
    if it doesn't exist yet. Ignores errors if the group already exists.

    Args:
        redis_client: Active async Redis client.
    """
    for priority, stream in STREAM_NAMES.items():
        try:
            await redis_client.xgroup_create(
                stream, CONSUMER_GROUP, id="0", mkstream=True
            )
            logger.info("consumer_group_created", stream=stream, group=CONSUMER_GROUP)
        except redis.ResponseError as e:
            # "BUSYGROUP Consumer Group name already exists" is expected
            if "BUSYGROUP" in str(e):
                pass
            else:
                raise


async def xadd_job(
    redis_client: redis.Redis,
    job_id: uuid.UUID,
    priority: str,
) -> str:
    """
    Add a job to the appropriate priority stream.

    Args:
        redis_client: Active async Redis client.
        job_id: UUID of the job to enqueue.
        priority: Priority level ('critical', 'high', 'normal', 'low').

    Returns:
        The Redis stream message ID assigned to this entry.
    """
    stream = STREAM_NAMES.get(priority, STREAM_NAMES["normal"])
    message_id = await redis_client.xadd(stream, {"job_id": str(job_id)})

    logger.info("job_enqueued", job_id=str(job_id), stream=stream, message_id=message_id)
    return message_id


async def xreadgroup_job(
    redis_client: redis.Redis,
    consumer_name: str,
    *,
    block_ms: int = 500,
) -> tuple[str, str, uuid.UUID] | None:
    """
    Read one job from Redis streams in strict priority order.

    Tries each priority stream from critical to low. Returns the first
    available message, or None if all streams are empty after blocking.

    Args:
        redis_client: Active async Redis client.
        consumer_name: Unique worker ID used as the consumer name.
        block_ms: How long to block waiting for messages (milliseconds).

    Returns:
        Tuple of (stream_name, message_id, job_id) if a message was read,
        or None if no messages were available.
    """
    for priority in PRIORITY_ORDER:
        stream = STREAM_NAMES[priority]
        try:
            result = await redis_client.xreadgroup(
                CONSUMER_GROUP,
                consumer_name,
                {stream: ">"},   # ">" means only new messages not yet delivered
                count=1,
                block=block_ms,
            )

            if result:
                # result format: [[stream_name, [(message_id, {fields})]]]
                stream_name = result[0][0]
                message_id = result[0][1][0][0]
                fields = result[0][1][0][1]
                job_id = uuid.UUID(fields["job_id"])

                logger.debug(
                    "message_read",
                    stream=stream_name,
                    message_id=message_id,
                    job_id=str(job_id),
                    consumer=consumer_name,
                )
                return stream_name, message_id, job_id

        except Exception as e:
            logger.warning("xreadgroup_error", stream=stream, error=str(e))
            continue

    return None


async def xack_job(
    redis_client: redis.Redis,
    stream: str,
    message_id: str,
) -> None:
    """
    Acknowledge a message in a Redis stream, removing it from the PEL.

    Called after a worker successfully processes (or decides to skip) a job.
    Unacknowledged messages will be reclaimed by the reaper.

    Args:
        redis_client: Active async Redis client.
        stream: The stream name the message was read from.
        message_id: The Redis message ID to acknowledge.
    """
    await redis_client.xack(stream, CONSUMER_GROUP, message_id)
    logger.debug("message_acked", stream=stream, message_id=message_id)


async def xlen_stream(redis_client: redis.Redis, stream: str) -> int:
    """
    Get the total number of messages in a stream.

    Used for metrics and backpressure calculations.

    Args:
        redis_client: Active async Redis client.
        stream: The stream name to measure.

    Returns:
        Number of messages in the stream.
    """
    try:
        return await redis_client.xlen(stream)
    except Exception:
        return 0


async def get_queue_depths(redis_client: redis.Redis) -> dict[str, int]:
    """
    Get message counts for all priority streams.

    Args:
        redis_client: Active async Redis client.

    Returns:
        Dict mapping priority names to their queue depths.
    """
    depths = {}
    for priority, stream in STREAM_NAMES.items():
        depths[priority] = await xlen_stream(redis_client, stream)
    return depths
