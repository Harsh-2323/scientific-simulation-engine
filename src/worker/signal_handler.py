"""
Redis Pub/Sub listener for job control signals.

Workers subscribe to per-job control channels to receive real-time
pause and cancel commands from the API server. The signal handler
runs as a background task alongside job execution, setting flags
that the execution loop checks between simulation steps.
"""

import asyncio
import json
import uuid

import redis.asyncio as redis

import structlog

logger = structlog.get_logger(__name__)


class SignalHandler:
    """
    Listens for control signals (pause, cancel) on a job's Redis Pub/Sub channel.

    The API server publishes signals to 'job:{job_id}:control' when a user
    requests pause or cancel. This handler receives those signals and sets
    flags that the executor checks between simulation steps.

    Usage:
        handler = SignalHandler(redis_client, job_id)
        await handler.start()
        ...
        if handler.should_pause: ...
        if handler.should_cancel: ...
        ...
        await handler.stop()
    """

    def __init__(self, redis_client: redis.Redis, job_id: uuid.UUID) -> None:
        """
        Initialize the signal handler.

        Args:
            redis_client: Active async Redis client.
            job_id: UUID of the job to listen for signals on.
        """
        self._redis = redis_client
        self._job_id = job_id
        self._channel_name = f"job:{job_id}:control"
        self._pubsub: redis.client.PubSub | None = None
        self._listener_task: asyncio.Task | None = None

        # Flags checked by the executor between steps
        self.should_pause = False
        self.should_cancel = False

        self._log = logger.bind(job_id=str(job_id))

    async def start(self) -> None:
        """
        Subscribe to the job's control channel and start listening.

        Spawns a background task that reads messages from the Pub/Sub
        channel and sets the appropriate signal flags.
        """
        self._pubsub = self._redis.pubsub()
        await self._pubsub.subscribe(self._channel_name)
        self._listener_task = asyncio.create_task(self._listen())
        self._log.debug("signal_handler_started", channel=self._channel_name)

    async def stop(self) -> None:
        """
        Unsubscribe and clean up the Pub/Sub connection.

        Cancels the listener task and closes the subscription.
        """
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass

        if self._pubsub:
            await self._pubsub.unsubscribe(self._channel_name)
            await self._pubsub.close()

        self._log.debug("signal_handler_stopped")

    async def _listen(self) -> None:
        """
        Background loop that reads control messages from the Pub/Sub channel.

        Expected message format: {"action": "pause"} or {"action": "cancel"}
        Ignores subscription confirmation messages and invalid payloads.
        """
        try:
            async for message in self._pubsub.listen():
                if message["type"] != "message":
                    # Skip subscription confirmations and other non-data messages
                    continue

                try:
                    payload = json.loads(message["data"])
                    action = payload.get("action")

                    if action == "cancel":
                        self.should_cancel = True
                        self._log.info("cancel_signal_received")
                    elif action == "pause":
                        self.should_pause = True
                        self._log.info("pause_signal_received")
                    else:
                        self._log.warning("unknown_signal", action=action)

                except (json.JSONDecodeError, AttributeError) as e:
                    self._log.warning("invalid_signal_payload", error=str(e))

        except asyncio.CancelledError:
            # Normal shutdown path
            pass
        except Exception as e:
            self._log.error("signal_listener_error", error=str(e))


async def publish_control_signal(
    redis_client: redis.Redis,
    job_id: uuid.UUID,
    action: str,
) -> int:
    """
    Publish a control signal to a job's Pub/Sub channel.

    Called by the API server when a user requests pause or cancel.

    Args:
        redis_client: Active async Redis client.
        job_id: UUID of the target job.
        action: The control action ('pause' or 'cancel').

    Returns:
        Number of subscribers that received the message (0 if no worker is listening).
    """
    channel = f"job:{job_id}:control"
    payload = json.dumps({"action": action})
    receivers = await redis_client.publish(channel, payload)

    logger.info(
        "control_signal_published",
        job_id=str(job_id),
        action=action,
        receivers=receivers,
    )
    return receivers
