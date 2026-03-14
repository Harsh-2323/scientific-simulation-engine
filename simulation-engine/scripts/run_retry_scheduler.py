"""
Retry scheduler entrypoint script.

Starts the retry scheduler process that re-enqueues jobs whose
backoff timer has expired. Runs continuously until terminated.

Usage:
    python scripts/run_retry_scheduler.py
"""

import asyncio
import signal
import sys

import structlog

sys.path.insert(0, ".")

from src.core.retry_scheduler import RetryScheduler

logger = structlog.get_logger(__name__)


async def main() -> None:
    """Start the retry scheduler with graceful shutdown handling."""
    scheduler = RetryScheduler()

    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("retry_scheduler_shutdown_signal")
        asyncio.create_task(scheduler.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    try:
        await scheduler.start()
    except KeyboardInterrupt:
        await scheduler.stop()


if __name__ == "__main__":
    asyncio.run(main())
