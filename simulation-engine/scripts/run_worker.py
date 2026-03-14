"""
Worker entrypoint script.

Starts a single worker process that claims and executes simulation jobs.
Handles SIGTERM/SIGINT for graceful shutdown with checkpoint preservation.

Usage:
    python scripts/run_worker.py
    python scripts/run_worker.py --worker-id my-custom-id
"""

import asyncio
import signal
import sys

import structlog

# Ensure the project root is on the Python path
sys.path.insert(0, ".")

from src.config import settings
from src.worker.base import Worker

logger = structlog.get_logger(__name__)


async def main() -> None:
    """Initialize and run a worker, handling shutdown signals."""
    # Parse optional worker ID from command line
    worker_id = None
    if len(sys.argv) > 2 and sys.argv[1] == "--worker-id":
        worker_id = sys.argv[2]

    worker = Worker(worker_id=worker_id)

    # Register signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("shutdown_signal_received")
        asyncio.create_task(worker.shutdown())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    try:
        await worker.start()
    except KeyboardInterrupt:
        await worker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
