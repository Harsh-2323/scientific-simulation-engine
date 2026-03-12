"""
Reaper entrypoint script.

Starts the reaper process that detects dead workers and reclaims
their jobs. Runs continuously until terminated.

Usage:
    python scripts/run_reaper.py
"""

import asyncio
import signal
import sys

import structlog

sys.path.insert(0, ".")

from src.core.reaper import Reaper

logger = structlog.get_logger(__name__)


async def main() -> None:
    """Start the reaper with graceful shutdown handling."""
    reaper = Reaper()

    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("reaper_shutdown_signal")
        asyncio.create_task(reaper.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    try:
        await reaper.start()
    except KeyboardInterrupt:
        await reaper.stop()


if __name__ == "__main__":
    asyncio.run(main())
