from __future__ import annotations

import asyncio
import logging

from distributed._signals import wait_for_signals
from distributed.compatibility import asyncio_run
from distributed.config import get_loop_factory
from distributed.serverless.dispatcher import SchedulerDispatcher

logger = logging.getLogger(__name__)


async def run():
    logger.info("-" * 47)
    dispatcher = SchedulerDispatcher(
        host="127.0.0.1",
        port=8888,
        protocol="ws"
    )
    logger.info("-" * 47)

    async def wait_for_dispatcher_to_finish():
        """Wait for the scheduler to initialize and finish"""
        await dispatcher
        await dispatcher.finished()

    async def wait_for_signals_and_close():
        """Wait for SIGINT or SIGTERM and close the scheduler upon receiving one of those signals"""
        signum = await wait_for_signals()
        await dispatcher.close(reason=f"signal-{signum}")

    wait_for_signals_and_close_task = asyncio.create_task(
        wait_for_signals_and_close()
    )
    wait_for_dispatcher_to_finish_task = asyncio.create_task(
        wait_for_dispatcher_to_finish()
    )

    done, _ = await asyncio.wait(
        [wait_for_signals_and_close_task, wait_for_dispatcher_to_finish_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    # Re-raise exceptions from done tasks
    [task.result() for task in done]
    logger.info("Stopped scheduler at %r", dispatcher.address)


if __name__ == "__main__":
    try:
        asyncio_run(run(), loop_factory=get_loop_factory())
    finally:
        logger.info("End scheduler")
