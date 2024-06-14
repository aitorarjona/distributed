from __future__ import annotations

import asyncio
import logging

import dask.config

from distributed import Worker
from distributed._signals import wait_for_signals
from distributed.compatibility import asyncio_run
from distributed.config import get_loop_factory
from distributed.serverless.worker import bootstrap_worker

logger = logging.getLogger(__name__)


async def run():
    dask.config.set({"scheduler-address": "amqp://Scheduler-00000000-0000-0000-0000-000000000000:0"})
    logger.info("-" * 47)
    worker = Worker(
        # scheduler_ip="amqp://Scheduler-00000000-0000-0000-0000-000000000000:0",
        # scheduler_ip="ws://127.0.0.1",
        # scheduler_port=5555,
        nthreads=1,
        name="Worker-0-00000000-0000-0000-0000-000000000000",
        contact_address="amqp://Worker-0-00000000-0000-0000-0000-000000000000:0",
        heartbeat_interval="5s",
        dashboard=False,
        # host="127.0.0.1",
        # port=8989,
        # protocol="tcp",
        nanny=None,
        validate=False,
        memory_limit=2147483648,
    )
    logger.info("-" * 47)

    async def wait_for_worker_to_finish():
        """Wait for the scheduler to initialize and finish"""
        await bootstrap_worker(worker)
        await worker.finished()

    async def wait_for_signals_and_close():
        """Wait for SIGINT or SIGTERM and close the scheduler upon receiving one of those signals"""
        signum = await wait_for_signals()
        await worker.close(reason=f"signal-{signum}")

    wait_for_signals_and_close_task = asyncio.create_task(
        wait_for_signals_and_close()
    )
    wait_for_dispatcher_to_finish_task = asyncio.create_task(
        wait_for_worker_to_finish()
    )

    done, _ = await asyncio.wait(
        [wait_for_signals_and_close_task, wait_for_dispatcher_to_finish_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    # Re-raise exceptions from done tasks
    [task.result() for task in done]
    logger.info("Stopped worker at %r", worker.address)


if __name__ == "__main__":
    try:
        asyncio_run(run(), loop_factory=get_loop_factory())
    finally:
        logger.info("End worker")

