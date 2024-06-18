from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from distributed import Scheduler, Status, connect
from distributed.batched import BatchedSend
from distributed.comm import Comm
from distributed.scheduler import ClientState, WorkerState

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.10)
    # TODO import from typing (requires Python >=3.11)
    from typing_extensions import Self

logger = logging.getLogger(__name__)


class EphemeralScheduler(Scheduler):
    def __init__(self,
                 client: ClientState,
                 client_comm: Comm,
                 **kwargs):
        super().__init__(**kwargs)

        self.clients[client.client_key] = client
        self.client_comms[client] = client_comm
        self.id = client.client_key.replace("Client", "Scheduler")

    async def bootstrap_workers(self, n_workers, nthreads, memory_limit, versions, metrics):
        start = time.time()

        for i in range(n_workers):
            name = self.id.replace("Scheduler", f"Worker-{i}")
            address = "amqp://" + name + ":0"

            ws = WorkerState(
                address=address,
                status=Status.running,
                pid=0,
                name=name,
                nthreads=nthreads,
                memory_limit=memory_limit,
                local_directory=f"/tmp/dask-worker-space/{name}",
                nanny=None,
                server_id=name,
                services={},
                versions=versions,
                extra={},
                scheduler=self,
            )

            self.workers[address] = ws
            self.aliases[name] = name
            self.running.add(ws)
            self.total_nthreads += ws.nthreads

            # "Fake" worker heartbeat, needed to initialize some values in WorkerState
            self.heartbeat_worker(
                address=address,
                resolve_address=False,
                resources=None,
                host_info=None,
                metrics=metrics,
                executing={},
                extensions={},
            )

            # The comm object is not ready yet -- It's a LazyWorker, and it is not yet started, so it
            # cannot do the handshake with the scheduler. We need to delay the connection using
            # _delay_worker_conn. With BatchedSend, the scheduler will be able to
            # enqueue messages to the worker even if it's not established yet,
            # the actual messages which will be sent as soon as the connection is established
            bs = BatchedSend(interval="5ms", loop=self.loop)
            self.stream_comms[address] = bs
            self.loop.add_callback(self._delay_worker_conn, address)

            # Update idle state of the new worker
            self.check_idle_saturated(ws)

        self.total_nthreads_history.append((time.time(), self.total_nthreads))
        # Notify scheduler that the workers are available to process tasks
        self.stimulus_queue_slots_maybe_opened(stimulus_id=f"bootstrap-workers-{start}")

    async def _delay_worker_conn(self, address):
        logger.debug("Starting delayed worker connection to %s", address)
        comm = await connect(address)
        logger.debug("Delayed worker connection to %s successful 🤙", address)

        # Convert this to a stream connection
        await comm.write({"op": "connection_stream", "reply": False})

        batched_send = self.stream_comms[address]
        batched_send.start(comm)

        # This will keep running until the worker is removed
        await self.handle_worker(comm, address)
        logger.debug("Miau", address)
