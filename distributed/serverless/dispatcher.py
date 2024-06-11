from __future__ import annotations

import logging
import textwrap
import time
from typing import Any

import dask
from dask.typing import Key

from distributed import Status, connect
from distributed import versions as version_module
from distributed.batched import BatchedSend
from distributed.client import SourceCode
from distributed.comm import Comm
from distributed.comm.addressing import addresses_from_user_args
from distributed.core import error_message
from distributed.node import ServerNode
from distributed.protocol import deserialize
from distributed.scheduler import ClientState, _materialize_graph, WorkerState, Scheduler
from distributed.utils import is_python_shutting_down, offload

logger = logging.getLogger(__name__)


async def _delay_worker_conn(scheduler, address):
    logger.debug("Starting delayed worker connection to %s", address)
    print(address)
    comm = await connect(address)
    # This will keep running until the worker is removed
    await scheduler.handle_worker(comm, address)


class SchedulerDispatcher(ServerNode):
    default_port = 8788

    def __init__(self, host=None, port=None, interface=None, protocol=None):
        self.client_comms = {}
        self.clients = {}

        handlers = {
            "register-client": self.register_client,
        }

        stream_handlers = {
            "update-graph": self.update_graph,
            # "client-desires-keys": self.client_desires_keys,
            # "update-data": self.update_data,
            # "report-key": self.report_on_key,
            # "client-releases-keys": self.client_releases_keys,
            # "heartbeat-client": self.client_heartbeat,
            "close-client": self.remove_client,
            "subscribe-topic": self.subscribe_topic,
            # "unsubscribe-topic": self.unsubscribe_topic,
            # "cancel-keys": self.stimulus_cancel,
        }

        self.connection_args = {"handshake_overrides": {  # common denominator
            "pickle-protocol": 4
        }}

        addresses = addresses_from_user_args(
            host=host,
            port=port,
            interface=interface,
            protocol=protocol,
            security=None,
            default_port=self.default_port,
        )
        assert len(addresses) == 1
        self._start_address = addresses.pop()

        ServerNode.__init__(self, handlers=handlers, stream_handlers=stream_handlers)

    # ----------------
    # Handling clients
    # ----------------

    async def register_client(self, comm: Comm, client: str, versions: dict[str, Any]):
        assert client is not None
        comm.name = "Scheduler->Client"
        logger.info("Receive client connection: %s", client)
        # self.log_event(["all", client], {"action": "add-client", "client": client})
        self.clients[client] = ClientState(client, versions=versions)

        # for plugin in list(self.plugins.values()):
        #     try:
        #         plugin.add_client(scheduler=self, client=client)
        #     except Exception as e:
        #         logger.exception(e)

        try:
            bcomm = BatchedSend(interval="2ms", loop=self.loop)
            bcomm.start(comm)
            self.client_comms[client] = bcomm
            msg = {"op": "stream-start"}
            version_warning = version_module.error_message(
                version_module.get_versions(),
                {},
                versions,
            )
            msg.update(version_warning)
            bcomm.send(msg)

            try:
                await self.handle_stream(comm=comm, extra={"client": client})
            finally:
                self.remove_client(client=client, stimulus_id=f"remove-client-{time.time()}")
                logger.debug("Finished handling client %s", client)
        finally:
            if not comm.closed():
                self.client_comms[client].send({"op": "stream-closed"})
            try:
                if not is_python_shutting_down():
                    await self.client_comms[client].close()
                    del self.client_comms[client]
                    if self.status == Status.running:
                        logger.info("Close client connection: %s", client)
            except TypeError:  # comm becomes None during GC
                pass

    def remove_client(self, client: str, stimulus_id: str | None = None) -> None:
        """Remove client from network"""
        stimulus_id = stimulus_id or f"remove-client-{time()}"
        if self.status == Status.running:
            logger.info("Remove client %s", client)
        # self.log_event(["all", client], {"action": "remove-client", "client": client})
        try:
            cs: ClientState = self.clients[client]
        except KeyError:
            # XXX is this a legitimate condition?
            pass
        else:
            # self.client_releases_keys(
            #     keys=[ts.key for ts in cs.wants_what],
            #     client=cs.client_key,
            #     stimulus_id=stimulus_id,
            # )
            del self.clients[client]

            # for plugin in list(self.plugins.values()):
            #     try:
            #         plugin.remove_client(scheduler=self, client=client)
            #     except Exception as e:
            #         logger.exception(e)

    async def subscribe_topic(self, topic: str, client: str):
        logger.info("Client %s topic subscription: %s", client, topic)

    # ---------------------
    # Graph handling
    # ---------------------

    async def update_graph(
            self,
            client: str,
            graph_header: dict,
            graph_frames: list[bytes],
            keys: set[Key],
            internal_priority: dict[Key, int] | None,
            submitting_task: Key | None,
            user_priority: int | dict[Key, int] = 0,
            actors: bool | list[Key] | None = None,
            fifo_timeout: float = 0.0,
            code: tuple[SourceCode, ...] = (),
            annotations: dict | None = None,
            stimulus_id: str | None = None,
    ):
        logger.debug("Update graph "
                     "- Client: %s "
                     "- Graph Header: %s "
                     "- Keys: %s "
                     "- Priority: %s "
                     "- Submitting Task: %s "
                     "- User Priority: %s "
                     "- Actors: %s "
                     "- FIFO Timeout: %s "
                     "- Code: %s "
                     "- Annotations: %s "
                     "- Stimulus ID: %s",
                     client, graph_header, keys, internal_priority, submitting_task, user_priority, actors,
                     fifo_timeout, code, annotations, stimulus_id)
        start = time.time()
        scheduler_id = client.replace("Client", "Scheduler")
        logger.info("Bootstrap scheduler for %s", scheduler_id)
        try:
            t0 = time.perf_counter()
            try:
                graph = deserialize(graph_header, graph_frames).data
                del graph_header, graph_frames
            except Exception as e:
                msg = """\
                    Error during deserialization of the task graph. This frequently
                    occurs if the Scheduler and Client have different environments.
                    For more information, see
                    https://docs.dask.org/en/stable/deployment-considerations.html#consistent-software-environments
                """
                raise RuntimeError(textwrap.dedent(msg)) from e
            (
                dsk,
                dependencies,
                annotations_by_type,
            ) = await offload(
                _materialize_graph,
                graph=graph,
                global_annotations=annotations or {},
            )
            del graph
            if not internal_priority:
                # Removing all non-local keys before calling order()
                dsk_keys = set(
                    dsk
                )  # intersection() of sets is much faster than dict_keys
                stripped_deps = {
                    k: v.intersection(dsk_keys)
                    for k, v in dependencies.items()
                    if k in dsk_keys
                }
                internal_priority = await offload(
                    dask.order.order, dsk=dsk, dependencies=stripped_deps
                )

            # Bootstrap scheduler for this DAG run
            scheduler = Scheduler(
                host=scheduler_id,
                port=0,
                protocol="amqp",
                dashboard=False,
            )

            # Register client to scheduler
            scheduler.clients[client] = ClientState(client, versions=None)
            scheduler.client_comms[client] = self.client_comms[client]

            # Enqueue tasks to scheduler
            scheduler._create_taskstate_from_graph(
                dsk=dsk,
                client=client,
                dependencies=dependencies,
                keys=set(keys),
                ordered=internal_priority or {},
                submitting_task=submitting_task,
                user_priority=user_priority,
                actors=actors,
                fifo_timeout=fifo_timeout,
                code=code,
                annotations_by_type=annotations_by_type,
                # FIXME: This is just used to attach to Computation
                # objects. This should be removed
                global_annotations=annotations,
                start=start,
                stimulus_id=stimulus_id or f"update-graph-{start}",
            )
            t1 = time.perf_counter()
            logger.debug("Scheduler bootstrap took %.6f", t1 - t0)

            # TODO Compute number of workers based on the dag
            n_workers = 1

            # Bootstrap workers
            for i in range(n_workers):
                name = client.replace("Client", f"Worker-{i}")
                address = "amqp://" + ".".join([name, "dask", "serverless"]) + ":0"

                nthreads = 1  # TODO Compute number of threads for this worker

                ws = WorkerState(
                    address=address,
                    status=Status.running,
                    pid=0,
                    nthreads=nthreads,
                    memory_limit=99999999,
                    name=name,
                    scheduler=scheduler,
                    nanny=None,
                    local_directory=None,
                    server_id=None,
                )

                scheduler.workers[name] = ws
                scheduler.aliases[name] = name
                scheduler.running.add(ws)
                scheduler.total_nthreads += ws.nthreads

                # The comm object is not ready yet, it will be created by the task
                # _delay_worker_conn, but with BatchedSend, the scheduler will be able to
                # enqueue messages to the worker, which will be sent as soon as the connection is ready
                scheduler.stream_comms[address] = BatchedSend(interval="5ms", loop=self.loop)
                self.loop.add_callback(_delay_worker_conn, scheduler, address)

            logger.debug("Scheduler bootstrap for %s complete", scheduler_id)

            scheduler.total_nthreads_history.append((time.time(), scheduler.total_nthreads))
            # Notify scheduler that the workers are available to process tasks
            scheduler.stimulus_queue_slots_maybe_opened(stimulus_id=stimulus_id or f"update-graph-{start}")
            logger.debug("--- End update_graph ---")
        except RuntimeError as e:
            logger.error(str(e))
            err = error_message(e)
            for key in keys:
                self.report(
                    {
                        "op": "task-erred",
                        "key": key,
                        "exception": err["exception"],
                        "traceback": err["traceback"],
                    },
                    # This informs all clients in who_wants plus the current client
                    # (which may not have been added to who_wants yet)
                    client=client,
                )
        end = time.time()
        self.digest_metric("update-graph-duration", end - start)

    # ---------------------
    # Dispatcher management
    # ---------------------

    async def start_unsafe(self):
        await self.listen(
            self._start_address,
            allow_offload=False,
            handshake_overrides={"pickle-protocol": 4, "compression": None},
        )

        for listener in self.listeners:
            logger.info("Dispatcher listening at: %25s", listener.contact_address)

        return self
