from __future__ import annotations

import json
import logging
import os
import textwrap
import time
from typing import Any

import dask
import tornado
from dask.typing import Key
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

from distributed import Status
from distributed import versions as version_module
from distributed.batched import BatchedSend
from distributed.client import SourceCode
from distributed.comm import Comm
from distributed.comm.addressing import addresses_from_user_args
from distributed.core import error_message
from distributed.counter import Counter
from distributed.node import ServerNode
from distributed.protocol import deserialize
from distributed.scheduler import ClientState, _materialize_graph
from distributed.serverless.ephemeral_scheduler import EphemeralScheduler
from distributed.utils import is_python_shutting_down, offload

logger = logging.getLogger(__name__)

WORKERS_ENDPOINT = os.environ.get("WORKERS_ENDPOINT", "http://127.0.0.1:8080")


async def deploy_workers(http_client, payloads):
    async def _deploy_worker(url, payload):
        try:
            print(f"POST {url} --> {payload}")
            response = await http_client.fetch(url, method="POST", body=json.dumps(payload))
            return json.loads(response.body)
        except tornado.httpclient.HTTPClientError as e:
            print(f"Error fetching {url}: {e}")
        except json.JSONDecodeError as e:
            print(f"Error decoding response from {url}: {e}")

    url = WORKERS_ENDPOINT + "/worker"
    fetch_futures = [_deploy_worker(url, payload) for payload in payloads]
    responses = await tornado.gen.multi(fetch_futures)
    for payload, response in zip(payloads, responses):
        if response:
            print(f"Response {payload} --> {response}")
        else:
            print(f"No response from {url}")


class SchedulerDispatcher(ServerNode):
    default_port = 8788
    schedulers = {}
    client_schedulers = {}
    client_comms = {}
    clients = {}

    AsyncHTTPClient.configure("tornado.simple_httpclient.SimpleAsyncHTTPClient",
                              max_clients=1000,
                              defaults={"connect_timeout": 0, "request_timeout": 0})
    http_client = AsyncHTTPClient()

    def __init__(self, host=None, port=None, interface=None, protocol=None):
        handlers = {
            "register-client": self.register_client,
            "gather": self.gather,
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
        logger.debug("--- Start update_graph ---")

        # logger.debug("Update graph "
        #              "- Client: %s "
        #              "- Graph Header: %s "
        #              "- Keys: %s "
        #              "- Priority: %s "
        #              "- Submitting Task: %s "
        #              "- User Priority: %s "
        #              "- Actors: %s "
        #              "- FIFO Timeout: %s "
        #              "- Code: %s "
        #              "- Annotations: %s "
        #              "- Stimulus ID: %s",
        #              client, graph_header, keys, internal_priority, submitting_task, user_priority, actors,
        #              fifo_timeout, code, annotations, stimulus_id)
        start = time.time()
        req_uuid = client.replace("Client-", "")
        scheduler_id = f"Scheduler-{req_uuid}"
        logger.info("Bootstrap scheduler for %s", scheduler_id)
        cs = self.clients[client]
        try:
            t0 = time.perf_counter()

            # Get versions form worker here asynchronously
            # versions_req = HTTPRequest(url=WORKERS_ENDPOINT + "/versions", method="GET")
            # versions_res_fut = self.http_client.fetch(versions_req)

            # TODO Setup these parameters based on num of CPUs and worker specs
            nworkers = int(os.environ.get("N_WORKERS", 160))
            nthreads = int(os.environ.get("N_THREADS", 1))
            memory_limit = int(os.environ.get("MEMORY_LIMIT", 2147483648))  # 2GB
            logger.info("Going to deploy %d workers with %d threads and %d memory limit",
                        nworkers, nthreads, memory_limit)

            worker_payloads = []
            for i in range(nworkers):
                worker_id = f"Worker-{i}-{req_uuid}"
                payload = {
                    "name": worker_id,
                    "scheduler_address": f"amqp://{scheduler_id}:0",
                    "nthreads": nthreads,
                    "memory_limit": memory_limit,
                    "contact_address": f"amqp://{worker_id}:0"
                }
                worker_payloads.append(payload)

            # tornado.ioloop.IOLoop.current().add_callback(deploy_workers, self.http_client, worker_payloads)

            # Bootstrap scheduler for this DAG run
            scheduler = EphemeralScheduler(
                client=cs,
                client_comm=self.client_comms[client],
                host=scheduler_id,
                port=0,
                protocol="amqp",
                dashboard=False,
            )
            scheduler.client_comms[client] = self.client_comms[client]
            self.schedulers[scheduler_id] = scheduler
            self.client_schedulers[client] = scheduler
            await scheduler

            # Materialize DAG
            # We do not call Scheduler.update_graph directly because we want to have the DAG here
            # in order to calculate the number of workers needed for this DAG
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

            # versions_res = await versions_res_fut
            # versions = json.loads(versions_res.body)
            # logger.info("Versions: %s", versions)

            versions = {
                'host': {'python': '3.11.4.final.0', 'python-bits': 64, 'OS': 'Linux', 'OS-release': '6.2.0-39-generic',
                         'machine': 'x86_64', 'processor': 'x86_64', 'byteorder': 'little', 'LC_ALL': 'None',
                         'LANG': 'en_US.UTF-8'},
                'packages': {'python': '3.11.4.final.0', 'dask': '2024.4.2', 'distributed': '0+untagged.5753.g32de245',
                             'msgpack': '1.0.8', 'cloudpickle': '3.0.0', 'tornado': '6.4', 'toolz': '0.12.1',
                             'numpy': None, 'pandas': None, 'lz4': None}}
            heartbeat_metrics = {'task_counts': Counter(),
                                 'bandwidth': {'total': 100000000, 'workers': {}, 'types': {}},
                                 'digests_total_since_heartbeat': {'tick-duration': 0.0,
                                                                   'latency': 0.0}, 'managed_bytes': 0,
                                 'spilled_bytes': {'memory': 0, 'disk': 0},
                                 'transfer': {'incoming_bytes': 0, 'incoming_count': 0, 'incoming_count_total': 0,
                                              'outgoing_bytes': 0, 'outgoing_count': 0, 'outgoing_count_total': 0},
                                 'event_loop_interval': 0.0, 'cpu': 0.0, 'memory': 0,
                                 'time': time.time(),
                                 'host_net_io': {'read_bps': 0, 'write_bps': 0},
                                 'host_disk_io': {'read_bps': 0.0, 'write_bps': 0.0}, 'num_fds': 0}

            # Add WorkerState to scheduler
            # scheduler.bootstrap_workers(nworkers, nthreads, memory_limit, versions, heartbeat_metrics)

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

            # Add WorkerState to scheduler
            scheduler.bootstrap_workers(nworkers, nthreads, memory_limit, versions, heartbeat_metrics)

            await scheduler.finished()
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

    async def gather(self, keys, serializers=None):
        for scheduler in self.schedulers.values():
            res = await scheduler.gather(keys, serializers=serializers)
            if res["status"] == "OK":
                return res
            else:
                pass

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
