import asyncio
import logging
import weakref
from concurrent.futures import Executor
from typing import Any, Literal, Mapping, Callable, Collection

import dask
from dask.utils import format_bytes
from tornado.ioloop import IOLoop

from distributed import Worker, Security, Nanny, WorkerPlugin, connect
from distributed.comm.addressing import get_address_host
from distributed.core import Server, Status
from distributed.http import get_handlers
from distributed.proctitle import setproctitle
from distributed.utils import parse_ports
from distributed.utils_perf import enable_gc_diagnosis
from distributed.worker import DEFAULT_METRICS, DEFAULT_STARTUP_INFORMATION
from distributed.worker_memory import WorkerDataParameter

logger = logging.getLogger(__name__)


class LazyWorker(Worker):
    def __init__(self,
                 scheduler_ip: str | None = None,
                 scheduler_port: int | None = None, *,
                 scheduler_file: str | None = None,
                 nthreads: int | None = None,
                 loop: IOLoop | None = None,
                 local_directory: str | None = None,
                 services: dict | None = None,
                 name: Any | None = None,
                 reconnect: bool | None = None,
                 executor: Executor | dict[str, Executor] | Literal["offload"] | None = None,
                 resources: dict[str, float] | None = None,
                 silence_logs: int | None = None,
                 death_timeout: Any | None = None,
                 preload: list[str] | None = None,
                 preload_argv: list[str] | list[list[str]] | None = None,
                 security: Security | dict[str, Any] | None = None,
                 contact_address: str | None = None,
                 heartbeat_interval: Any = "25s",
                 extensions: dict[str, type] | None = None,
                 metrics: Mapping[str, Callable[[Worker], Any]] = DEFAULT_METRICS,
                 startup_information: Mapping[str, Callable[[Worker], Any]] = DEFAULT_STARTUP_INFORMATION,
                 interface: str | None = None,
                 host: str | None = None,
                 port: int | str | Collection[int] | None = None,
                 protocol: str | None = None,
                 dashboard_address: str | None = None,
                 dashboard: bool = False,
                 http_prefix: str = "/",
                 nanny: Nanny | None = None,
                 plugins: tuple[WorkerPlugin, ...] = (),
                 low_level_profiler: bool | None = None,
                 validate: bool | None = None,
                 profile_cycle_interval=None,
                 lifetime: Any | None = None,
                 lifetime_stagger: Any | None = None,
                 lifetime_restart: bool | None = None,
                 transition_counter_max: int | Literal[False] = False,
                 memory_limit: str | float = "auto",
                 data: WorkerDataParameter = None,
                 memory_target_fraction: float | Literal[False] | None = None,
                 memory_spill_fraction: float | Literal[False] | None = None,
                 memory_pause_fraction: float | Literal[False] | None = None,
                 scheduler_sni: str | None = None,
                 **kwargs):
        super().__init__(scheduler_ip, scheduler_port, scheduler_file=scheduler_file, nthreads=nthreads, loop=loop,
                         local_directory=local_directory, services=services, name=name, reconnect=reconnect,
                         executor=executor, resources=resources, silence_logs=silence_logs, death_timeout=death_timeout,
                         preload=preload, preload_argv=preload_argv, security=security, contact_address=contact_address,
                         heartbeat_interval=heartbeat_interval, extensions=extensions, metrics=metrics,
                         startup_information=startup_information, interface=interface, host=host, port=port,
                         protocol=protocol, dashboard_address=dashboard_address, dashboard=dashboard,
                         http_prefix=http_prefix, nanny=nanny, plugins=plugins, low_level_profiler=low_level_profiler,
                         validate=validate, profile_cycle_interval=profile_cycle_interval, lifetime=lifetime,
                         lifetime_stagger=lifetime_stagger, lifetime_restart=lifetime_restart,
                         transition_counter_max=transition_counter_max, memory_limit=memory_limit, data=data,
                         memory_target_fraction=memory_target_fraction, memory_spill_fraction=memory_spill_fraction,
                         memory_pause_fraction=memory_pause_fraction, scheduler_sni=scheduler_sni, **kwargs)

    async def start_unsafe(self):
        logger.debug("---------------- LazyWorker.start_unsafe -----------------")

        await super(Worker, self).start_unsafe()
        enable_gc_diagnosis()

        kwargs = self.security.get_listen_args("worker")
        await self.listen(self.contact_address, **kwargs)

        # Start HTTP server associated with this Worker node
        routes = get_handlers(
            server=self,
            modules=dask.config.get("distributed.worker.http.routes"),
            prefix=self._http_prefix,
        )
        self.start_http_server(routes, self._dashboard_address)
        if self._dashboard:
            try:
                import distributed.dashboard.worker
            except ImportError:
                logger.debug("To start diagnostics web server please install Bokeh")
            else:
                distributed.dashboard.worker.connect(
                    self.http_application,
                    self.http_server,
                    self,
                    prefix=self._http_prefix,
                )
        self.ip = get_address_host(self.address)

        if self.name is None:
            self.name = self.address

        await self.preloads.start()

        # Services listen on all addresses
        # Note Nanny is not a "real" service, just some metadata
        # passed in service_ports...
        self.start_services(self.ip)

        try:
            listening_address = "%s%s:%d" % (self.listener.prefix, self.ip, self.port)
        except Exception:
            listening_address = f"{self.listener.prefix}{self.ip}"

        logger.info("      Start worker at: %26s", self.address)
        logger.info("         Listening to: %26s", listening_address)
        if self.name != self.address_safe:
            # only if name was not None
            logger.info("          Worker name: %26s", self.name)
        for k, v in self.service_ports.items():
            logger.info("  {:>16} at: {:>26}".format(k, self.ip + ":" + str(v)))
        logger.info("Waiting to connect to: %26s", self.scheduler.address)
        logger.info("-" * 49)
        logger.info("              Threads: %26d", self.state.nthreads)
        if self.memory_manager.memory_limit:
            logger.info(
                "               Memory: %26s",
                format_bytes(self.memory_manager.memory_limit),
            )
        logger.info("      Local Directory: %26s", self.local_directory)

        setproctitle("dask worker [%s]" % self.address)

        plugins_msgs = await asyncio.gather(
            *(
                self.plugin_add(plugin=plugin, catch_errors=False)
                for plugin in self._pending_plugins
            ),
            return_exceptions=True,
        )
        plugins_exceptions = [msg for msg in plugins_msgs if isinstance(msg, Exception)]
        if len(plugins_exceptions) >= 1:
            if len(plugins_exceptions) > 1:
                logger.error(
                    "Multiple plugin exceptions raised. All exceptions will be logged, the first is raised."
                )
                for exc in plugins_exceptions:
                    logger.error(repr(exc))
            raise plugins_exceptions[0]

        self._pending_plugins = ()
        self.state.address = self.address
        # await self._register_with_scheduler()

        # Create comm to scheduler
        comm = await connect(self.scheduler.address, **self.connection_args)
        comm.name = "Worker->Scheduler"
        # await comm.write({"op": "connection_stream", "reply": False})
        await comm.write({"op": "connection_stream", "extra": {"worker": self.address}, "reply": False})

        comm._server = weakref.ref(self)
        self.batched_stream.start(comm)
        # self.status = Status.running
        self.loop.add_callback(self.handle_scheduler, comm)

        logger.info("        Registered to: %26s", self.scheduler.address)
        logger.info("-" * 49)

        self.start_periodic_callbacks()
        logger.debug("---------------- LazyWorker.start_unsafe -----------------")
        return self
