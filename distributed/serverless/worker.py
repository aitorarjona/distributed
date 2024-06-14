import asyncio
import logging
import weakref

import dask
from dask.utils import format_bytes

from distributed import Worker, Status, connect
from distributed.comm.addressing import address_from_user_args, get_address_host
from distributed.http import get_handlers
from distributed.proctitle import setproctitle
from distributed.utils import parse_ports
from distributed.utils_perf import enable_gc_diagnosis

logger = logging.getLogger(__name__)


async def bootstrap_worker(worker: Worker):
    await worker.rpc.start()

    enable_gc_diagnosis()

    kwargs = worker.security.get_listen_args("worker")
    try:
        await worker.listen(worker.contact_address, **kwargs)
    except OSError as e:
        raise
    worker._start_address = worker.contact_address

    # Start HTTP server associated with this Worker node
    routes = get_handlers(
        server=worker,
        modules=dask.config.get("distributed.worker.http.routes"),
        prefix=worker._http_prefix,
    )
    worker.start_http_server(routes, worker._dashboard_address)
    worker.ip = get_address_host(worker.address)

    if worker.name is None:
        worker.name = worker.address

    await worker.preloads.start()

    # Services listen on all addresses
    # Note Nanny is not a "real" service, just some metadata
    # passed in service_ports...
    worker.start_services(worker.ip)

    try:
        listening_address = "%s%s:%d" % (worker.listener.prefix, worker.ip, worker.port)
    except Exception:
        listening_address = f"{worker.listener.prefix}{worker.ip}"

    logger.info("      Start worker at: %26s", worker.address)
    logger.info("         Listening to: %26s", listening_address)
    if worker.name != worker.address_safe:
        # only if name was not None
        logger.info("          Worker name: %26s", worker.name)
    for k, v in worker.service_ports.items():
        logger.info("  {:>16} at: {:>26}".format(k, worker.ip + ":" + str(v)))
    logger.info("Waiting to connect to: %26s", worker.scheduler.address)
    logger.info("-" * 49)
    logger.info("              Threads: %26d", worker.state.nthreads)
    if worker.memory_manager.memory_limit:
        logger.info(
            "               Memory: %26s",
            format_bytes(worker.memory_manager.memory_limit),
        )
    logger.info("      Local Directory: %26s", worker.local_directory)

    setproctitle("dask worker [%s]" % worker.address)

    plugins_msgs = await asyncio.gather(
        *(
            worker.plugin_add(plugin=plugin, catch_errors=False)
            for plugin in worker._pending_plugins
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

    worker._pending_plugins = ()
    worker.state.address = worker.address
    # await worker._register_with_scheduler()
    worker.start_periodic_callbacks()
    
    # Create comm to scheduler
    # comm = await connect(worker.scheduler.address, **worker.connection_args)
    # comm.name = "Worker->Scheduler"
    # comm._server = weakref.ref(worker)
    # worker.batched_stream.start(comm)
    # worker.status = Status.running
    #
    # logger.info("        Registered to: %26s", worker.scheduler.address)
    # logger.info("-" * 49)
    #
    # worker.periodic_callbacks["keep-alive"].start()
    # worker.periodic_callbacks["heartbeat"].start()
    # worker.loop.add_callback(worker.handle_scheduler, comm)
