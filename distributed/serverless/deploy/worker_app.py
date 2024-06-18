import json
import logging

import dask
import tornado.ioloop
import tornado.web

from distributed.serverless.lazy_worker import LazyWorker

logger = logging.getLogger(__name__)


class WorkerHandler(tornado.web.RequestHandler):
    async def post(self):
        data = {}
        try:
            data = json.loads(self.request.body)
        except json.JSONDecodeError:
            self.set_status(400)
            self.write({"status": "error", "message": "Invalid JSON"})

        try:
            print(data)
            dask.config.set({"scheduler-address": data["scheduler_address"]})
            logger.info("-" * 47)
            worker = LazyWorker(
                # scheduler_ip="amqp://Scheduler-00000000-0000-0000-0000-000000000000:0",
                # scheduler_ip="ws://127.0.0.1",
                # scheduler_port=5555,
                nthreads=data["nthreads"],
                name=data["name"],
                contact_address=data["contact_address"],
                heartbeat_interval="5s",
                dashboard=False,
                # host="127.0.0.1",
                # port=8989,
                # protocol="tcp",
                nanny=None,
                validate=False,
                memory_limit=data["memory_limit"],
                connection_limit=1,
            )
            logger.info("-" * 47)

            print("Starting worker...")
            await worker
            await worker.finished()

            self.write({"status": "success", "data": data})
        except KeyError as e:
            self.set_status(400)
            self.write({"status": "error", "message": str(e)})
        except Exception as e:
            self.set_status(500)
            self.write({"status": "error", "message": str(e)})


class WorkerMetadataHandler(tornado.web.RequestHandler):
    versions = {
        'host': {'python': '3.11.4.final.0', 'python-bits': 64, 'OS': 'Linux', 'OS-release': '6.2.0-39-generic',
                 'machine': 'x86_64', 'processor': 'x86_64', 'byteorder': 'little', 'LC_ALL': 'None',
                 'LANG': 'en_US.UTF-8'},
        'packages': {'python': '3.11.4.final.0', 'dask': '2024.4.2', 'distributed': '0+untagged.5753.g32de245',
                     'msgpack': '1.0.8', 'cloudpickle': '3.0.0', 'tornado': '6.4', 'toolz': '0.12.1',
                     'numpy': None, 'pandas': None, 'lz4': None}}

    async def get(self):
        print(self.versions)
        self.write(self.versions)


if __name__ == "__main__":
    app = tornado.web.Application([
        (r"/worker", WorkerHandler),
        (r"/versions", WorkerMetadataHandler)
    ])
    app.listen(address="127.0.0.1", port=8080)
    print("Server is running on http://127.0.0.1:8080")
    tornado.ioloop.IOLoop.current().start()
