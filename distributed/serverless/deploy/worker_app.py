import json
import logging
import os

import dask
import tornado.ioloop
import tornado.web

from distributed.versions import get_versions
from distributed.serverless.lazy_worker import LazyWorker

logger = logging.getLogger(__name__)

HOST = os.environ.get("HOST", "127.0.0.1")
PORT = os.environ.get("PORT", 8080)


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
    versions = None

    async def get(self):
        if self.versions is None:
            self.versions = get_versions()
        print(self.versions)
        self.write(self.versions)


if __name__ == "__main__":
    app = tornado.web.Application([
        (r"/worker", WorkerHandler),
        (r"/versions", WorkerMetadataHandler)
    ])
    app.listen(address=HOST, port=PORT)
    print(f"Server is running on http://{HOST}:{PORT}")
    tornado.ioloop.IOLoop.current().start()
