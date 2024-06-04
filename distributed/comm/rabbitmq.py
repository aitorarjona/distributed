from __future__ import annotations

import logging
import struct

import dask
from aio_pika import ExchangeType, connect, Message
from aio_pika.abc import AbstractIncomingMessage
from tornado.queues import Queue

from distributed.comm.core import (
    BaseListener,
    Comm,
    Connector, CommClosedError,
)
from distributed.comm.registry import Backend, backends
from distributed.comm.utils import from_frames, to_frames

logger = logging.getLogger(__name__)

BIG_BYTES_SHARD_SIZE = dask.utils.parse_bytes(
    dask.config.get("distributed.comm.rabbitmq.shard")
)
EXCHANGE = dask.config.get("distributed.comm.rabbitmq.exchange")


class RabbitMQ(Comm):
    def __init__(self, consumer_queue, publisher_routing_key, exchange, deserialize):
        self.buffer = Queue()
        self._consumer_queue = consumer_queue
        self._publisher_routing_key = publisher_routing_key
        self._exchange = exchange
        Comm.__init__(self, deserialize)

    def __await__(self):
        return self._consumer_queue.bind(self._on_message)

    async def read(self, deserializers=None):
        try:
            n_frames = await self.buffer.get()
            if n_frames is None:
                # Connection is closed
                self.abort()
                raise CommClosedError()
            n_frames = struct.unpack("Q", n_frames)[0]
        except Exception as e:
            raise CommClosedError(e)

        frames = [(await self.buffer.get()) for _ in range(n_frames)]

        msg = await from_frames(
            frames,
            deserialize=self.deserialize,
            deserializers=deserializers,
            allow_offload=self.allow_offload,
        )
        return msg

    async def write(self, msg, serializers=None, on_error=None):
        frames = await to_frames(
            msg,
            allow_offload=self.allow_offload,
            serializers=serializers,
            on_error=on_error,
            context={
                "sender": self.local_info,
                "recipient": self.remote_info,
                **self.handshake_options,
            },
            frame_split_size=BIG_BYTES_SHARD_SIZE,
        )
        n = struct.pack("Q", len(frames))
        nbytes_frames = 0
        try:
            message = Message(body=n)
            await self._exchange.publish(message, routing_key=self._publisher_routing_key)
            for frame in frames:
                if type(frame) is not bytes:
                    frame = bytes(frame)
                message = Message(body=frame)
                await self._exchange.publish(message, routing_key=self._publisher_routing_key)
                nbytes_frames += len(frame)
        except Exception as e:
            raise CommClosedError(e)

        return nbytes_frames

    async def close(self):
        raise NotImplementedError()

    def abort(self):
        raise NotImplementedError()

    def closed(self):
        raise NotImplementedError()

    @property
    def local_address(self) -> str:
        raise NotImplementedError()

    @property
    def peer_address(self) -> str:
        raise NotImplementedError()

    async def _on_message(self, message: AbstractIncomingMessage):
        async with message.process():
            print(f"Received message: {message.body!r}")
            await self.buffer.put(message.body)


class RabbitMQListener(BaseListener):
    def __init__(self, loc, handle_comm, deserialize, **connection_args):
        self._listener_queue = None
        self._consumer_tag = None
        self._exchange = None
        self._channel = None
        self._connection = None
        self._loc = loc
        self._comm_handler = handle_comm
        self._deserialize = deserialize
        self._connection_args = connection_args

        BaseListener.__init__(self)

    async def start(self):
        amqp_url = dask.config.get("distributed.comm.rabbitmq.amqp_url")
        self._connection = await connect(amqp_url)
        self._channel = await self._connection.channel()
        # await channel.set_qos(prefetch_count=1)

        self._exchange = await self._channel.declare_exchange(
            EXCHANGE, ExchangeType.DIRECT,
        )

        self._listener_queue = await self._channel.declare_queue(self._loc, exclusive=True, durable=False,
                                                                 auto_delete=True)

        await self._listener_queue.bind(self._exchange)
        self._consumer_tag = await self._listener_queue.consume(self._on_message)

    def stop(self):
        raise NotImplementedError()

    @property
    def listen_address(self):
        return self._loc

    @property
    def contact_address(self):
        return self._loc

    async def _on_message(self, message: AbstractIncomingMessage):
        async with message.process():
            logger.debug(f"Incoming connection from {message.reply_to!r} ==> {message.body!r}")

            consumer_queue_name = "dask-server"
            consumer_queue = await self._channel.declare_queue(consumer_queue_name, durable=False,
                                                               exclusive=True, auto_delete=True)
            await consumer_queue.bind(self._exchange)
            message = Message(reply_to=consumer_queue_name, body=b"")
            self._exchange.publish(message, routing_key=message.reply_to)

        comm = RabbitMQ(consumer_queue, message.reply_to, self._exchange, self._deserialize)
        await comm
        await self._comm_handler(comm)


class RabbitMQConnector(Connector):
    async def connect(self, address, deserialize=True):
        logger.debug(f"Connecting to {address}")
        amqp_url = dask.config.get("distributed.comm.rabbitmq.amqp_url")
        connection = await connect(amqp_url)
        channel = await connection.channel()
        # await channel.set_qos(prefetch_count=1)

        exchange = await channel.declare_exchange(
            EXCHANGE, ExchangeType.DIRECT,
        )

        # Create a queue for the client to listen to
        reply_to = "dask-client"
        queue = await channel.declare_queue(reply_to, exclusive=True, durable=False, auto_delete=True)
        await queue.bind(exchange)

        # Send a message to the server with the reply_to queue
        msg = Message(reply_to=reply_to, body=b"")
        await exchange.publish(msg, routing_key=address)

        comm = RabbitMQ(queue, address, exchange, deserialize)
        return comm


class RabbitMQBackend(Backend):
    def get_connector(self):
        return RabbitMQConnector()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return RabbitMQListener(loc, handle_comm, deserialize, **connection_args)

    def get_address_host(self, loc):
        raise NotImplementedError()

    def resolve_address(self, loc):
        raise NotImplementedError()

    def get_local_address_for(self, loc):
        raise NotImplementedError()


backends["amqp"] = RabbitMQBackend()
