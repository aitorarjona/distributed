from __future__ import annotations

import logging
import struct

import aio_pika
import dask
from aio_pika import ExchangeType, Message
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
DURABLE = False
AUTO_DELETE = True
EXCLUSIVE = True


def address_to_queue_name(address: str):
    """Translate a "tcp-like address" address or uri (e.g. amqp://queuename:port) to a queue name, where the
    host is the queue name, the port is ignored"""
    if address.startswith("amqp://"):
        address = address.split("://")[1]
    if ":" in address:
        address = address.split(":")[0]
    return address


def queue_name_to_address(queue_name: str):
    """Translate a queue name to a "tcp-like address", where the queue name is the host,
     and the port is always 0 (e.g. amqp://queuename:0)"""
    return f"amqp://{queue_name}:0"


class RabbitMQ(Comm):
    """
    A RabbitMQ Comm to communicate two peers.
    For bidirectional communication, we create two queues: the consumer queue, in which this client will
    consume messages sent from the remote peer, and the publisher queue, in which the client will publish messages,
    which the remote peer will consume.
    """

    def __init__(self, client_queue, peer_routing_key, exchange, deserialize):
        self.buffer = Queue()
        self._client_queue = client_queue
        self._consumer_tag = None
        self._publisher_routing_key = peer_routing_key
        self._exchange = exchange
        Comm.__init__(self, deserialize)

    async def init(self):
        self._consumer_tag = await self._client_queue.consume(self._on_message)

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
        logger.debug("Sending message to queue %s --> %s", self._publisher_routing_key, str(msg))
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

        logger.debug("Sent header + %d messages to %s", len(frames), self._publisher_routing_key)
        return nbytes_frames

    async def close(self):
        raise NotImplementedError()

    def abort(self):
        raise NotImplementedError()

    def closed(self):
        raise NotImplementedError()

    @property
    def local_address(self) -> str:
        return queue_name_to_address(self._client_queue.name)

    @property
    def peer_address(self) -> str:
        return queue_name_to_address(self._publisher_routing_key)

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
        self._queue_name = loc
        self._comm_handler = handle_comm
        self._deserialize = deserialize
        self._connection_args = connection_args

        BaseListener.__init__(self)

    async def start(self):
        amqp_url = dask.config.get("distributed.comm.rabbitmq.amqp_url")
        self._connection = await aio_pika.connect(amqp_url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=10)

        self._exchange = await self._channel.declare_exchange(
            EXCHANGE, ExchangeType.DIRECT,
        )

        self._listener_queue = await self._channel.declare_queue(self._queue_name, exclusive=EXCLUSIVE, durable=DURABLE,
                                                                 auto_delete=AUTO_DELETE)

        await self._listener_queue.bind(self._exchange)
        self._consumer_tag = await self._listener_queue.consume(self._on_message)

    def stop(self):
        raise NotImplementedError()

    @property
    def listen_address(self):
        return queue_name_to_address(self._queue_name)

    @property
    def contact_address(self):
        return queue_name_to_address(self._queue_name)

    async def _on_message(self, message: AbstractIncomingMessage):
        async with message.process():
            logger.debug(f"Incoming connection from {message.reply_to!r} ==> {message.body!r}")

            consumer_queue_name = "dask-server"
            consumer_queue = await self._channel.declare_queue(consumer_queue_name, durable=DURABLE,
                                                               exclusive=EXCLUSIVE, auto_delete=AUTO_DELETE)
            await consumer_queue.bind(self._exchange)
            message = Message(reply_to=consumer_queue_name, body=b"")
            self._exchange.publish(message, routing_key=message.reply_to)

        comm = RabbitMQ(consumer_queue, message.reply_to, self._exchange, self._deserialize)
        await comm.init()
        await self.on_connection(comm)
        await self._comm_handler(comm)


class RabbitMQConnector(Connector):
    async def connect(self, address, deserialize=True):
        amqp_url = dask.config.get("distributed.comm.rabbitmq.amqp_url")
        connection = await aio_pika.connect(amqp_url)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=10)

        exchange = await channel.declare_exchange(
            EXCHANGE, ExchangeType.DIRECT,
        )

        # Declare and bind the peer queue
        peer_queue_name = address_to_queue_name(address)
        logger.debug(f"Connecting to queue {peer_queue_name}")
        peer_queue = await channel.declare_queue(peer_queue_name, durable=DURABLE, exclusive=EXCLUSIVE,
                                                 auto_delete=AUTO_DELETE)
        await peer_queue.bind(exchange)

        # Declare and bind a queue for the peer to reply to this client
        reply_to = peer_queue_name + "-reply"
        client_queue = await channel.declare_queue(reply_to, durable=DURABLE, exclusive=EXCLUSIVE,
                                                   auto_delete=AUTO_DELETE)
        await client_queue.bind(exchange)

        comm = RabbitMQ(client_queue, peer_queue_name, exchange, deserialize)
        await comm.init()

        # Send a message to the peer with the reply_to queue routing key
        msg = Message(reply_to=reply_to, body=b"")
        await exchange.publish(msg, routing_key=address)

        return comm


class RabbitMQBackend(Backend):
    def get_connector(self):
        return RabbitMQConnector()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return RabbitMQListener(loc, handle_comm, deserialize, **connection_args)

    def get_address_host(self, loc):
        return loc

    def resolve_address(self, loc):
        return loc

    def get_local_address_for(self, loc):
        return loc


backends["amqp"] = RabbitMQBackend()
