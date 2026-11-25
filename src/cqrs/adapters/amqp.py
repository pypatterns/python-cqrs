"""
AMQP publisher and consumer adapters.

``AMQPConsumer.consume`` takes an ``aio_pika`` message and blocks on
``asyncio.Future``. ``NeutralAMQPClient`` is a separate consumer for
``AMQPTransport``: ``close`` cancels it, and the handler receives
``AMQPDelivery`` instead of an ``aio_pika`` type.
"""

import asyncio
import logging
import typing
from functools import partial

from cqrs.adapters import protocol

import aio_pika
from aio_pika import abc, pool

logger = logging.getLogger("cqrs")


async def connection_pool_factory(url: str) -> abc.AbstractRobustConnection:
    return await aio_pika.connect_robust(url=url)


async def channel_pool_factory(
    connection_pool: pool.Pool[aio_pika.abc.AbstractConnection],
) -> aio_pika.abc.AbstractChannel:
    async with connection_pool.acquire() as connection:
        return await connection.channel()


class AMQPPublisher(protocol.AMQPPublisher):
    def __init__(self, channel_pool: pool.Pool[aio_pika.abc.AbstractChannel]):
        self.channel_pool = channel_pool

    async def publish(
        self,
        message: abc.AbstractMessage,
        queue_name: str,
        exchange_name: str,
    ) -> None:
        async with self.channel_pool.acquire() as channel:
            queue = await channel.declare_queue(queue_name)
            exchange = await channel.declare_exchange(
                exchange_name,
                type="direct",
                auto_delete=True,
            )
            await queue.bind(exchange=exchange, routing_key=queue_name)
            await exchange.publish(message=message, routing_key=queue_name)


class AMQPConsumer(protocol.AMQPConsumer):
    def __init__(self, channel_pool: pool.Pool[aio_pika.abc.AbstractChannel]):
        self.channel_pool = channel_pool

    async def consume(
        self,
        handler: typing.Callable[[abc.AbstractIncomingMessage], typing.Awaitable[None]],
        queue_name: str,
    ) -> None:
        async with self.channel_pool.acquire() as channel:
            await channel.set_qos(prefetch_count=1)
            queue = await channel.declare_queue(queue_name)
            await queue.consume(handler)
            await asyncio.Future()


def amqp_publisher_factory(
    url: typing.Text,
    max_connection_pool_size: int = 2,
    max_channel_pool_size: int = 10,
) -> AMQPPublisher:
    max_connection_pool_size = max_connection_pool_size
    max_channel_pool_size = max_channel_pool_size
    connection_pool = pool.Pool(
        partial(connection_pool_factory, url=url),
        max_size=max_connection_pool_size,
    )
    channel_pool = pool.Pool(
        partial(channel_pool_factory, connection_pool=connection_pool),
        max_size=max_channel_pool_size,
    )
    return AMQPPublisher(channel_pool=channel_pool)


def amqp_consumer_factory(
    url: typing.Text,
    max_connection_pool_size: int = 2,
    max_channel_pool_size: int = 10,
) -> AMQPConsumer:
    max_connection_pool_size = max_connection_pool_size
    max_channel_pool_size = max_channel_pool_size
    connection_pool = pool.Pool(
        partial(connection_pool_factory, url=url),
        max_size=max_connection_pool_size,
    )
    channel_pool = pool.Pool(
        partial(channel_pool_factory, connection_pool=connection_pool),
        max_size=max_channel_pool_size,
    )
    return AMQPConsumer(channel_pool=channel_pool)


def _header_text(value: typing.Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _copy_headers(headers: typing.Mapping[str, typing.Any] | None) -> dict[str, str]:
    if not headers:
        return {}
    return {str(key): _header_text(value) for key, value in headers.items()}


class AMQPDelivery:
    """One AMQP delivery. ``aio_pika`` stays behind this object."""

    def __init__(self, message: abc.AbstractIncomingMessage) -> None:
        self._message = message
        self._settled = False
        self.body = bytes(message.body)
        routing_key = message.routing_key or ""
        self.topic = routing_key if isinstance(routing_key, str) else str(routing_key)
        self.message_id = message.message_id
        content_type = message.content_type
        self.content_type = content_type if content_type else None
        self.headers = _copy_headers(message.headers)

    @property
    def settled(self) -> bool:
        return self._settled

    async def ack(self) -> None:
        if self._settled:
            return
        await self._message.ack()
        self._settled = True

    async def nack(self, *, requeue: bool = True) -> None:
        if self._settled:
            return
        await self._message.nack(requeue=requeue)
        self._settled = True


class AMQPConsumeSession:
    """A running ``NeutralAMQPClient.consume`` call. ``close`` cancels it."""

    def __init__(self) -> None:
        self._closed = False
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._queue: aio_pika.abc.AbstractQueue | None = None
        self._consumer_tag: typing.Any = None
        self._tasks: set[asyncio.Task[None]] = set()

    def attach(
        self,
        channel: aio_pika.abc.AbstractChannel,
        queue: aio_pika.abc.AbstractQueue,
        consumer_tag: typing.Any,
    ) -> None:
        self._channel = channel
        self._queue = queue
        self._consumer_tag = consumer_tag

    def watch(self, task: asyncio.Task[None]) -> None:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        pending = [task for task in self._tasks if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if self._queue is not None and self._consumer_tag is not None:
            try:
                await self._queue.cancel(self._consumer_tag)
            except Exception:
                logger.debug("AMQP consumer cancel failed", exc_info=True)
        if self._channel is not None and not self._channel.is_closed:
            try:
                await self._channel.close()
            except Exception:
                logger.debug("AMQP consume channel close failed", exc_info=True)


AMQPHandler = typing.Callable[[AMQPDelivery], typing.Awaitable[None]]


class NeutralAMQPClient:
    """
    One connection that publishes and consumes without a process-global bus.

    ``consume`` returns a session whose ``close`` cancels the consumer and
    the handlers it already started. The handler is given ``AMQPDelivery``.
    """

    def __init__(self, url: str) -> None:
        self._url = url
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._publish_channel: aio_pika.abc.AbstractChannel | None = None
        self._publish_lock = asyncio.Lock()
        self._connect_lock = asyncio.Lock()
        self._closed = False

    async def connect(self) -> None:
        if self._closed:
            return
        if self._connection is not None and not self._connection.is_closed:
            return
        async with self._connect_lock:
            if self._closed:
                return
            if self._connection is not None and not self._connection.is_closed:
                return
            connection = await aio_pika.connect_robust(self._url)
            if self._closed:
                await _close_connection(connection)
                return
            self._connection = connection
            self._publish_channel = None

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        connection = self._connection
        self._connection = None
        self._publish_channel = None
        if connection is not None and not connection.is_closed:
            await connection.close()

    async def publish(
        self,
        *,
        exchange_name: str,
        routing_key: str,
        body: bytes,
        headers: dict[str, str] | None,
        content_type: str | None,
        message_id: str,
    ) -> None:
        """Publish to a direct exchange. Does not declare the topic queue."""

        async with self._publish_lock:
            channel = await self._channel_for_publish()
            exchange = await _declare_direct(channel, exchange_name)
            kwargs: dict[str, typing.Any] = {"message_id": message_id}
            if content_type is not None:
                kwargs["content_type"] = content_type
            if headers:
                kwargs["headers"] = headers
            await exchange.publish(
                aio_pika.Message(body=body, **kwargs),
                routing_key=routing_key,
            )

    async def consume(
        self,
        handler: AMQPHandler,
        *,
        exchange_name: str,
        queue_name: str,
        routing_key: str,
        prefetch: int,
        dead_letter_routing_key: str | None = None,
        dead_letter_queue_name: str | None = None,
    ) -> AMQPConsumeSession:
        """
        Declare ``queue_name``, bind it to ``routing_key``, and consume.

        ``prefetch`` is the channel QoS. The callback awaits ``handler``,
        so this consumer does not run a second worker pool. When
        ``dead_letter_routing_key`` is set, ``nack(requeue=False)`` is the
        broker dead-letter exchange. The session is cancelled with
        ``AMQPConsumeSession.close``.
        """

        await self.connect()
        if self._connection is None:
            raise RuntimeError("AMQP connection is closed")
        channel = await self._connection.channel()
        session = AMQPConsumeSession()

        async def _on_message(message: abc.AbstractIncomingMessage) -> None:
            current = asyncio.current_task()
            if current is not None:
                session.watch(current)
            try:
                try:
                    delivery = AMQPDelivery(message)
                except Exception:
                    logger.exception("AMQP delivery could not be decoded")
                    await message.nack(requeue=False)
                    return
                await handler(delivery)
            finally:
                if current is not None:
                    session._tasks.discard(current)

        try:
            await channel.set_qos(prefetch_count=max(1, prefetch))
            exchange = await _declare_direct(channel, exchange_name)
            arguments: dict[str, typing.Any] | None = None
            if dead_letter_routing_key is not None:
                arguments = {
                    "x-dead-letter-exchange": exchange_name,
                    "x-dead-letter-routing-key": dead_letter_routing_key,
                }
                if dead_letter_queue_name is not None:
                    retained = await channel.declare_queue(
                        dead_letter_queue_name,
                        durable=False,
                        auto_delete=False,
                        exclusive=False,
                    )
                    await retained.bind(exchange, routing_key=dead_letter_routing_key)
            queue = await channel.declare_queue(
                queue_name,
                durable=False,
                auto_delete=True,
                exclusive=False,
                arguments=arguments,
            )
            await queue.bind(exchange, routing_key=routing_key)
            consumer_tag = await queue.consume(_on_message)
        except BaseException:
            await _close_channel(channel)
            raise

        session.attach(channel, queue, consumer_tag)
        return session

    async def _channel_for_publish(self) -> aio_pika.abc.AbstractChannel:
        await self.connect()
        if self._connection is None:
            raise RuntimeError("AMQP connection is closed")
        channel = self._publish_channel
        if channel is None or channel.is_closed:
            channel = await self._connection.channel()
            self._publish_channel = channel
        return channel


async def _close_connection(connection: aio_pika.abc.AbstractRobustConnection) -> None:
    try:
        if not connection.is_closed:
            await connection.close()
    except Exception:
        logger.debug("amqp connection cleanup failed", exc_info=True)


async def _close_channel(channel: aio_pika.abc.AbstractChannel) -> None:
    try:
        await channel.close()
    except Exception:
        logger.debug("amqp channel cleanup failed", exc_info=True)


async def _declare_direct(
    channel: aio_pika.abc.AbstractChannel,
    exchange_name: str,
) -> aio_pika.abc.AbstractExchange:
    return await channel.declare_exchange(
        exchange_name,
        type="direct",
        durable=False,
        auto_delete=False,
    )
