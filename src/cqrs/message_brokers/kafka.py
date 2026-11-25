"""
Kafka broker and transport.

[AI Instruction]
Import ``KafkaMessageBroker`` or ``create_transport`` from this module.
Build its producer with ``cqrs.adapters.kafka.kafka_producer_factory``.
Both need the ``kafka`` extra (``pip install "python-cqrs[kafka]"``).
An ``ImportError`` here means that extra is missing. Pass the broker to
bootstrap or to ``cqrs.EventProducer``.

``KafkaTransport`` is the publish/subscribe transport. Construct it with a
parsed URL (``create_transport`` in this module), a URL string,
or ``bootstrap_servers``. ``topic`` is the Kafka topic. ``group`` is the
consumer ``group_id``. Each ``subscribe`` call is one consumer. Do not add
key lanes, a local delivery counter, or a header that pairs publish with
consume. ``prefetch`` is ``max_poll_records``. A set ``key`` is the
partition key. Do not define ``__bool__`` or ``__len__``.
``send_message`` on ``KafkaMessageBroker`` is unchanged: ``key`` is forwarded
only when it is not ``None``.
"""

from __future__ import annotations

import asyncio
import logging
import typing
import urllib.parse
import uuid
from typing import TYPE_CHECKING, ClassVar

from cqrs.adapters import protocol as adapters_protocol
from cqrs.message_brokers import protocol
from cqrs.serializers.default import message_wire_bytes
from cqrs.transport.errors import TransportClosed, TransportError
from cqrs.transport.protocol import (
    HandlerWatch,
    IncomingHandler,
    IncomingMessage,
    SubscribeOptions,
    Subscription,
    Transport,
    TransportCapabilities,
    require_options,
)

if TYPE_CHECKING:
    from cqrs.adapters.kafka import KafkaRecord, KafkaTopicConsumer

logger = logging.getLogger("cqrs")

_CONTENT_TYPE = "cqrs-content-type"


class KafkaMessageBroker(protocol.MessageBroker):
    def __init__(
        self,
        producer: adapters_protocol.KafkaProducer,
        aiokafka_log_level: typing.Text = "ERROR",
    ):
        self._producer = producer
        logging.getLogger("aiokafka").setLevel(aiokafka_log_level)

    def _produce_value(self, message: protocol.Message) -> typing.Any:
        """
        Choose the value passed to ``KafkaProducer.produce``.

        When a legacy ``value_serializer`` was configured on the producer and the
        message has no codec wire bytes, forward ``message.payload`` so that
        serializer can run. Otherwise prefer ``payload_bytes`` / encoded payload
        so Outbox and EventSerializer bytes are not discarded.
        """
        legacy_value_serializer = getattr(
            self._producer,
            "legacy_value_serializer",
            False,
        )
        if legacy_value_serializer and message.payload_bytes is None and message.content_type is None:
            return message.payload
        return message_wire_bytes(message)

    async def send_message(self, message: protocol.Message) -> None:
        payload = self._produce_value(message)
        kwargs: dict[str, typing.Any] = {}
        if message.headers is not None:
            kwargs["headers"] = message.headers
        # Omit ``key`` unless it is set so a producer without that parameter
        # still accepts the call.
        if message.key is not None:
            kwargs["key"] = message.key
        await self._producer.produce(message.topic, payload, **kwargs)


class _KafkaIncoming(IncomingMessage):
    def __init__(
        self,
        *,
        topic: str,
        body: bytes,
        headers: dict[str, str],
        content_type: str | None,
        key: str | None,
    ) -> None:
        self._topic = topic
        self._body = body
        self._headers = headers
        self._content_type = content_type
        self._key = key
        self._settled = False
        self._kind = ""

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def body(self) -> bytes:
        return self._body

    @property
    def headers(self) -> dict[str, str]:
        return self._headers

    @property
    def content_type(self) -> str | None:
        return self._content_type

    @property
    def key(self) -> str | None:
        return self._key

    @property
    def delivery_attempt(self) -> int:
        return 1

    async def ack(self) -> None:
        if self._settled:
            return
        self._settled = True
        self._kind = "ack"

    async def nack(self, *, requeue: bool = True) -> None:
        if self._settled:
            return
        self._settled = True
        self._kind = "requeue" if requeue else "dead"


class _KafkaSubscription(Subscription):
    def __init__(
        self,
        transport: KafkaTransport,
        *,
        topic: str,
        group: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
        consumer: KafkaTopicConsumer,
    ) -> None:
        self._transport = transport
        self.topic = topic
        self.group = group
        self.handler = handler
        self.options = options
        self.consumer = consumer
        self.active = True
        self.task: asyncio.Task[None] | None = None
        self.dead_lettered: set[str] = set()

    async def close(self) -> None:
        await self._transport._close_subscription(self)


class KafkaTransport(Transport):
    """
    Kafka publish/subscribe transport.

    ``topic`` is the Kafka topic and ``group`` is the consumer ``group_id``.
    Each subscription is its own consumer. Subscribers in one group compete
    through Kafka. Different groups each get a copy. A set ``key`` is the
    partition key, so ``ordering_per_key`` is true. Records from one fetch
    are handled in order. ``nack(requeue=False)`` publishes the bytes once
    to ``{topic}.dlq`` and commits. Kafka has no delivery count, so
    ``delivery_attempt`` is 1. There is no ``__bool__`` and no ``__len__``.

    ``create_transport("kafka://localhost:9092")`` calls this with the parsed
    URL. ``KafkaTransport(bootstrap_servers="localhost:9092")`` is the same
    broker. Each instance has its own producer and consumers.
    """

    capabilities: ClassVar[TransportCapabilities] = TransportCapabilities(
        ordering_per_key=True,
        native_dlq=False,
        delayed_redelivery=False,
        headers=True,
        topic="Kafka topic; group is the consumer group_id",
    )

    def __init__(
        self,
        url: urllib.parse.ParseResult | str | None = None,
        *,
        bootstrap_servers: str | None = None,
    ) -> None:
        logging.getLogger("aiokafka").setLevel("ERROR")
        self._bootstrap_servers = _bootstrap_servers(url, bootstrap_servers)
        self._closed = False
        self._subscriptions: list[_KafkaSubscription] = []
        self._tasks: set[asyncio.Task[None]] = set()
        self._topics: set[str] = set()
        self._topic_lock = asyncio.Lock()
        self._producer_lock = asyncio.Lock()
        self._watch = HandlerWatch()
        self._producer: adapters_protocol.KafkaProducer | None = None
        self._aio_producer: typing.Any = None

    async def start(self) -> None:
        if self._closed:
            return
        await self._ensure_producer()

    async def send_message(self, message: protocol.Message) -> None:
        self._check_open("send_message")
        expected = self._group_count(message.topic)
        token = uuid.uuid4().hex
        self._watch.reserve(token, expected)
        headers = dict(message.headers) if message.headers else {}
        if message.content_type is not None:
            headers[_CONTENT_TYPE] = message.content_type
        try:
            producer = await self._ensure_producer()
            kwargs: dict[str, typing.Any] = {}
            if headers:
                kwargs["headers"] = headers
            if message.key is not None:
                kwargs["key"] = message.key
            metadata = await producer.produce(message.topic, message_wire_bytes(message), **kwargs)
        except BaseException:
            self._watch.abandon(f"~{token}", expected)
            raise
        self._watch.bind(token, _record_id(metadata.topic, metadata.partition, metadata.offset))

    async def subscribe(
        self,
        topic: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> Subscription:
        self._check_open("subscribe")
        require_options(options)
        await self._ensure_producer()
        await self._ensure_topic(topic)
        from cqrs.adapters.kafka import KafkaTopicConsumer

        consumer = KafkaTopicConsumer(
            self._bootstrap_servers,
            group_id=options.group,
            topic=topic,
            auto_offset_reset=options.start_from,
        )
        try:
            await consumer.start()
        except BaseException:
            await _stop_quietly(consumer.stop())
            raise
        if self._closed:
            await _stop_quietly(consumer.stop())
            raise TransportClosed("subscribe")
        subscription = _KafkaSubscription(
            self,
            topic=topic,
            group=options.group,
            handler=handler,
            options=options,
            consumer=consumer,
        )
        self._subscriptions.append(subscription)
        subscription.task = self._track(asyncio.create_task(self._run(subscription)))
        return subscription

    async def drain(self, timeout: float = 5) -> None:
        """Wait until handlers for published records have finished."""

        await self._watch.wait(timeout, self._describe)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for subscription in self._subscriptions:
            subscription.active = False
        tasks = [task for task in self._tasks if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for subscription in self._subscriptions:
            await _stop_quietly(subscription.consumer.stop())
        producer = self._aio_producer
        self._aio_producer = None
        self._producer = None
        if producer is not None:
            await _stop_quietly(producer.stop())
        self._watch.abandon_all()
        self._watch.inflight = 0
        self._watch.poke()

    async def _close_subscription(self, subscription: _KafkaSubscription) -> None:
        if not subscription.active and subscription.task is None:
            return
        subscription.active = False
        task = subscription.task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        subscription.task = None
        if subscription in self._subscriptions:
            self._subscriptions.remove(subscription)
        await _stop_quietly(subscription.consumer.stop())

    async def _run(self, subscription: _KafkaSubscription) -> None:
        try:
            while subscription.active and not self._closed:
                try:
                    records = await subscription.consumer.poll(
                        timeout_ms=100,
                        max_records=subscription.options.prefetch,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("kafka poll failed for topic %s", subscription.topic)
                    await asyncio.sleep(0.1)
                    continue
                index = 0
                while index < len(records):
                    if not subscription.active or self._closed:
                        await self._seek_unprocessed(subscription, records[index:])
                        return
                    try:
                        rewind = await self._handle(subscription, records[index])
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("kafka handler failed for topic %s", subscription.topic)
                        rewind = True
                    if rewind:
                        await self._seek_unprocessed(subscription, records[index:])
                        await asyncio.sleep(0.05)
                        break
                    index += 1
        except asyncio.CancelledError:
            return

    async def _handle(self, subscription: _KafkaSubscription, record: KafkaRecord) -> bool:
        record_id = _record_id(record.topic, record.partition, record.offset)
        if record_id in subscription.dead_lettered:
            return await self._commit_source(subscription, record)
        visible, content_type = _visible(record)
        incoming = _KafkaIncoming(
            topic=record.topic,
            body=record.body,
            headers=visible,
            content_type=content_type,
            key=record.key,
        )
        self._watch.begin()
        try:
            try:
                await subscription.handler(incoming)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "subscriber %s failed for topic %s",
                    subscription.group,
                    subscription.topic,
                )
                if not incoming._settled:
                    await incoming.nack(requeue=False)
            if not incoming._settled:
                await incoming.nack(requeue=False)
            if incoming._kind == "requeue":
                return True
            try:
                return await self._finish(subscription, record, incoming, visible, content_type)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "kafka finish failed for topic %s partition %s",
                    subscription.topic,
                    record.partition,
                )
                return True
        finally:
            self._watch.end()

    async def _finish(
        self,
        subscription: _KafkaSubscription,
        record: KafkaRecord,
        incoming: _KafkaIncoming,
        headers: dict[str, str],
        content_type: str | None,
    ) -> bool:
        record_id = _record_id(record.topic, record.partition, record.offset)
        if incoming._kind == "dead" and record_id not in subscription.dead_lettered:
            if await self._dead_letter(subscription, record, headers, content_type):
                subscription.dead_lettered.add(record_id)
        return await self._commit_source(subscription, record)

    async def _seek_unprocessed(self, subscription: _KafkaSubscription, records: list[KafkaRecord]) -> None:
        for partition, offset in _rewind_points(
            [(record.partition, record.offset) for record in records],
        ).items():
            try:
                await subscription.consumer.seek(partition, offset)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "kafka seek skipped for topic %s partition %s",
                    subscription.topic,
                    partition,
                    exc_info=True,
                )

    async def _commit_source(self, subscription: _KafkaSubscription, record: KafkaRecord) -> bool:
        """Commit the source offset. ``True`` means the caller should rewind and retry."""

        record_id = _record_id(record.topic, record.partition, record.offset)
        try:
            await subscription.consumer.commit(record.partition, record.offset + 1)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "kafka commit failed for topic %s partition %s",
                subscription.topic,
                record.partition,
            )
            return True
        subscription.dead_lettered.discard(record_id)
        self._watch.settle(record_id)
        return False

    async def _dead_letter(
        self,
        subscription: _KafkaSubscription,
        record: KafkaRecord,
        headers: dict[str, str],
        content_type: str | None,
    ) -> bool:
        topic = subscription.options.dead_letter_topic or f"{subscription.topic}.dlq"
        try:
            await self.send_message(
                protocol.Message(
                    message_name="dead_letter",
                    topic=topic,
                    payload=record.body,
                    payload_bytes=record.body,
                    content_type=content_type,
                    headers=dict(headers) or None,
                    key=record.key,
                ),
            )
        except TransportClosed:
            return False
        return True

    async def _ensure_producer(self) -> adapters_protocol.KafkaProducer:
        if self._producer is not None:
            return self._producer
        async with self._producer_lock:
            if self._producer is not None:
                return self._producer
            if self._closed:
                raise TransportClosed("send_message")
            import aiokafka

            from cqrs.adapters.kafka import KafkaProducer

            aio_producer = aiokafka.AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
            try:
                await aio_producer.start()
            except BaseException:
                from cqrs.adapters.kafka import stop_client

                await stop_client(aio_producer)
                raise
            if self._closed:
                await _stop_quietly(aio_producer.stop())
                raise TransportClosed("send_message")
            self._aio_producer = aio_producer
            self._producer = KafkaProducer(aio_producer)
            return self._producer

    async def _ensure_topic(self, topic: str) -> None:
        if topic in self._topics:
            return
        async with self._topic_lock:
            if topic in self._topics:
                return
            from aiokafka.admin import AIOKafkaAdminClient, NewTopic

            admin = AIOKafkaAdminClient(bootstrap_servers=self._bootstrap_servers)
            await admin.start()
            try:
                response = await admin.create_topics(
                    [NewTopic(name=topic, num_partitions=1, replication_factor=1)],
                )
            finally:
                await admin.close()
            _raise_for_topic_errors(response)
            self._topics.add(topic)

    def _group_count(self, topic: str) -> int:
        return len({item.group for item in self._subscriptions if item.topic == topic and item.active})

    def _check_open(self, operation: str) -> None:
        if self._closed:
            raise TransportClosed(operation)

    def _describe(self) -> str:
        parts = [
            f"topic={item.topic} group={item.group} pending={self._watch.owed} in_flight={self._watch.inflight}"
            for item in self._subscriptions
            if item.active
        ]
        if not parts:
            parts.append(f"topic=* group=* queue_length=0 in_flight={self._watch.inflight}")
        return "drain timed out: " + "; ".join(parts)

    def _track(self, task: asyncio.Task[None]) -> asyncio.Task[None]:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task


def _record_id(topic: str, partition: int, offset: int) -> str:
    return f"{topic}:{partition}:{offset}"


def _rewind_points(records: list[tuple[int, int]]) -> dict[int, int]:
    """First unprocessed offset of each partition. Later records in the fetch stay behind it."""

    points: dict[int, int] = {}
    for partition, offset in records:
        points.setdefault(partition, offset)
    return points


def _bootstrap_servers(
    url: urllib.parse.ParseResult | str | None,
    bootstrap_servers: str | None,
) -> str:
    if bootstrap_servers:
        return bootstrap_servers
    parsed: urllib.parse.ParseResult | None
    if isinstance(url, urllib.parse.ParseResult):
        parsed = url
    elif isinstance(url, str) and "://" in url:
        parsed = urllib.parse.urlparse(url)
    elif isinstance(url, str) and url:
        return url
    else:
        return "localhost:9092"
    if parsed.hostname:
        return f"{parsed.hostname}:{parsed.port or 9092}"
    if parsed.netloc:
        return parsed.netloc
    return "localhost:9092"


def _visible(record: KafkaRecord) -> tuple[dict[str, str], str | None]:
    headers = {key: value for key, value in record.headers.items() if key != _CONTENT_TYPE}
    content_type = record.headers.get(_CONTENT_TYPE) or None
    return headers, content_type


def _raise_for_topic_errors(response: object) -> None:
    to_object = getattr(response, "to_object", None)
    if not callable(to_object):
        return
    payload = to_object()
    if not isinstance(payload, dict):
        return
    errors = payload.get("topic_errors", [])
    if not isinstance(errors, list):
        return
    for item in errors:
        if not isinstance(item, dict):
            continue
        code = item.get("error_code", 0)
        if code in (0, 36, None):
            continue
        detail = item.get("error_message") or code
        raise TransportError(f"Could not create Kafka topic: {detail}")


async def _stop_quietly(stopping: typing.Awaitable[object]) -> None:
    try:
        await stopping
    except Exception:
        logger.debug("kafka transport shutdown step failed", exc_info=True)


def create_transport(url: str) -> KafkaTransport:
    """Build ``KafkaTransport`` from a ``kafka://`` URL."""

    parsed = urllib.parse.urlparse(url)
    if not parsed.scheme:
        raise TransportError(f"Transport URL {url!r} has no scheme")
    if parsed.scheme != "kafka":
        raise TransportError(
            f"KafkaTransport URL scheme must be 'kafka', got {parsed.scheme!r}",
        )
    return KafkaTransport(parsed)


__all__ = ("KafkaTransport", "create_transport")
