"""
AMQP broker and transport.

[AI Instruction]
Import ``AMQPMessageBroker`` from this module. It needs the ``rabbit``
extra. Pass it to bootstrap or to ``cqrs.EventProducer``. The publisher
and consumer types live in ``cqrs.adapters.amqp``.

``AMQPTransport`` is the publish/subscribe transport. Construct it with a
parsed URL (``create_transport("amqp://guest:guest@localhost:5672/")``), a
URL string, or rely on that default. ``topic`` is the direct-exchange
routing key. The queue name is derived from the exchange, the topic, and
the group, so two topics or two exchanges can share a group name without
sharing a queue. The subscriber declares and binds that queue. ``send_message`` publishes to the exchange
and does not replace ``AMQPMessageBroker``. A set ``key`` is the reserved
header ``cqrs-key`` and does not change the route. A user header named
``key`` is left untouched. Do not define ``__bool__`` or ``__len__``.
``AMQPMessageBroker`` still declares a queue named ``message.topic`` and
does not read ``key``.
"""

from __future__ import annotations

import asyncio
import logging
import urllib.parse
import uuid
from typing import ClassVar

import aio_pika

from cqrs.adapters import protocol as adapters_protocol
from cqrs.adapters.amqp import AMQPConsumeSession, AMQPDelivery, NeutralAMQPClient
from cqrs.message_brokers import protocol
from cqrs.serializers.default import message_wire_bytes
from cqrs.transport.errors import TransportClosed, UnsupportedCapability
from cqrs.transport.protocol import (
    IncomingHandler,
    IncomingMessage,
    SubscribeOptions,
    Subscription,
    Transport,
    TransportCapabilities,
)

logger = logging.getLogger("cqrs")

_ATTEMPT = "cqrs-delivery-attempt"
_WIRE_ID = "cqrs-message-id"
_KEY = "cqrs-key"


class AMQPMessageBroker(protocol.MessageBroker):
    def __init__(
        self,
        publisher: adapters_protocol.AMQPPublisher,
        exchange_name: str,
        pika_log_level: str = "ERROR",
    ):
        self.publisher = publisher
        self.exchange_name = exchange_name
        logging.getLogger("aiormq").setLevel(pika_log_level)
        logging.getLogger("aio_pika").setLevel(pika_log_level)

    async def send_message(self, message: protocol.Message) -> None:
        kwargs: dict = {}
        if message.content_type is not None:
            kwargs["content_type"] = message.content_type
        if message.headers is not None:
            kwargs["headers"] = message.headers
        await self.publisher.publish(
            message=aio_pika.Message(
                body=message_wire_bytes(message),
                **kwargs,
            ),
            queue_name=message.topic,
            exchange_name=self.exchange_name,
        )


class _AMQPIncoming(IncomingMessage):
    def __init__(
        self,
        transport: AMQPTransport,
        subscription: _AMQPSubscription,
        delivery: AMQPDelivery,
        *,
        headers: dict[str, str],
        key: str | None,
        delivery_attempt: int,
    ) -> None:
        self._transport = transport
        self._subscription = subscription
        self._delivery = delivery
        self._headers = headers
        self._key = key
        self._delivery_attempt = delivery_attempt
        self._message_id = delivery.message_id
        self._settled = False
        self._dlq_published = False
        self._settle_lock = asyncio.Lock()
        self._accounted = False

    @property
    def topic(self) -> str:
        return self._delivery.topic or self._subscription.group.topic

    @property
    def body(self) -> bytes:
        return self._delivery.body

    @property
    def headers(self) -> dict[str, str]:
        return self._headers

    @property
    def content_type(self) -> str | None:
        return self._delivery.content_type

    @property
    def key(self) -> str | None:
        return self._key

    @property
    def delivery_attempt(self) -> int:
        return self._delivery_attempt

    async def ack(self) -> None:
        async with self._settle_lock:
            if self._settled:
                return
            await self._delivery.ack()
            self._settled = True
            self._transport._forget(self._attempt_key())
            self._transport._finish_incoming(self)

    async def nack(self, *, requeue: bool = True) -> None:
        async with self._settle_lock:
            if self._settled:
                return
            final = (not requeue) or self._delivery_attempt >= self._subscription.options.max_deliveries
            if final:
                await self._finish_final(requeue)
                return
            attempt_key = self._attempt_key()
            if attempt_key is not None:
                self._transport._attempts[attempt_key] = self._delivery_attempt + 1
            await self._delivery.nack(requeue=True)
            self._settled = True

    async def _finish_final(self, requeue: bool) -> None:
        """Publish the dead letter once, then settle the original.

        The lock is held by ``nack``. A failed publish leaves the flag clear
        so the next attempt can publish. A publish that succeeded is kept
        when settling the original still has to be retried.
        """

        if not self._dlq_published:
            try:
                await self._transport._dead_letter(self)
            except Exception:
                logger.exception("amqp dead-letter publish failed for topic %s", self.topic)
                await self._delivery.nack(requeue=True)
                self._settled = True
                return
            self._dlq_published = True
        if requeue:
            await self._delivery.ack()
        else:
            await self._delivery.nack(requeue=False)
        self._settled = True
        self._transport._forget(self._attempt_key())
        self._transport._finish_incoming(self)

    def _attempt_key(self) -> tuple[str, str, str] | None:
        if not self._message_id:
            return None
        group = self._subscription.group
        return (group.topic, group.name, self._message_id)


class _AMQPGroup:
    def __init__(self, topic: str, name: str) -> None:
        self.topic = topic
        self.name = name
        self.subscriptions: list[_AMQPSubscription] = []

    def active(self) -> bool:
        return any(subscription.active for subscription in self.subscriptions)


class _AMQPSubscription(Subscription):
    def __init__(
        self,
        transport: AMQPTransport,
        group: _AMQPGroup,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> None:
        self._transport = transport
        self.group = group
        self.handler = handler
        self.options = options
        self.active = True
        self.session: AMQPConsumeSession | None = None

    async def close(self) -> None:
        if not self.active and self.session is None:
            return
        self.active = False
        session = self.session
        self.session = None
        if session is not None:
            await session.close()


class AMQPTransport(Transport):
    """
    AMQP publish/subscribe transport.

    ``topic`` is the routing key on a direct exchange (default name
    ``cqrs``, or ``?exchange=`` on the URL). The queue name comes from the
    exchange, the topic, and the group together. Competing subscribers of
    that triple share the queue. The same group on another topic or another
    exchange is a different queue. A different group is another queue and
    gets its own copy.
    ``ordering_per_key`` is false. ``native_dlq`` is false: after
    ``max_deliveries``, or on ``nack(requeue=False)``, the bytes are
    republished to ``{topic}.dlq``. ``ack`` and ``nack`` call aio_pika.
    A set ``message.key`` is stored in the reserved header ``cqrs-key`` and
    is not a routing key. There is no ``__bool__`` and no ``__len__``.

    ``create_transport("amqp://guest:guest@localhost:5672/")`` calls this
    with the parsed URL. Each instance opens its own connection.
    """

    capabilities: ClassVar[TransportCapabilities] = TransportCapabilities(
        ordering_per_key=False,
        native_dlq=False,
        delayed_redelivery=False,
        headers=True,
        topic="direct-exchange routing key; the queue is named from the exchange, the topic, and the group",
    )

    def __init__(
        self,
        url: urllib.parse.ParseResult | str | None = None,
        *,
        exchange_name: str | None = None,
    ) -> None:
        logging.getLogger("aiormq").setLevel("ERROR")
        logging.getLogger("aio_pika").setLevel("ERROR")
        self._url, parsed_exchange = _amqp_endpoint(url)
        self._exchange_name = exchange_name or parsed_exchange
        self._client = NeutralAMQPClient(self._url)
        self._closed = False
        self._groups: dict[tuple[str, str], _AMQPGroup] = {}
        self._outstanding: dict[str, int] = {}
        self._attempts: dict[tuple[str, str, str], int] = {}
        self._in_flight = 0
        self._idle = asyncio.Event()
        self._idle.set()

    async def start(self) -> None:
        if self._closed:
            return
        await self._client.connect()

    async def send_message(self, message: protocol.Message) -> None:
        await self._produce(message, delivery_attempt=1)

    async def subscribe(
        self,
        topic: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> Subscription:
        self._check_open("subscribe")
        _check_options(options)
        await self._client.connect()
        group = self._groups.get((topic, options.group))
        if group is None:
            group = _AMQPGroup(topic, options.group)
            self._groups[(topic, options.group)] = group
        subscription = _AMQPSubscription(self, group, handler, options)
        group.subscriptions.append(subscription)

        async def on_delivery(delivery: AMQPDelivery) -> None:
            await self._on_delivery(subscription, delivery)

        try:
            subscription.session = await self._client.consume(
                on_delivery,
                exchange_name=self._exchange_name,
                queue_name=_queue_name(self._exchange_name, topic, options.group),
                routing_key=topic,
                prefetch=options.concurrency * options.prefetch,
                concurrency=options.concurrency,
            )
        except BaseException:
            self._drop_subscription(topic, options.group, subscription)
            raise
        return subscription

    async def drain(self, timeout: float = 5) -> None:
        """Wait until published messages have been acked or dead-lettered."""

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not self._is_idle():
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(self._drain_message())
            self._idle.clear()
            if self._is_idle():
                return
            try:
                await asyncio.wait_for(self._idle.wait(), remaining)
            except asyncio.TimeoutError:
                if self._is_idle():
                    return
                raise TimeoutError(self._drain_message()) from None

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        subscriptions = [subscription for group in self._groups.values() for subscription in group.subscriptions]
        for subscription in subscriptions:
            subscription.active = False
        for subscription in subscriptions:
            session = subscription.session
            subscription.session = None
            if session is not None:
                await session.close()
        await self._client.close()
        self._outstanding.clear()
        self._in_flight = 0
        self._note()

    async def _produce(self, message: protocol.Message, *, delivery_attempt: int) -> None:
        self._check_open("send_message")
        copies = self._destinations(message.topic)
        wire_id = uuid.uuid4().hex
        if copies:
            self._outstanding[wire_id] = copies
            self._note_work()
        headers = dict(message.headers) if message.headers else {}
        headers[_WIRE_ID] = wire_id
        if message.key is not None:
            headers[_KEY] = message.key
        if delivery_attempt > 1:
            headers[_ATTEMPT] = str(delivery_attempt)
        try:
            await self._client.connect()
            await self._client.publish(
                exchange_name=self._exchange_name,
                routing_key=message.topic,
                body=message_wire_bytes(message),
                headers=headers or None,
                content_type=message.content_type,
                message_id=wire_id,
            )
        except Exception:
            if copies:
                self._outstanding.pop(wire_id, None)
                self._note()
            raise

    async def _on_delivery(self, subscription: _AMQPSubscription, delivery: AMQPDelivery) -> None:
        if not subscription.active or self._closed:
            if not delivery.settled:
                await delivery.nack(requeue=True)
            return
        headers, key, header_attempt, wire_id = _view(delivery.headers)
        identity = wire_id or delivery.message_id
        attempt = self._attempt_for(subscription.group.topic, subscription.group.name, identity, header_attempt)
        incoming = _AMQPIncoming(
            self,
            subscription,
            delivery,
            headers=headers,
            key=key,
            delivery_attempt=attempt,
        )
        incoming._message_id = identity
        self._in_flight += 1
        try:
            try:
                await subscription.handler(incoming)
            except asyncio.CancelledError:
                if not incoming._settled:
                    self._finish_incoming(incoming)
                raise
            except Exception:
                logger.exception(
                    "subscriber %s failed for topic %s",
                    subscription.options.group,
                    incoming.topic,
                )
                if not incoming._settled:
                    await incoming.nack(requeue=True)
        finally:
            self._in_flight = max(0, self._in_flight - 1)
            self._note()

    async def _dead_letter(self, incoming: _AMQPIncoming) -> None:
        topic = incoming._subscription.options.dead_letter_topic or f"{incoming._subscription.group.topic}.dlq"
        try:
            await self._produce(
                protocol.Message(
                    message_name="dead_letter",
                    topic=topic,
                    payload=incoming.body,
                    payload_bytes=incoming.body,
                    content_type=incoming.content_type,
                    headers=dict(incoming.headers) or None,
                    key=incoming.key,
                ),
                delivery_attempt=incoming.delivery_attempt,
            )
        except TransportClosed:
            return

    def _attempt_for(
        self,
        topic: str,
        group: str,
        message_id: str | None,
        header_attempt: int | None,
    ) -> int:
        if message_id is not None:
            stored = self._attempts.get((topic, group, message_id))
            if stored is not None:
                return stored
        if header_attempt is not None:
            return header_attempt
        return 1

    def _drop_subscription(self, topic: str, group_name: str, subscription: _AMQPSubscription) -> None:
        group = self._groups.get((topic, group_name))
        if group is None:
            return
        if subscription in group.subscriptions:
            group.subscriptions.remove(subscription)
        if not group.subscriptions:
            self._groups.pop((topic, group_name), None)

    def _forget(self, attempt_key: tuple[str, str, str] | None) -> None:
        if attempt_key is not None:
            self._attempts.pop(attempt_key, None)

    def _destinations(self, topic: str) -> int:
        return sum(1 for group in self._groups.values() if group.topic == topic and group.active())

    def _finish_incoming(self, incoming: _AMQPIncoming) -> None:
        if incoming._accounted:
            return
        incoming._accounted = True
        self._complete(incoming._message_id)

    def _complete(self, wire_id: str | None) -> None:
        if wire_id is None or wire_id not in self._outstanding:
            return
        remaining = self._outstanding[wire_id] - 1
        if remaining <= 0:
            del self._outstanding[wire_id]
        else:
            self._outstanding[wire_id] = remaining
        self._note()

    def _check_open(self, operation: str) -> None:
        if self._closed:
            raise TransportClosed(operation)

    def _is_idle(self) -> bool:
        return not self._outstanding and self._in_flight == 0

    def _note_work(self) -> None:
        self._idle.clear()

    def _note(self) -> None:
        if self._is_idle():
            self._idle.set()

    def _drain_message(self) -> str:
        parts = [
            f"topic={group.topic} group={group.name} queue_length=0 in_flight={self._in_flight}"
            for group in self._groups.values()
        ]
        if not parts:
            parts.append(f"topic=* group=* queue_length=0 in_flight={self._in_flight}")
        parts.append(f"outstanding={sum(self._outstanding.values())}")
        return "drain timed out: " + "; ".join(parts)


def _queue_name(exchange: str, topic: str, group: str) -> str:
    """One queue per exchange, topic, and group. Length prefixes keep the parts distinct."""

    return f"{len(exchange)}.{exchange}.{len(topic)}.{topic}.{group}"


def _amqp_endpoint(url: urllib.parse.ParseResult | str | None) -> tuple[str, str]:
    parsed: urllib.parse.ParseResult | None
    if isinstance(url, urllib.parse.ParseResult):
        parsed = url
    elif isinstance(url, str) and "://" in url:
        parsed = urllib.parse.urlparse(url)
    elif isinstance(url, str) and url:
        return url, "cqrs"
    else:
        return "amqp://guest:guest@localhost:5672/", "cqrs"
    exchange = "cqrs"
    if parsed.query:
        chosen = urllib.parse.parse_qs(parsed.query).get("exchange", [])
        if chosen and chosen[0]:
            exchange = chosen[0]
    if not parsed.hostname:
        return "amqp://guest:guest@localhost:5672/", exchange
    path = parsed.path or "/"
    cleaned = urllib.parse.urlunparse((parsed.scheme or "amqp", parsed.netloc, path, "", "", ""))
    return cleaned, exchange


def _check_options(options: SubscribeOptions) -> None:
    if options.start_from not in ("latest", "earliest"):
        raise UnsupportedCapability(f"start_from={options.start_from!r} is not supported")
    if options.concurrency < 1 or options.prefetch < 1 or options.max_deliveries < 1:
        raise UnsupportedCapability("concurrency, prefetch, and max_deliveries must be >= 1")


def _view(headers: dict[str, str]) -> tuple[dict[str, str], str | None, int | None, str | None]:
    visible = dict(headers)
    wire_id = visible.pop(_WIRE_ID, None)
    key = visible.pop(_KEY, None)
    raw_attempt = visible.pop(_ATTEMPT, None)
    attempt: int | None = None
    if raw_attempt is not None:
        try:
            attempt = int(raw_attempt)
        except ValueError:
            attempt = None
    return visible, key, attempt, wire_id
