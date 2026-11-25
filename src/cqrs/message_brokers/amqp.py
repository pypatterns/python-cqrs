"""
AMQP broker and transport.

[AI Instruction]
Import ``AMQPMessageBroker`` or ``create_transport`` from this module.
It needs the ``rabbit`` extra (``pip install "python-cqrs[rabbit]"``).
An ``ImportError`` here means that extra is missing. Pass the broker to
bootstrap or to ``cqrs.EventProducer``. The publisher
and consumer types live in ``cqrs.adapters.amqp``.

``AMQPTransport`` is the publish/subscribe transport. Construct it with a
parsed URL (``create_transport`` in this module), a
URL string, or rely on that default. ``topic`` is the direct-exchange
routing key. The queue name is derived from the exchange, the topic, and
the group. Each ``subscribe`` call is one consumer on that queue.
``prefetch`` is channel QoS. Do not add a worker pool, a local delivery
counter, or a header that pairs publish with consume. A set ``key`` is
the header ``cqrs-key`` and does not change the route. Do not define
``__bool__`` or ``__len__``. ``AMQPMessageBroker`` still declares a queue
named ``message.topic`` and does not read ``key``.
"""

from __future__ import annotations

import asyncio
import logging
import urllib.parse
from typing import ClassVar

import aio_pika

from cqrs.adapters import protocol as adapters_protocol
from cqrs.adapters.amqp import AMQPConsumeSession, AMQPDelivery, NeutralAMQPClient
from cqrs.message_brokers import protocol
from cqrs.serializers.default import message_wire_bytes
from cqrs.transport.errors import TransportClosed, TransportError, UnsupportedCapability
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

logger = logging.getLogger("cqrs")

_KEY = "cqrs-key"
_DELIVERY_COUNT = "x-delivery-count"


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
    ) -> None:
        self._transport = transport
        self._subscription = subscription
        self._delivery = delivery
        self._headers = headers
        self._key = key
        self._settled = False
        self._message_id = str(delivery.message_id or "")

    @property
    def topic(self) -> str:
        return self._delivery.topic or self._subscription.topic

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
        raw = self._headers.get(_DELIVERY_COUNT)
        if raw is None:
            return 1
        try:
            count = int(raw)
        except ValueError:
            return 1
        return max(1, count + 1)

    async def ack(self) -> None:
        if self._settled:
            return
        self._settled = True
        await self._delivery.ack()
        self._transport._watch.settle(self._message_id)

    async def nack(self, *, requeue: bool = True) -> None:
        if self._settled:
            return
        self._settled = True
        # Classic queues do not send x-delivery-count, so delivery_attempt stays 1
        # and only max_deliveries=1 turns a requeue into the dead-letter exchange.
        final = (not requeue) or self.delivery_attempt >= self._subscription.options.max_deliveries
        await self._delivery.nack(requeue=not final)
        if not final:
            return
        dlq_topic = self._subscription.options.dead_letter_topic or f"{self._subscription.topic}.dlq"
        if self._transport._group_count(dlq_topic) < 1:
            self._transport._watch.settle(self._message_id)


class _AMQPSubscription(Subscription):
    def __init__(
        self,
        transport: AMQPTransport,
        *,
        topic: str,
        group: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> None:
        self._transport = transport
        self.topic = topic
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
        self._transport._drop(self)


class AMQPTransport(Transport):
    """
    AMQP publish/subscribe transport.

    ``topic`` is the routing key on a direct exchange (default name
    ``cqrs``, or ``?exchange=`` on the URL). The queue name comes from the
    exchange, the topic, and the group together. Two subscriptions with
    that triple are two consumers on one queue. A different group is
    another queue and gets its own copy. ``ordering_per_key`` is false.
    ``nack(requeue=False)`` is the queue dead-letter exchange.
    ``delivery_attempt`` reads ``x-delivery-count`` when the broker set it.
    A set ``message.key`` is the header ``cqrs-key``. There is no
    ``__bool__`` and no ``__len__``.

    ``create_transport("amqp://guest:guest@localhost:5672/")`` calls this
    with the parsed URL. Each instance opens its own connection.
    """

    capabilities: ClassVar[TransportCapabilities] = TransportCapabilities(
        ordering_per_key=False,
        native_dlq=True,
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
        self._subscriptions: list[_AMQPSubscription] = []
        self._watch = HandlerWatch()

    async def start(self) -> None:
        if self._closed:
            return
        await self._client.connect()

    async def send_message(self, message: protocol.Message) -> None:
        self._check_open("send_message")
        expected = self._group_count(message.topic)
        message_key = str(message.message_id)
        self._watch.expect(message_key, expected)
        headers = dict(message.headers) if message.headers else {}
        if message.key is not None:
            headers[_KEY] = message.key
        try:
            await self._client.connect()
            await self._client.publish(
                exchange_name=self._exchange_name,
                routing_key=message.topic,
                body=message_wire_bytes(message),
                headers=headers or None,
                content_type=message.content_type,
                message_id=message_key,
            )
        except BaseException:
            self._watch.abandon(message_key, expected)
            raise

    async def subscribe(
        self,
        topic: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> Subscription:
        self._check_open("subscribe")
        require_options(options)
        if options.start_from == "earliest":
            raise UnsupportedCapability("AMQPTransport has no log to replay; start_from must be latest")
        await self._client.connect()
        subscription = _AMQPSubscription(
            self,
            topic=topic,
            group=options.group,
            handler=handler,
            options=options,
        )

        async def on_delivery(delivery: AMQPDelivery) -> None:
            await self._on_delivery(subscription, delivery)

        session = await self._client.consume(
            on_delivery,
            exchange_name=self._exchange_name,
            queue_name=_queue_name(self._exchange_name, topic, options.group),
            routing_key=topic,
            prefetch=options.prefetch,
            dead_letter_routing_key=options.dead_letter_topic or f"{topic}.dlq",
            dead_letter_queue_name="cqrs-hold."
            + _queue_name(
                self._exchange_name,
                options.dead_letter_topic or f"{topic}.dlq",
                "hold",
            ),
        )
        if self._closed:
            await session.close()
            raise TransportClosed("subscribe")
        subscription.session = session
        self._subscriptions.append(subscription)
        return subscription

    async def drain(self, timeout: float = 5) -> None:
        """Wait until handlers for published messages have finished."""

        await self._watch.wait(timeout, self._describe)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        subscriptions = list(self._subscriptions)
        for subscription in subscriptions:
            subscription.active = False
        for subscription in subscriptions:
            session = subscription.session
            subscription.session = None
            if session is not None:
                await session.close()
        self._subscriptions.clear()
        await self._client.close()
        self._watch.abandon_all()
        self._watch.inflight = 0
        self._watch.poke()

    async def _on_delivery(self, subscription: _AMQPSubscription, delivery: AMQPDelivery) -> None:
        if not subscription.active or self._closed:
            if not delivery.settled:
                await delivery.nack(requeue=True)
            return
        headers, key = _view(delivery.headers)
        incoming = _AMQPIncoming(
            self,
            subscription,
            delivery,
            headers=headers,
            key=key,
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
                    incoming.topic,
                )
                if not incoming._settled:
                    await incoming.nack(requeue=False)
        finally:
            self._watch.end()

    def _drop(self, subscription: _AMQPSubscription) -> None:
        if subscription in self._subscriptions:
            self._subscriptions.remove(subscription)

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


def _view(headers: dict[str, str]) -> tuple[dict[str, str], str | None]:
    visible = dict(headers)
    key = visible.pop(_KEY, None)
    return visible, key


def create_transport(url: str) -> AMQPTransport:
    """Build ``AMQPTransport`` from an ``amqp://`` URL."""

    parsed = urllib.parse.urlparse(url)
    if not parsed.scheme:
        raise TransportError(f"Transport URL {url!r} has no scheme")
    if parsed.scheme != "amqp":
        raise TransportError(
            f"AMQPTransport URL scheme must be 'amqp', got {parsed.scheme!r}",
        )
    return AMQPTransport(parsed)


__all__ = ("AMQPTransport", "create_transport")
