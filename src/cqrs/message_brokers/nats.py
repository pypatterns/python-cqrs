"""
NATS JetStream transport.

[AI Instruction]
``create_transport`` in this module builds ``NATSTransport``. The
``topic`` is the JetStream subject, unchanged. ``key`` is a header. The
subject is not suffixed with the key, so ``ordering_per_key`` is false.
``group`` is the durable consumer name. Each ``subscribe`` call is one
pull consumer on that durable. ``prefetch`` is ``max_ack_pending``.
``max_deliveries`` is the durable ``max_deliver``. ``ack`` / ``nak`` /
``term`` are the client calls. Do not republish a dead letter, lock by
key, or count deliveries in this process. Install the ``nats`` extra
(``pip install "python-cqrs[nats]"``). An ``ImportError`` here means that
extra is missing. Two transports on one server need different ``subject_prefix``
values. There is no module-level bus, no ``__bool__``, and no ``__len__``.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import urllib.parse
import uuid
from typing import ClassVar

from cqrs.adapters.nats import JetStreamConsumer, JetStreamMessage, JetStreamSession
from cqrs.message_brokers.protocol import Message
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

_CONTENT_TYPE_HEADER = "cqrs-content-type"
_KEY_HEADER = "cqrs-key"
_INVALID_DURABLE = set(">*. /\\\t\r\n")
_FETCH_TIMEOUT = 0.2


class _NATSSubscription(Subscription):
    def __init__(
        self,
        transport: NATSTransport,
        *,
        topic: str,
        group: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
        consumer: JetStreamConsumer,
    ) -> None:
        self._transport = transport
        self.topic = topic
        self.group = group
        self.handler = handler
        self.options = options
        self.consumer = consumer
        self.active = True
        self.task: asyncio.Task[None] | None = None
        self.delivery_limit = options.max_deliveries

    async def close(self) -> None:
        if not self.active and self.task is None:
            return
        self.active = False
        task = self.task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.task = None
        await self.consumer.unsubscribe()
        self._transport._drop(self)


class _NATSIncoming(IncomingMessage):
    def __init__(
        self,
        transport: NATSTransport,
        subscription: _NATSSubscription,
        wire: JetStreamMessage,
        *,
        headers: dict[str, str],
        content_type: str | None,
        key: str | None,
    ) -> None:
        self._transport = transport
        self._subscription = subscription
        self._wire = wire
        self._headers = headers
        self._content_type = content_type
        self._key = key
        self._settled = False

    @property
    def topic(self) -> str:
        return self._subscription.topic

    @property
    def body(self) -> bytes:
        return self._wire.data

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
        return self._wire.delivery_count

    async def ack(self) -> None:
        if self._settled:
            return
        self._settled = True
        await self._wire.ack()
        self._transport._watch.settle(_nats_id(self._subscription.topic, self._wire.stream_seq))

    async def nack(self, *, requeue: bool = True) -> None:
        if self._settled:
            return
        self._settled = True
        limit = self._subscription.delivery_limit
        if requeue and self.delivery_attempt < limit:
            await self._wire.nak()
            return
        await self._wire.term()
        self._transport._watch.settle(_nats_id(self._subscription.topic, self._wire.stream_seq))


class NATSTransport(Transport):
    """
    JetStream publish and subscribe.

    ``url`` is a ``nats://`` URL or the parsed URL from ``create_transport``.
    ``subject_prefix`` is prepended to every subject and removed before the
    handler sees ``topic``. A missing trailing dot is added. An empty prefix
    publishes the topic unchanged. On one shared server, give each transport
    its own prefix so their subjects do not overlap.

    The same ``group`` is one durable. Each ``subscribe`` pulls from it.
    ``nack(requeue=True)`` calls ``nak`` until ``max_deliver``. A later nack,
    and ``nack(requeue=False)``, call ``term``. ``delivery_attempt`` is
    JetStream ``num_delivered``. There is no ``__bool__`` and no ``__len__``.
    """

    capabilities: ClassVar[TransportCapabilities] = TransportCapabilities(
        ordering_per_key=False,
        native_dlq=True,
        delayed_redelivery=False,
        headers=True,
        topic="JetStream subject, used as given",
    )

    def __init__(
        self,
        url: str | urllib.parse.ParseResult = "nats://localhost:4222",
        *,
        subject_prefix: str | None = None,
        connect_timeout: int = 2,
        storage: str | None = None,
        delete_stream_on_close: bool | None = None,
    ) -> None:
        servers, query = _read_url(url)
        if subject_prefix is None:
            subject_prefix = _first(query, "subject_prefix", "")
        if storage is None:
            storage = _first(query, "storage", "file")
        if delete_stream_on_close is None:
            delete_stream_on_close = _as_bool(_first(query, "delete_stream_on_close", "false"))
        if storage not in ("file", "memory"):
            raise UnsupportedCapability(f"NATSTransport does not support storage={storage!r}")
        self.subject_prefix = _normalize_prefix(subject_prefix)
        self._delete_stream_on_close = delete_stream_on_close
        self._session = JetStreamSession(servers, connect_timeout=connect_timeout, storage=storage)
        self._closed = False
        self._subscriptions: list[_NATSSubscription] = []
        self._tasks: set[asyncio.Task[None]] = set()
        self._watch = HandlerWatch()

    async def start(self) -> None:
        if self._closed:
            return
        await self._session.connect()

    async def send_message(self, message: Message) -> None:
        self._check_open("send_message")
        expected = self._group_count(message.topic)
        token = uuid.uuid4().hex
        self._watch.reserve(token, expected)
        subject = self._wire_subject(message.topic)
        try:
            await self._session.ensure_stream(_stream_name(subject), subject)
            seq = await self._session.publish(subject, message_wire_bytes(message), _outgoing_headers(message))
        except BaseException:
            self._watch.abandon(f"~{token}", expected)
            raise
        self._watch.bind(token, _nats_id(message.topic, seq))

    async def subscribe(
        self,
        topic: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> Subscription:
        self._check_open("subscribe")
        require_options(options)
        subject = self._wire_subject(topic)
        stream = _stream_name(subject)
        await self._session.ensure_stream(stream, subject)
        consumer = await self._session.pull(
            subject=subject,
            stream=stream,
            durable=_durable_name(options.group),
            start_from=options.start_from,
            max_deliver=options.max_deliveries,
            max_ack_pending=options.prefetch,
        )
        if self._closed:
            await consumer.unsubscribe()
            raise TransportClosed("subscribe")
        subscription = _NATSSubscription(
            self,
            topic=topic,
            group=options.group,
            handler=handler,
            options=options,
            consumer=consumer,
        )
        try:
            stored_limit = await consumer.max_deliver()
        except Exception:
            try:
                await consumer.unsubscribe()
            except Exception:
                logger.debug("nats consumer close failed for topic %s", topic, exc_info=True)
            raise
        if stored_limit is not None:
            subscription.delivery_limit = stored_limit
        self._subscriptions.append(subscription)
        subscription.task = self._track(asyncio.create_task(self._fetch(subscription)))
        return subscription

    async def drain(self, timeout: float = 5) -> None:
        """Wait until handlers for published messages have finished."""

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
            await subscription.consumer.unsubscribe()
        self._subscriptions.clear()
        await self._session.close(delete_streams=self._delete_stream_on_close)
        self._watch.abandon_all()
        self._watch.inflight = 0
        self._watch.poke()

    async def _fetch(self, subscription: _NATSSubscription) -> None:
        try:
            while subscription.active and not self._closed:
                try:
                    messages = await subscription.consumer.fetch(
                        batch=subscription.options.prefetch,
                        timeout=_FETCH_TIMEOUT,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("nats fetch failed for topic %s", subscription.topic)
                    await asyncio.sleep(0.1)
                    continue
                for index, message in enumerate(messages):
                    if not subscription.active or self._closed:
                        for leftover in messages[index:]:
                            await leftover.nak()
                        return
                    try:
                        await self._handle(subscription, message)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("nats handler failed for topic %s", subscription.topic)
        except asyncio.CancelledError:
            return

    async def _handle(self, subscription: _NATSSubscription, message: JetStreamMessage) -> None:
        headers, content_type, key = _parse_delivery(message)
        incoming = _NATSIncoming(
            self,
            subscription,
            message,
            headers=headers,
            content_type=content_type,
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
                    subscription.topic,
                )
                if not incoming._settled:
                    await incoming.nack(requeue=True)
        finally:
            self._watch.end()

    def _drop(self, subscription: _NATSSubscription) -> None:
        if subscription in self._subscriptions:
            self._subscriptions.remove(subscription)

    def _group_count(self, topic: str) -> int:
        return len({item.group for item in self._subscriptions if item.topic == topic and item.active})

    def _wire_subject(self, topic: str) -> str:
        return f"{self.subject_prefix}{topic}"

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


def _nats_id(topic: str, seq: int) -> str:
    return f"{topic}:{seq}"


def _read_url(url: str | urllib.parse.ParseResult) -> tuple[str, dict[str, list[str]]]:
    parsed = url if isinstance(url, urllib.parse.ParseResult) else urllib.parse.urlparse(url)
    if not parsed.scheme:
        raise TransportError(f"Transport URL {url!r} has no scheme")
    if parsed.scheme != "nats":
        raise TransportError(f"NATSTransport URL scheme must be 'nats', got {parsed.scheme!r}")
    host = parsed.hostname or "localhost"
    port = parsed.port or 4222
    userinfo = ""
    if parsed.username:
        userinfo = parsed.username
        if parsed.password:
            userinfo = f"{userinfo}:{parsed.password}"
        userinfo = f"{userinfo}@"
    return f"nats://{userinfo}{host}:{port}", urllib.parse.parse_qs(parsed.query)


def _first(query: dict[str, list[str]], name: str, default: str) -> str:
    values = query.get(name)
    if not values:
        return default
    return values[0]


def _as_bool(value: str) -> bool:
    return value.lower() in ("1", "true", "yes")


def _normalize_prefix(prefix: str) -> str:
    if prefix == "":
        return ""
    if any(character in prefix for character in "*>\n\r\t "):
        raise TransportError(f"subject_prefix {prefix!r} contains a NATS wildcard or whitespace")
    if not prefix.endswith("."):
        prefix = f"{prefix}."
    if any(token == "" for token in prefix[:-1].split(".")):
        raise TransportError(f"subject_prefix {prefix!r} has an empty subject token")
    return prefix


def _durable_name(group: str) -> str:
    if not group or any(character in _INVALID_DURABLE for character in group):
        raise TransportError(
            f"NATS durable name {group!r} is invalid. group is the durable consumer name.",
        )
    return group


def _stream_name(subject: str) -> str:
    digest = hashlib.sha256(subject.encode()).hexdigest()[:10]
    slug = "".join(character if character.isalnum() or character in "-_" else "_" for character in subject)
    slug = slug.strip("_") or "cqrs"
    return f"{slug[:200]}_{digest}"


def _outgoing_headers(message: Message) -> dict[str, str] | None:
    headers: dict[str, str] = {}
    if message.headers:
        headers.update(message.headers)
    if message.content_type is not None:
        headers[_CONTENT_TYPE_HEADER] = message.content_type
    if message.key is not None:
        headers[_KEY_HEADER] = message.key
    if not headers:
        return None
    return headers


def _parse_delivery(message: JetStreamMessage) -> tuple[dict[str, str], str | None, str | None]:
    headers = dict(message.headers)
    content_type = headers.pop(_CONTENT_TYPE_HEADER, None) or None
    key = headers.pop(_KEY_HEADER, None) or None
    return headers, content_type, key


def create_transport(url: str) -> NATSTransport:
    """Build ``NATSTransport`` from a ``nats://`` URL."""

    parsed = urllib.parse.urlparse(url)
    if not parsed.scheme:
        raise TransportError(f"Transport URL {url!r} has no scheme")
    if parsed.scheme != "nats":
        raise TransportError(
            f"NATSTransport URL scheme must be 'nats', got {parsed.scheme!r}",
        )
    return NATSTransport(parsed)


__all__ = ("NATSTransport", "create_transport")
