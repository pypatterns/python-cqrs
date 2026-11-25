"""
Redis Streams transport.

[AI Instruction]
``create_transport`` in this module builds ``RedisTransport`` from a
``redis://`` URL. Import this module to call it; install the ``redis`` extra
(``pip install "python-cqrs[redis]"``). An ``ImportError`` here means that
extra is missing. Publish with ``send_message``.
Subscribe with ``subscribe``. There is no ``publish`` alias, no ``__bool__``,
and no ``__len__``. Do not import FastAPI or FastStream.

``topic`` is the Redis stream key. ``group`` is the consumer group. Each
``subscribe`` call is one ``XREADGROUP`` consumer. A second consumer in the
same group competes. A different group has its own cursor. ``message.key``
is a stream field and does not order delivery. ``prefetch`` is ``COUNT``.
``delivery_attempt`` is ``XPENDING`` ``times_delivered``. There is no local
counter and no retry queue. ``nack(requeue=True)`` leaves the id pending so
``XAUTOCLAIM`` can deliver it again. At ``max_deliveries``, or on
``nack(requeue=False)``, the bytes are published once to ``{topic}.dlq``
and the id is ``XACK``ed.

Pass ``prefix`` when two transports share one Redis and must not see each
other's streams. The prefix is prepended to the stream key only.
``IncomingMessage.topic`` stays the topic the caller used.
"""

from __future__ import annotations

import asyncio
import logging
import urllib.parse
import uuid
from collections.abc import Mapping
from typing import ClassVar

import orjson

from cqrs.adapters.redis import RedisStreamClient, StreamEntry
from cqrs.message_brokers.protocol import Message
from cqrs.serializers.default import message_wire_bytes
from cqrs.transport.errors import TransportClosed, TransportError
from cqrs.transport.protocol import (
    IncomingHandler,
    IncomingMessage,
    SubscribeOptions,
    Subscription,
    Transport,
    TransportCapabilities,
    require_options,
)

logger = logging.getLogger("cqrs")

_OWN_QUERY = frozenset({"prefix", "claim_idle_ms", "block_ms"})
_DEFAULT_CLAIM_IDLE_MS = 30_000
_DEFAULT_BLOCK_MS = 200


class _Group:
    def __init__(self, topic: str, name: str, stream: str) -> None:
        self.topic = topic
        self.name = name
        self.stream = stream
        self.subscriptions: list[_RedisSubscription] = []


class _RedisSubscription(Subscription):
    def __init__(
        self,
        transport: RedisTransport,
        group: _Group,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> None:
        self._transport = transport
        self.group = group
        self.handler = handler
        self.options = options
        self.active = True
        self.worker: asyncio.Task[None] | None = None

    async def close(self) -> None:
        if not self.active:
            return
        self.active = False
        worker = self.worker
        if worker is not None and not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)


class _RedisIncoming(IncomingMessage):
    def __init__(
        self,
        transport: RedisTransport,
        group: _Group,
        subscription: _RedisSubscription,
        entry: StreamEntry,
        *,
        body: bytes,
        headers: dict[str, str],
        content_type: str | None,
        key: str | None,
        delivery_attempt: int,
    ) -> None:
        self._transport = transport
        self._group = group
        self._subscription = subscription
        self._entry = entry
        self._body = body
        self._headers = headers
        self._content_type = content_type
        self._key = key
        self._delivery_attempt = delivery_attempt
        self._settled = False

    @property
    def topic(self) -> str:
        return self._group.topic

    @property
    def body(self) -> bytes:
        return self._body

    @property
    def headers(self) -> Mapping[str, str]:
        return self._headers

    @property
    def content_type(self) -> str | None:
        return self._content_type

    @property
    def key(self) -> str | None:
        return self._key

    @property
    def delivery_attempt(self) -> int:
        return self._delivery_attempt

    async def ack(self) -> None:
        if self._settled:
            return
        self._settled = True
        if self._transport._closed:
            return
        await self._transport._client.ack(self._group.stream, self._group.name, _as_bytes(self._entry.message_id))

    async def nack(self, *, requeue: bool = True) -> None:
        if self._settled:
            return
        self._settled = True
        if self._transport._closed:
            return
        give_up = (not requeue) or self._delivery_attempt >= self._subscription.options.max_deliveries
        if give_up:
            await self._transport._dead_letter(self)
            return


class RedisTransport(Transport):
    """
    Redis Streams transport.

    Construct it with the URL or parsed URL from ``create_transport``, or call
    ``RedisTransport(url)`` directly. ``prefix`` isolates stream keys when
    several transports share one Redis. Do not define ``__bool__`` or ``__len__``.
    ``drain`` waits until subscribed groups have no lag, no pending entries,
    and no handler still running. ``close`` stops readers. ``send_message``
    and ``subscribe`` after ``close`` raise ``TransportClosed``.
    """

    capabilities: ClassVar[TransportCapabilities] = TransportCapabilities(
        ordering_per_key=False,
        native_dlq=False,
        delayed_redelivery=True,
        headers=True,
        topic="Redis stream key",
    )

    def __init__(
        self,
        parsed: urllib.parse.ParseResult | str,
        *,
        prefix: str | None = None,
        claim_idle_ms: int | None = None,
        block_ms: int | None = None,
    ) -> None:
        if isinstance(parsed, str):
            parsed = urllib.parse.urlparse(parsed)
        query = urllib.parse.parse_qs(parsed.query)
        if prefix is None:
            prefix = query.get("prefix", [""])[0]
        if claim_idle_ms is None:
            claim_idle_ms = _query_int(query, "claim_idle_ms", _DEFAULT_CLAIM_IDLE_MS)
        if block_ms is None:
            block_ms = _query_int(query, "block_ms", _DEFAULT_BLOCK_MS)
        if claim_idle_ms < 0:
            raise TransportError(f"Invalid claim_idle_ms {claim_idle_ms!r}")
        if block_ms < 1:
            raise TransportError(f"Invalid block_ms {block_ms!r}")
        self._prefix = prefix
        self._claim_idle_ms = claim_idle_ms
        self._block_ms = block_ms
        self._client = RedisStreamClient(_connection_url(parsed))
        self._closed = False
        self._groups: dict[tuple[str, str], _Group] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._handlers = 0
        self._backlog_note = "topic=* group=* queue_length=0 in_flight=0"

    async def start(self) -> None:
        """Connect to Redis. A later ``close`` makes this a no-op."""

        if self._closed:
            return
        await self._client.ping()

    async def send_message(self, message: Message) -> None:
        if self._closed:
            raise TransportClosed("send_message")
        headers = dict(message.headers) if message.headers else {}
        fields: dict[str, bytes | str] = {
            "body": message_wire_bytes(message),
            "headers": orjson.dumps(headers),
            "content_type": message.content_type or "",
        }
        if message.key is not None:
            fields["key"] = message.key
        await self._client.add(self._stream_key(message.topic), fields)

    async def subscribe(
        self,
        topic: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> Subscription:
        if self._closed:
            raise TransportClosed("subscribe")
        require_options(options)
        group, created = self._ensure_group(topic, options.group)
        if created:
            try:
                start_id = "0" if options.start_from == "earliest" else "$"
                await self._client.create_group(group.stream, group.name, start_id)
            except Exception:
                self._groups.pop((topic, options.group), None)
                raise
        if self._closed:
            raise TransportClosed("subscribe")
        subscription = _RedisSubscription(self, group, handler, options)
        group.subscriptions.append(subscription)
        consumer = f"{options.group}-{uuid.uuid4().hex}"
        subscription.worker = self._track(
            asyncio.create_task(self._reader(subscription, group, consumer, options.prefetch)),
        )
        return subscription

    async def drain(self, timeout: float = 5) -> None:
        """
        Wait until subscribed groups have no lag, no pending entries, and no
        handlers still running.
        """

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            if await self._is_drained():
                return
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(self._drain_message())
            await asyncio.sleep(min(0.01, remaining))

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for group in self._groups.values():
            for subscription in group.subscriptions:
                subscription.active = False
        tasks = [task for task in self._tasks if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._handlers = 0
        try:
            await self._client.aclose()
        except Exception:
            logger.exception("redis transport close failed")

    async def _reader(
        self,
        subscription: _RedisSubscription,
        group: _Group,
        consumer: str,
        count: int,
    ) -> None:
        try:
            while subscription.active and not self._closed:
                try:
                    entries = await self._client.read_group(
                        group.stream,
                        group.name,
                        consumer,
                        count=count,
                        block_ms=self._block_ms,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("redis read failed for topic %s", group.topic)
                    await asyncio.sleep(0.05)
                    continue
                claimed = await self._claim(group, consumer, count)
                seen: set[bytes] = set()
                for entry in (*entries, *claimed):
                    identity = _as_bytes(entry.message_id)
                    if identity in seen:
                        continue
                    seen.add(identity)
                    if not subscription.active or self._closed:
                        return
                    await self._handle(subscription, group, entry)
        except asyncio.CancelledError:
            return

    async def _claim(self, group: _Group, consumer: str, count: int) -> list[StreamEntry]:
        try:
            return await self._client.autoclaim(
                group.stream,
                group.name,
                consumer,
                self._claim_idle_ms,
                count=count,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("redis autoclaim failed for topic %s", group.topic, exc_info=True)
            return []

    async def _handle(self, subscription: _RedisSubscription, group: _Group, entry: StreamEntry) -> None:
        body, headers, content_type, key = _envelope(entry)
        try:
            attempt = await self._client.times_delivered(group.stream, group.name, _as_bytes(entry.message_id))
        except Exception:
            logger.exception("redis pending count failed for topic %s", group.topic)
            attempt = 1
        incoming = _RedisIncoming(
            self,
            group,
            subscription,
            entry,
            body=body,
            headers=headers,
            content_type=content_type,
            key=key,
            delivery_attempt=attempt,
        )
        self._handlers += 1
        try:
            try:
                await subscription.handler(incoming)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("subscriber %s failed for topic %s", subscription.options.group, group.topic)
                if not incoming._settled:
                    try:
                        await incoming.nack(requeue=True)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("redis nack failed for topic %s", group.topic)
        finally:
            self._handlers = max(0, self._handlers - 1)

    async def _dead_letter(self, incoming: _RedisIncoming) -> None:
        topic = incoming._subscription.options.dead_letter_topic or f"{incoming.topic}.dlq"
        await self.send_message(
            Message(
                message_name="dead_letter",
                topic=topic,
                payload=incoming.body,
                payload_bytes=incoming.body,
                headers=dict(incoming.headers) or None,
                content_type=incoming.content_type,
                key=incoming.key,
            ),
        )
        if self._closed:
            return
        await self._client.ack(
            incoming._group.stream,
            incoming._group.name,
            _as_bytes(incoming._entry.message_id),
        )

    async def _is_drained(self) -> bool:
        if self._handlers:
            self._backlog_note = f"topic=* group=* queue_length=0 in_flight={self._handlers}"
            return False
        notes: list[str] = []
        for group in self._groups.values():
            if not any(subscription.active for subscription in group.subscriptions):
                continue
            try:
                pending, lag = await self._client.backlog(group.stream, group.name)
            except Exception:
                logger.exception("redis backlog failed for topic %s", group.topic)
                return False
            if pending or lag:
                notes.append(
                    f"topic={group.topic} group={group.name} "
                    f"queue_length={lag} in_flight={pending + self._handlers}",
                )
        if notes:
            self._backlog_note = "; ".join(notes)
            return False
        if self._handlers:
            self._backlog_note = f"topic=* group=* queue_length=0 in_flight={self._handlers}"
            return False
        self._backlog_note = "topic=* group=* queue_length=0 in_flight=0"
        return True

    def _drain_message(self) -> str:
        return "drain timed out: " + self._backlog_note

    def _ensure_group(self, topic: str, name: str) -> tuple[_Group, bool]:
        key = (topic, name)
        group = self._groups.get(key)
        if group is not None:
            return group, False
        group = _Group(topic, name, self._stream_key(topic))
        self._groups[key] = group
        return group, True

    def _stream_key(self, topic: str) -> str:
        return f"{self._prefix}{topic}"

    def _track(self, task: asyncio.Task[None]) -> asyncio.Task[None]:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task


def _connection_url(parsed: urllib.parse.ParseResult) -> str:
    pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    kept = [(key, value) for key, value in pairs if key not in _OWN_QUERY]
    return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(kept), fragment=""))


def _query_int(query: dict[str, list[str]], name: str, default: int) -> int:
    values = query.get(name)
    if not values:
        return default
    try:
        return int(values[0])
    except ValueError as exc:
        raise TransportError(f"Invalid {name} {values[0]!r}") from exc


def _envelope(entry: StreamEntry) -> tuple[bytes, dict[str, str], str | None, str | None]:
    fields = entry.fields
    raw_headers = _field(fields, "headers")
    content_type = _text(_field(fields, "content_type"))
    key = _text(_field(fields, "key"))
    return (
        _as_bytes(_field(fields, "body")),
        _headers(raw_headers),
        content_type or None,
        key or None,
    )


def _field(fields: Mapping[bytes, bytes], name: str) -> bytes:
    value = fields.get(name.encode())
    if value is None:
        return b""
    return value


def _headers(raw: bytes) -> dict[str, str]:
    if not raw:
        return {}
    try:
        loaded = orjson.loads(raw)
    except orjson.JSONDecodeError:
        return {}
    if not isinstance(loaded, dict):
        return {}
    parsed: dict[str, str] = {}
    for key, value in loaded.items():
        parsed[str(key)] = value if isinstance(value, str) else str(value)
    return parsed


def _text(value: bytes) -> str:
    if not value:
        return ""
    return value.decode()


def _as_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return str(value).encode()


def create_transport(url: str) -> RedisTransport:
    """Build ``RedisTransport`` from a ``redis://`` URL."""

    parsed = urllib.parse.urlparse(url)
    if not parsed.scheme:
        raise TransportError(f"Transport URL {url!r} has no scheme")
    if parsed.scheme != "redis":
        raise TransportError(
            f"RedisTransport URL scheme must be 'redis', got {parsed.scheme!r}",
        )
    return RedisTransport(parsed)


__all__ = ("RedisTransport", "create_transport")
