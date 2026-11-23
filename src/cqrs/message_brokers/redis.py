"""
Redis Streams transport.

[AI Instruction]
``create_transport("redis://localhost:6379/0")`` builds ``RedisTransport``.
The factory calls ``RedisTransport(parsed_url)``. Publish with ``send_message``.
Subscribe with ``subscribe``. There is no ``publish`` alias, no ``__bool__``,
and no ``__len__``. Do not import FastAPI or FastStream.

``topic`` is the Redis stream key. ``group`` is the consumer group. A second
subscriber with the same topic and group competes for each message. A
different group has its own cursor, so it receives its own copy. ``message.key``
is stored as a stream field and does not order delivery. Dead letters are a
republish to ``{topic}.dlq`` (or ``dead_letter_topic``). ``nack(requeue=True)``
claims the pending id so the delivery count from ``XPENDING`` increases.
Messages left pending past ``claim_idle_ms`` are reclaimed with ``XAUTOCLAIM``.

Pass ``prefix`` when two transports share one Redis and must not see each
other's streams. The prefix is prepended to the stream key only.
``IncomingMessage.topic`` stays the topic the caller used. State lives on the
instance, not in a module global.
"""

from __future__ import annotations

import asyncio
import logging
import urllib.parse
from collections.abc import Mapping
from typing import ClassVar

import orjson

from cqrs.adapters.redis import RedisStreamClient, StreamEntry
from cqrs.message_brokers.protocol import Message
from cqrs.serializers.default import message_wire_bytes
from cqrs.transport.errors import TransportClosed, TransportError, UnsupportedCapability
from cqrs.transport.protocol import (
    IncomingHandler,
    IncomingMessage,
    SubscribeOptions,
    Subscription,
    Transport,
    TransportCapabilities,
)

logger = logging.getLogger("cqrs")

_OWN_QUERY = frozenset({"prefix", "claim_idle_ms", "block_ms"})
_DEFAULT_CLAIM_IDLE_MS = 30_000
_DEFAULT_BLOCK_MS = 200


class _Envelope:
    def __init__(
        self,
        message_id: bytes,
        body: bytes,
        headers: dict[str, str],
        content_type: str | None,
        key: str | None,
        stored_attempt: int,
    ) -> None:
        self.message_id = message_id
        self.body = body
        self.headers = headers
        self.content_type = content_type
        self.key = key
        # Attempt number to report on the first delivery of this stream entry.
        # A dead-letter republish stores the attempt that failed. Later
        # claims add ``XPENDING.times_delivered - 1``.
        self.stored_attempt = stored_attempt


class _Group:
    def __init__(self, topic: str, name: str, stream: str) -> None:
        self.topic = topic
        self.name = name
        self.stream = stream
        self.subscriptions: list[_RedisSubscription] = []
        self.retry: asyncio.Queue[_Envelope] = asyncio.Queue()
        self.queued: set[bytes] = set()
        self.claimer: asyncio.Task[None] | None = None


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
        self.workers: list[asyncio.Task[None]] = []

    async def close(self) -> None:
        if not self.active:
            return
        self.active = False
        workers = list(self.workers)
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        self._transport._stop_claimer_if_idle(self.group)


class _RedisIncoming(IncomingMessage):
    def __init__(
        self,
        transport: RedisTransport,
        group: _Group,
        subscription: _RedisSubscription,
        envelope: _Envelope,
        consumer: str,
        delivery_attempt: int,
    ) -> None:
        self._transport = transport
        self._group = group
        self._subscription = subscription
        self._envelope = envelope
        self._consumer = consumer
        self._delivery_attempt = delivery_attempt
        self._headers = dict(envelope.headers)
        self._settled = False

    @property
    def topic(self) -> str:
        return self._group.topic

    @property
    def body(self) -> bytes:
        return self._envelope.body

    @property
    def headers(self) -> Mapping[str, str]:
        return self._headers

    @property
    def content_type(self) -> str | None:
        return self._envelope.content_type

    @property
    def key(self) -> str | None:
        return self._envelope.key

    @property
    def delivery_attempt(self) -> int:
        return self._delivery_attempt

    async def ack(self) -> None:
        if self._settled:
            return
        self._settled = True
        if self._transport._closed:
            return
        await self._transport._client.ack(self._group.stream, self._group.name, self._envelope.message_id)

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
        await self._transport._requeue(self)


class RedisTransport(Transport):
    """
    Redis Streams transport.

    Construct it with the parsed URL from ``create_transport``, or call
    ``RedisTransport(parsed)`` directly. ``prefix`` isolates stream keys when
    several transports share one Redis. Do not define ``__bool__`` or ``__len__``.
    Call ``drain`` to wait until handlers for published messages have finished.
    Call ``close`` to stop readers. ``send_message`` and ``subscribe`` after
    ``close`` raise ``TransportClosed``.
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
        parsed: urllib.parse.ParseResult,
        *,
        prefix: str | None = None,
        claim_idle_ms: int | None = None,
        block_ms: int | None = None,
    ) -> None:
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
        await self._publish(message, delivery_attempt=1)

    async def subscribe(
        self,
        topic: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> Subscription:
        if self._closed:
            raise TransportClosed("subscribe")
        _check_options(options)
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
            group.claimer = self._track(asyncio.create_task(self._claim_loop(group)))
        subscription = _RedisSubscription(self, group, handler, options)
        group.subscriptions.append(subscription)
        count = options.prefetch if options.prefetch > 0 else 1
        for index in range(options.concurrency):
            consumer = f"{options.group}-{id(subscription):x}-{index}"
            worker = self._track(
                asyncio.create_task(self._reader(subscription, group, consumer, count)),
            )
            subscription.workers.append(worker)
        return subscription

    async def drain(self, timeout: float = 5) -> None:
        """
        Wait until subscribed groups have no lag, no pending entries, and no
        handlers still running.

        Raises ``TimeoutError`` with the topic, group, lag, and in-flight count.
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

    async def _publish(self, message: Message, *, delivery_attempt: int) -> None:
        if self._closed:
            raise TransportClosed("send_message")
        headers = dict(message.headers) if message.headers else {}
        fields: dict[str, bytes | str] = {
            "body": message_wire_bytes(message),
            "headers": orjson.dumps(headers),
            "content_type": message.content_type or "",
            "delivery_attempt": str(delivery_attempt),
        }
        if message.key is not None:
            fields["key"] = message.key
        await self._client.add(self._stream_key(message.topic), fields)

    async def _dead_letter(self, incoming: _RedisIncoming) -> None:
        topic = incoming._subscription.options.dead_letter_topic or f"{incoming.topic}.dlq"
        message = Message(
            message_name="dead_letter",
            topic=topic,
            payload=incoming.body,
            payload_bytes=bytes(incoming.body),
            content_type=incoming.content_type,
            headers=dict(incoming.headers) or None,
            key=incoming.key,
        )
        try:
            await self._publish(message, delivery_attempt=incoming.delivery_attempt)
        except TransportClosed:
            return
        await self._client.ack(incoming._group.stream, incoming._group.name, incoming._envelope.message_id)

    async def _requeue(self, incoming: _RedisIncoming) -> None:
        claimed: StreamEntry | None = None
        for _ in range(3):
            claimed = await self._client.claim(
                incoming._group.stream,
                incoming._group.name,
                incoming._consumer,
                incoming._envelope.message_id,
            )
            if claimed is not None:
                break
            await asyncio.sleep(0.01)
        if claimed is None:
            logger.error(
                "XCLAIM failed for topic %s group %s id %s",
                incoming.topic,
                incoming._group.name,
                incoming._envelope.message_id,
            )
            return
        self._enqueue(incoming._group, self._envelope(claimed))

    async def _reader(
        self,
        subscription: _RedisSubscription,
        group: _Group,
        consumer: str,
        count: int,
    ) -> None:
        try:
            while subscription.active and not self._closed:
                envelope = self._pop_retry(group)
                if envelope is None:
                    try:
                        fetched = await self._client.read_group(
                            group.stream,
                            group.name,
                            consumer,
                            count=count,
                            block_ms=self._block_ms,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        if self._closed or not subscription.active:
                            return
                        logger.exception(
                            "XREADGROUP failed for topic %s group %s",
                            group.topic,
                            group.name,
                        )
                        await asyncio.sleep(0.05)
                        continue
                    if self._closed or not subscription.active or not fetched:
                        continue
                    envelope = self._envelope(fetched[0])
                    for extra in fetched[1:]:
                        self._enqueue(group, self._envelope(extra))
                await self._deliver(subscription, group, consumer, envelope)
        except asyncio.CancelledError:
            return

    async def _deliver(
        self,
        subscription: _RedisSubscription,
        group: _Group,
        consumer: str,
        envelope: _Envelope,
    ) -> None:
        if self._closed or not subscription.active:
            return
        times = await self._client.times_delivered(group.stream, group.name, envelope.message_id)
        attempt = (envelope.stored_attempt - 1) + times
        incoming = _RedisIncoming(self, group, subscription, envelope, consumer, attempt)
        self._handlers += 1
        try:
            try:
                await subscription.handler(incoming)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "subscriber %s failed for topic %s",
                    subscription.options.group,
                    group.topic,
                )
                if not incoming._settled:
                    await incoming.nack(requeue=True)
        finally:
            self._handlers = max(0, self._handlers - 1)

    async def _claim_loop(self, group: _Group) -> None:
        delay = min(1.0, max(0.05, self._claim_idle_ms / 2000))
        consumer = f"{group.name}-claim"
        try:
            while not self._closed and _active(group):
                await asyncio.sleep(delay)
                if self._closed or not _active(group):
                    return
                try:
                    claimed = await self._client.autoclaim(
                        group.stream,
                        group.name,
                        consumer,
                        self._claim_idle_ms,
                        count=10,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "XAUTOCLAIM failed for topic %s group %s",
                        group.topic,
                        group.name,
                    )
                    continue
                for entry in claimed:
                    self._enqueue(group, self._envelope(entry))
        except asyncio.CancelledError:
            return

    async def _is_drained(self) -> bool:
        if self._handlers or self._retry_waiting():
            self._remember_local()
            return False
        notes: list[str] = []
        for group in self._groups.values():
            if not _active(group):
                continue
            pending, lag = await self._client.backlog(group.stream, group.name)
            queued = group.retry.qsize()
            if pending or lag or queued or self._handlers:
                notes.append(
                    f"topic={group.topic} group={group.name} "
                    f"queue_length={lag + queued} in_flight={pending + self._handlers}",
                )
        if notes or self._handlers or self._retry_waiting():
            if notes:
                self._backlog_note = "; ".join(notes)
            else:
                self._remember_local()
            return False
        return True

    def _remember_local(self) -> None:
        parts: list[str] = []
        for group in self._groups.values():
            queued = group.retry.qsize()
            if queued or self._handlers:
                parts.append(
                    f"topic={group.topic} group={group.name} " f"queue_length={queued} in_flight={self._handlers}",
                )
        if parts:
            self._backlog_note = "; ".join(parts)
        else:
            self._backlog_note = f"topic=* group=* queue_length=0 in_flight={self._handlers}"

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

    def _enqueue(self, group: _Group, envelope: _Envelope) -> None:
        if envelope.message_id in group.queued:
            return
        group.queued.add(envelope.message_id)
        group.retry.put_nowait(envelope)

    def _pop_retry(self, group: _Group) -> _Envelope | None:
        try:
            envelope = group.retry.get_nowait()
        except asyncio.QueueEmpty:
            return None
        group.queued.discard(envelope.message_id)
        return envelope

    def _retry_waiting(self) -> bool:
        return any(not group.retry.empty() for group in self._groups.values())

    def _envelope(self, entry: StreamEntry) -> _Envelope:
        fields = entry.fields
        raw_headers = _field(fields, "headers")
        content_type = _text(_field(fields, "content_type"))
        key = _text(_field(fields, "key"))
        attempt_text = _text(_field(fields, "delivery_attempt")) or "1"
        try:
            stored_attempt = int(attempt_text)
        except ValueError:
            stored_attempt = 1
        if stored_attempt < 1:
            stored_attempt = 1
        return _Envelope(
            message_id=_as_bytes(entry.message_id),
            body=_as_bytes(_field(fields, "body")),
            headers=_headers(raw_headers),
            content_type=content_type or None,
            key=key or None,
            stored_attempt=stored_attempt,
        )

    def _track(self, task: asyncio.Task[None]) -> asyncio.Task[None]:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _stop_claimer_if_idle(self, group: _Group) -> None:
        if _active(group):
            return
        claimer = group.claimer
        if claimer is not None and not claimer.done():
            claimer.cancel()


def _active(group: _Group) -> bool:
    return any(subscription.active for subscription in group.subscriptions)


def _check_options(options: SubscribeOptions) -> None:
    if options.start_from not in ("latest", "earliest"):
        raise UnsupportedCapability(f"start_from={options.start_from!r} is not supported")
    if options.concurrency < 1 or options.prefetch < 1 or options.max_deliveries < 1:
        raise UnsupportedCapability("concurrency, prefetch, and max_deliveries must be >= 1")


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


__all__ = ("RedisTransport",)
