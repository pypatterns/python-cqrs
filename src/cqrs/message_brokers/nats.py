"""
NATS JetStream transport.

[AI Instruction]
``create_transport("nats://host:4222")`` builds ``NATSTransport``. The
``topic`` is the JetStream subject, unchanged. ``key`` is a header. The
subject is not suffixed with the key, so ``ordering_per_key`` is false.
``group`` is the durable consumer name: the same group competes, a different
group is another durable on that subject. Dead letters are republished to
``{topic}.dlq`` (``native_dlq`` is false). Install the ``nats`` extra
(``nats-py``). Two transports on one server need different ``subject_prefix``
values. There is no module-level bus, no ``__bool__``, and no ``__len__``.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import urllib.parse
from typing import ClassVar

from cqrs.adapters.nats import JetStreamConsumer, JetStreamMessage, JetStreamSession
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

_CONTENT_TYPE_HEADER = "cqrs-content-type"
_KEY_HEADER = "cqrs-key"
_ATTEMPT_HEADER = "cqrs-delivery-attempt"
_INVALID_DURABLE = set(">*. /\\\t\r\n")
_FETCH_TIMEOUT = 0.2


class _Parsed:
    def __init__(
        self,
        headers: dict[str, str],
        content_type: str | None,
        key: str | None,
        attempt_base: int | None,
    ) -> None:
        self.headers = headers
        self.content_type = content_type
        self.key = key
        self.attempt_base = attempt_base


class _Slot:
    def __init__(self, handler: IncomingHandler, options: SubscribeOptions) -> None:
        self.handler = handler
        self.options = options
        self.active = True
        self.tasks: set[asyncio.Task[None]] = set()


class _KeyUse:
    """A per-key lock plus the tasks waiting on it or holding it."""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.users = 0


class _Group:
    def __init__(self, topic: str, durable: str, wire_subject: str, stream: str) -> None:
        self.topic = topic
        self.durable = durable
        self.wire_subject = wire_subject
        self.stream = stream
        self.slots: list[_Slot] = []
        self.cursor = 0
        self.key_locks: dict[str, _KeyUse] = {}
        self.outstanding = 0
        self.running = 0
        self.delivery_limit: int | None = None
        self.tasks: set[asyncio.Task[None]] = set()
        self.loop_task: asyncio.Task[None] | None = None
        self.consumer: JetStreamConsumer | None = None
        self.space = asyncio.Event()
        self.ready = asyncio.Event()


class _NATSSubscription(Subscription):
    def __init__(self, transport: NATSTransport, group: _Group, slot: _Slot) -> None:
        self._transport = transport
        self._group = group
        self._slot = slot

    async def close(self) -> None:
        if not self._slot.active:
            return
        self._slot.active = False
        pending = [task for task in self._slot.tasks if not task.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await self._transport._deactivate(self._group)


class _NATSIncoming(IncomingMessage):
    def __init__(
        self,
        transport: NATSTransport,
        group: _Group,
        slot: _Slot,
        wire: JetStreamMessage,
        parsed: _Parsed,
    ) -> None:
        self._transport = transport
        self._group = group
        self._slot = slot
        self._wire = wire
        self._parsed = parsed
        self.settled = False
        self._dlq_published = False

    @property
    def topic(self) -> str:
        return self._group.topic

    @property
    def body(self) -> bytes:
        return self._wire.data

    @property
    def headers(self) -> dict[str, str]:
        return self._parsed.headers

    @property
    def content_type(self) -> str | None:
        return self._parsed.content_type

    @property
    def key(self) -> str | None:
        return self._parsed.key

    @property
    def delivery_attempt(self) -> int:
        count = self._wire.delivery_count
        if self._parsed.attempt_base is None:
            return count
        return self._parsed.attempt_base + count - 1

    async def ack(self) -> None:
        if self.settled:
            return
        self.settled = True
        await self._wire.ack()
        self._transport._settle(self._group)

    async def nack(self, *, requeue: bool = True) -> None:
        if self.settled:
            return
        limit = self._group.delivery_limit
        if limit is None:
            limit = self._slot.options.max_deliveries
        if requeue and self.delivery_attempt < limit:
            await self._wire.nak()
            self.settled = True
            return
        try:
            if not self._dlq_published:
                await self._transport._dead_letter(self)
                self._dlq_published = True
        except Exception:
            logger.exception("nats dead-letter publish failed for topic %s", self.topic)
            if self.delivery_attempt < limit:
                await self._wire.nak()
                self.settled = True
                return
            # JetStream will not redeliver after the durable's limit. Keep the
            # original unacked and let ``drain`` retry the copy without running
            # the handler again.
            self._transport._hold(self)
            return
        try:
            await self._wire.term()
        except Exception:
            logger.exception("nats term failed for topic %s", self.topic)
            self._transport._hold(self)
            return
        self.settled = True
        self._transport._settle(self._group)


class NATSTransport(Transport):
    """
    JetStream publish and subscribe.

    ``url`` is a ``nats://`` URL or the parsed URL from ``create_transport``.
    ``subject_prefix`` is prepended to every subject and removed before the
    handler sees ``topic``. A missing trailing dot is added. An empty prefix
    publishes the topic unchanged. On one shared server, give each transport
    its own prefix so their subjects do not overlap.

    The same ``(topic, group)`` shares one durable. This process also runs one
    handler at a time for a given key so a single subscription stays ordered.
    That lock is not shared with another process, and the subject is not
    rewritten, so ``ordering_per_key`` stays false. Do not wait, inside a
    handler, for a later message that uses the same key.

    ``nack(requeue=False)`` and a requeue past ``max_deliveries`` republish
    the body to ``{topic}.dlq`` and ``term`` the original. Handler exceptions
    are a requeue and do not escape ``send_message``. ``drain`` waits until
    those deliveries settle. ``close`` stops consumers.

    There is no ``__bool__`` and no ``__len__``.
    """

    capabilities: ClassVar[TransportCapabilities] = TransportCapabilities(
        ordering_per_key=False,
        native_dlq=False,
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
        self._groups: dict[tuple[str, str], _Group] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._recover: list[_NATSIncoming] = []
        self._recover_error: Exception | None = None
        self._idle = asyncio.Event()
        self._idle.set()

    async def start(self) -> None:
        if self._closed:
            raise TransportClosed("start")
        await self._session.connect()

    async def send_message(self, message: Message) -> None:
        if self._closed:
            raise TransportClosed("send_message")
        await self._publish(message, attempt=None)

    async def subscribe(
        self,
        topic: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> Subscription:
        if self._closed:
            raise TransportClosed("subscribe")
        self._check_options(options)
        durable = _durable_name(options.group)
        await self._session.connect()
        if self._closed:
            raise TransportClosed("subscribe")
        wire = self._wire_subject(topic)
        stream = _stream_name(wire)
        await self._session.ensure_stream(stream, wire)
        group = self._ensure_group(topic, durable, wire, stream)
        slot = _Slot(handler, options)
        group.slots.append(slot)
        if group.delivery_limit is None:
            group.delivery_limit = options.max_deliveries
        if group.loop_task is None or group.loop_task.done():
            group.ready = asyncio.Event()
            group.loop_task = asyncio.create_task(self._fetch_loop(group, options))
            self._track(group.loop_task)
        await self._wait_until_consuming(group)
        if self._closed:
            raise TransportClosed("subscribe")
        return _NATSSubscription(self, group, slot)

    async def drain(self, timeout: float = 5) -> None:
        """
        Wait until subscribed deliveries have been acked or dead-lettered.

        Raises ``TimeoutError`` with the topic, group, outstanding count, and
        in-flight count.
        """

        await self._retry_recoveries()
        if self._recover:
            detail = f": {self._recover_error}" if self._recover_error is not None else ""
            raise TransportError(
                "dead-letter publish failed and the JetStream message is still unacknowledged" + detail,
            )
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
        for group in self._groups.values():
            for slot in group.slots:
                slot.active = False
        tasks = [task for task in list(self._tasks) if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for group in self._groups.values():
            group.outstanding = 0
            group.running = 0
        self._note_progress()
        await self._session.close(delete_streams=self._delete_stream_on_close)

    async def _publish(self, message: Message, *, attempt: int | None) -> None:
        if self._closed:
            raise TransportClosed("send_message")
        await self._session.connect()
        if self._closed:
            raise TransportClosed("send_message")
        wire = self._wire_subject(message.topic)
        await self._session.ensure_stream(_stream_name(wire), wire)
        matched = [group for group in self._groups.values() if group.topic == message.topic and _group_active(group)]
        for group in matched:
            group.outstanding += 1
        if matched:
            self._note_work()
        try:
            await self._session.publish(
                wire,
                message_wire_bytes(message),
                _outgoing_headers(message, attempt),
            )
        except Exception:
            for group in matched:
                if group.outstanding > 0:
                    group.outstanding -= 1
            self._note_progress()
            raise

    async def _dead_letter(self, incoming: _NATSIncoming) -> None:
        topic = incoming._slot.options.dead_letter_topic or f"{incoming.topic}.dlq"
        headers = dict(incoming.headers) or None
        await self._publish(
            Message(
                message_name="dead_letter",
                topic=topic,
                payload=incoming.body,
                payload_bytes=incoming.body,
                content_type=incoming.content_type,
                headers=headers,
                key=incoming.key,
            ),
            attempt=incoming.delivery_attempt,
        )

    async def _fetch_loop(self, group: _Group, options: SubscribeOptions) -> None:
        try:
            consumer = await self._session.pull(
                subject=group.wire_subject,
                stream=group.stream,
                durable=group.durable,
                start_from=options.start_from,
                max_deliver=group.delivery_limit or options.max_deliveries,
            )
            try:
                reported = await consumer.max_deliver()
            except Exception:
                logger.debug("nats consumer info failed for topic %s", group.topic, exc_info=True)
                reported = None
            if reported is not None:
                group.delivery_limit = reported
        except asyncio.CancelledError:
            group.ready.set()
            raise
        except Exception:
            group.ready.set()
            raise
        group.consumer = consumer
        group.ready.set()
        try:
            while not self._closed and _group_active(group):
                while self._at_capacity(group) and not self._closed and _group_active(group):
                    group.space.clear()
                    if not self._at_capacity(group) or self._closed or not _group_active(group):
                        break
                    await group.space.wait()
                if self._closed or not _group_active(group):
                    return
                try:
                    messages = await consumer.fetch(timeout=_FETCH_TIMEOUT)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("nats fetch failed for topic %s", group.topic)
                    await asyncio.sleep(_FETCH_TIMEOUT)
                    continue
                for message in messages:
                    if self._closed or not _group_active(group):
                        await self._drop(group, message)
                        continue
                    self._start(group, message)
        except asyncio.CancelledError:
            raise

    def _start(self, group: _Group, message: JetStreamMessage) -> None:
        slot = self._pick(group)
        if slot is None:
            task = asyncio.create_task(self._release_unassigned(group, message))
            self._watch(group, None, task)
            return
        task = asyncio.create_task(self._dispatch(group, slot, message))
        self._watch(group, slot, task)

    async def _release_unassigned(self, group: _Group, message: JetStreamMessage) -> None:
        await self._drop(group, message)

    async def _drop(self, group: _Group, message: JetStreamMessage) -> None:
        try:
            await message.term()
        except Exception:
            logger.debug("nats term failed for topic %s", group.topic, exc_info=True)
        finally:
            self._settle(group)

    async def _dispatch(self, group: _Group, slot: _Slot, message: JetStreamMessage) -> None:
        try:
            parsed = _parse_delivery(message)
            if parsed.key is None:
                await self._run(group, slot, message, parsed)
                return
            entry = group.key_locks.get(parsed.key)
            if entry is None:
                entry = _KeyUse()
                group.key_locks[parsed.key] = entry
            entry.users += 1
            try:
                async with entry.lock:
                    if self._closed:
                        await self._return_pending(message)
                        return
                    target = slot if slot.active else self._pick(group)
                    if target is None:
                        await self._return_pending(message)
                        return
                    await self._run(group, target, message, parsed)
            finally:
                entry.users -= 1
                if entry.users == 0 and group.key_locks.get(parsed.key) is entry:
                    group.key_locks.pop(parsed.key, None)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "delivery failed for topic %s group %s",
                group.topic,
                group.durable,
            )

    async def _run(
        self,
        group: _Group,
        slot: _Slot,
        message: JetStreamMessage,
        parsed: _Parsed,
    ) -> None:
        incoming = _NATSIncoming(self, group, slot, message, parsed)
        group.running += 1
        try:
            try:
                await slot.handler(incoming)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "subscriber %s failed for topic %s",
                    slot.options.group,
                    group.topic,
                )
                if not incoming.settled:
                    await incoming.nack(requeue=True)
        finally:
            group.running = max(0, group.running - 1)
            self._note_progress()

    async def _wait_until_consuming(self, group: _Group) -> None:
        await group.ready.wait()
        task = group.loop_task
        if task is not None and task.done() and not task.cancelled():
            error = task.exception()
            if error is not None:
                raise error

    async def _deactivate(self, group: _Group) -> None:
        if _group_active(group):
            return
        task = group.loop_task
        if task is None or task.done() or task is asyncio.current_task():
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    def _ensure_group(self, topic: str, durable: str, wire: str, stream: str) -> _Group:
        key = (topic, durable)
        group = self._groups.get(key)
        if group is None:
            group = _Group(topic, durable, wire, stream)
            self._groups[key] = group
        return group

    def _pick(self, group: _Group) -> _Slot | None:
        active = [slot for slot in group.slots if slot.active]
        if not active:
            return None
        slot = active[group.cursor % len(active)]
        group.cursor += 1
        return slot

    def _at_capacity(self, group: _Group) -> bool:
        limit = 0
        for slot in group.slots:
            if slot.active:
                limit += slot.options.concurrency * slot.options.prefetch
        if limit < 1:
            return True
        running = sum(1 for task in group.tasks if not task.done())
        return running >= limit

    def _watch(self, group: _Group, slot: _Slot | None, task: asyncio.Task[None]) -> None:
        self._tasks.add(task)
        group.tasks.add(task)
        if slot is not None:
            slot.tasks.add(task)

        def _done(finished: asyncio.Task[None]) -> None:
            self._tasks.discard(finished)
            group.tasks.discard(finished)
            if slot is not None:
                slot.tasks.discard(finished)
            group.space.set()
            self._note_progress()

        task.add_done_callback(_done)
        self._note_work()

    def _track(self, task: asyncio.Task[None]) -> None:
        self._tasks.add(task)

        def _done(finished: asyncio.Task[None]) -> None:
            self._tasks.discard(finished)

        task.add_done_callback(_done)

    def _hold(self, incoming: _NATSIncoming) -> None:
        if incoming not in self._recover:
            self._recover.append(incoming)
        self._note_work()

    async def _retry_recoveries(self) -> None:
        """Copy held finals to the dead-letter subject without running handlers."""

        pending: list[_NATSIncoming] = []
        for incoming in self._recover:
            try:
                if not incoming._dlq_published:
                    await self._dead_letter(incoming)
                    incoming._dlq_published = True
                await incoming._wire.term()
            except Exception as exc:
                self._recover_error = exc
                pending.append(incoming)
                continue
            incoming.settled = True
            self._settle(incoming._group)
        self._recover = pending
        if not pending:
            self._recover_error = None

    async def _return_pending(self, message: JetStreamMessage) -> None:
        """Leave the delivery eligible for another subscriber. Do not term it."""

        try:
            await message.nak()
        except Exception:
            logger.debug("nats nak failed", exc_info=True)

    def _settle(self, group: _Group) -> None:
        if group.outstanding > 0:
            group.outstanding -= 1
        self._note_progress()

    def _wire_subject(self, topic: str) -> str:
        return f"{self.subject_prefix}{topic}"

    def _check_options(self, options: SubscribeOptions) -> None:
        if options.start_from not in ("latest", "earliest"):
            raise UnsupportedCapability(
                f"start_from={options.start_from!r} is not supported",
            )
        if options.concurrency < 1 or options.prefetch < 1 or options.max_deliveries < 1:
            raise UnsupportedCapability(
                "concurrency, prefetch, and max_deliveries must be >= 1",
            )

    def _is_idle(self) -> bool:
        for group in self._groups.values():
            if group.outstanding or group.running:
                return False
            if any(not task.done() for task in group.tasks):
                return False
        return True

    def _note_work(self) -> None:
        self._idle.clear()

    def _note_progress(self) -> None:
        if self._is_idle():
            self._idle.set()

    def _drain_message(self) -> str:
        parts: list[str] = []
        for group in self._groups.values():
            in_flight = group.running + sum(1 for task in group.tasks if not task.done())
            if group.outstanding or in_flight:
                parts.append(
                    f"topic={group.topic} group={group.durable} "
                    f"queue_length={group.outstanding} in_flight={in_flight}",
                )
        if not parts:
            parts.append("topic=* group=* queue_length=0 in_flight=0")
        return "drain timed out: " + "; ".join(parts)


def _read_url(url: str | urllib.parse.ParseResult) -> tuple[str, dict[str, list[str]]]:
    parsed = url if isinstance(url, urllib.parse.ParseResult) else urllib.parse.urlparse(url)
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


def _outgoing_headers(message: Message, attempt: int | None) -> dict[str, str] | None:
    headers: dict[str, str] = {}
    if message.headers:
        headers.update(message.headers)
    if message.content_type is not None:
        headers[_CONTENT_TYPE_HEADER] = message.content_type
    if message.key is not None:
        headers[_KEY_HEADER] = message.key
    if attempt is not None:
        headers[_ATTEMPT_HEADER] = str(attempt)
    if not headers:
        return None
    return headers


def _parse_delivery(message: JetStreamMessage) -> _Parsed:
    headers = dict(message.headers)
    content_type = headers.pop(_CONTENT_TYPE_HEADER, None)
    key = headers.pop(_KEY_HEADER, None)
    raw_attempt = headers.pop(_ATTEMPT_HEADER, None)
    attempt_base: int | None = None
    if raw_attempt is not None:
        try:
            parsed = int(raw_attempt)
        except ValueError:
            parsed = 0
        if parsed >= 1:
            attempt_base = parsed
    return _Parsed(headers, content_type, key, attempt_base)


def _group_active(group: _Group) -> bool:
    return any(slot.active for slot in group.slots)


__all__ = ("NATSTransport",)
