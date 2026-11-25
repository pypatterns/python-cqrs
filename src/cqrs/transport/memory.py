"""
In-memory transport for tests and a single local process.

[AI Instruction]
Construct ``InMemoryTransport`` per test. It does not share messages with
another instance. ``queued`` (the default) returns from ``send_message``
after the enqueue. ``inline`` returns after the handlers for this message
have finished. Call ``drain`` to wait for handler tasks. Call ``close`` to
stop.

This is a queue and one task per subscription. It does not order by key,
count deliveries, or run a worker pool. ``nack(requeue=True)`` puts the
bytes back on that group's queue. In ``inline`` mode the same send waits
``redelivery_delay`` and calls the handler again, without nesting the
call. ``nack(requeue=False)`` publishes them once to the dead-letter
topic. ``delivery_attempt`` is always 1.
"""

from __future__ import annotations

import asyncio
import logging
import urllib.parse
import uuid
from typing import ClassVar, Literal

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

Delivery = Literal["queued", "inline"]


class _Record:
    def __init__(
        self,
        body: bytes,
        headers: dict[str, str],
        content_type: str | None,
        key: str | None,
        token: str,
    ) -> None:
        self.body = body
        self.headers = headers
        self.content_type = content_type
        self.key = key
        self.token = token

    def copy(self) -> _Record:
        return _Record(self.body, dict(self.headers), self.content_type, self.key, self.token)


class _Group:
    def __init__(self, topic: str, name: str) -> None:
        self.topic = topic
        self.name = name
        self.queue: asyncio.Queue[_Record] = asyncio.Queue()
        self.subscribers: list[_Subscription] = []
        self.start_from: str | None = None

    def active(self) -> list[_Subscription]:
        return [subscriber for subscriber in self.subscribers if subscriber.active]


class _Subscription(Subscription):
    def __init__(
        self,
        transport: InMemoryTransport,
        group: _Group,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> None:
        self._transport = transport
        self.group = group
        self.handler = handler
        self.options = options
        self.active = True
        self.inflight = 0
        self.task: asyncio.Task[None] | None = None
        self.handlers: set[asyncio.Task[None]] = set()

    async def close(self) -> None:
        await self._transport._close_subscription(self)


class _Incoming(IncomingMessage):
    def __init__(
        self,
        transport: InMemoryTransport,
        group: _Group,
        subscription: _Subscription,
        record: _Record,
    ) -> None:
        self._transport = transport
        self._group = group
        self._subscription = subscription
        self._record = record
        self._settled = False
        self._retry = False
        self._headers = dict(record.headers)

    @property
    def topic(self) -> str:
        return self._group.topic

    @property
    def body(self) -> bytes:
        return self._record.body

    @property
    def headers(self) -> dict[str, str]:
        return self._headers

    @property
    def content_type(self) -> str | None:
        return self._record.content_type

    @property
    def key(self) -> str | None:
        return self._record.key

    @property
    def delivery_attempt(self) -> int:
        return 1

    async def ack(self) -> None:
        if self._settled:
            return
        self._settled = True
        self._transport._watch.settle(self._record.token)

    async def nack(self, *, requeue: bool = True) -> None:
        if self._settled:
            return
        self._settled = True
        if requeue:
            if self._transport.delivery == "inline":
                self._retry = True
                return
            self._transport._requeue(self._group, self._record.copy())
            return
        try:
            await self._transport._dead_letter(self._record, self._subscription)
        except asyncio.CancelledError:
            self._transport._watch.settle(self._record.token)
            raise
        self._transport._watch.settle(self._record.token)


class InMemoryTransport(Transport):
    """
    Process-local queue.

    Two instances do not see each other's messages. One task per
    subscription reads that group's queue. A second subscription with the
    same topic and group is another task on the same queue, so they compete.
    A different group gets its own copy. ``key`` is delivered as given and
    does not select a lane. There is no ``__bool__`` and no ``__len__``.
    """

    capabilities: ClassVar[TransportCapabilities] = TransportCapabilities(
        ordering_per_key=False,
        native_dlq=False,
        delayed_redelivery=True,
        headers=True,
        topic="logical name shared by the publisher and every subscriber",
    )

    def __init__(
        self,
        *,
        delivery: Delivery = "queued",
        redelivery_delay: float = 0,
    ) -> None:
        if delivery not in ("queued", "inline"):
            raise UnsupportedCapability(
                f"InMemoryTransport does not support delivery={delivery!r}",
            )
        if redelivery_delay < 0:
            raise UnsupportedCapability("redelivery_delay must be >= 0")
        self.delivery: Delivery = delivery
        self.redelivery_delay = redelivery_delay
        self._closed = False
        self._groups: dict[tuple[str, str], _Group] = {}
        self._log: dict[str, list[_Record]] = {}
        self._watch = HandlerWatch()
        self._tasks: set[asyncio.Task[None]] = set()

    async def send_message(self, message: Message) -> None:
        self._check_open("send_message")
        record = _record(message)
        self._log.setdefault(message.topic, []).append(record.copy())
        groups = self._groups_for(message.topic)
        if not groups:
            return
        if self.delivery == "inline":
            awaits: list[asyncio.Task[None]] = []
            for group in groups:
                active = group.active()
                if not active:
                    continue
                self._watch.expect(record.token)
                task = asyncio.create_task(self._deliver(active[0], group, record.copy()))
                awaits.append(self._own(active[0], task))
            if awaits:
                await asyncio.gather(*awaits)
            return
        for group in groups:
            self._watch.expect(record.token)
            group.queue.put_nowait(record.copy())

    async def subscribe(
        self,
        topic: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> Subscription:
        self._check_open("subscribe")
        require_options(options)
        key = (topic, options.group)
        group = self._groups.get(key)
        created = group is None
        if group is None:
            group = _Group(topic, options.group)
            group.start_from = options.start_from
            self._groups[key] = group
        elif group.start_from != options.start_from:
            raise UnsupportedCapability(
                f"group {options.group!r} on {topic!r} already consumes from {group.start_from}",
            )
        subscription = _Subscription(self, group, handler, options)
        group.subscribers.append(subscription)
        if self.delivery == "queued":
            subscription.task = self._track(asyncio.create_task(self._read(subscription)))
        if created and options.start_from == "earliest":
            await self._replay(subscription, group)
        return subscription

    async def drain(self, timeout: float = 5) -> None:
        """Wait until queued records have been acked or dead-lettered."""

        await self._watch.wait(timeout, self._describe)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for group in self._groups.values():
            for subscription in group.subscribers:
                subscription.active = False
        tasks = [task for task in self._tasks if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._watch.abandon_all()
        self._watch.inflight = 0
        self._watch.poke()

    async def _close_subscription(self, subscription: _Subscription) -> None:
        if not subscription.active:
            return
        subscription.active = False
        pending: list[asyncio.Task[None]] = []
        task = subscription.task
        if task is not None and not task.done():
            task.cancel()
            pending.append(task)
        for handler in list(subscription.handlers):
            if handler is not task and not handler.done():
                handler.cancel()
                pending.append(handler)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        subscription.task = None
        group = subscription.group
        if group.active():
            return
        while not group.queue.empty():
            self._watch.abandon(group.queue.get_nowait().token)

    async def _read(self, subscription: _Subscription) -> None:
        group = subscription.group
        try:
            while subscription.active and not self._closed:
                record = await group.queue.get()
                if not subscription.active or self._closed:
                    if group.active():
                        group.queue.put_nowait(record)
                    else:
                        self._watch.abandon(record.token)
                    return
                try:
                    await self._deliver(subscription, group, record)
                except asyncio.CancelledError:
                    raise
        except asyncio.CancelledError:
            return

    async def _deliver(self, subscription: _Subscription, group: _Group, record: _Record) -> None:
        subscription.inflight += 1
        self._watch.begin()
        try:
            while True:
                incoming = _Incoming(self, group, subscription, record)
                try:
                    await subscription.handler(incoming)
                except asyncio.CancelledError:
                    if not incoming._settled:
                        self._hand_off(subscription, group, record)
                    raise
                except Exception:
                    logger.exception(
                        "subscriber %s failed for topic %s",
                        subscription.options.group,
                        group.topic,
                    )
                    if not incoming._settled:
                        await incoming.nack(requeue=False)
                if not incoming._retry:
                    return
                try:
                    if self.redelivery_delay > 0:
                        await asyncio.sleep(self.redelivery_delay)
                except asyncio.CancelledError:
                    self._hand_off(subscription, group, record)
                    raise
                if self._closed or not subscription.active:
                    self._hand_off(subscription, group, record)
                    return
        finally:
            subscription.inflight -= 1
            self._watch.end()

    async def _replay(self, subscription: _Subscription, group: _Group) -> None:
        past = list(self._log.get(group.topic, ()))
        if not past:
            return
        if self.delivery == "inline":
            for record in past:
                self._watch.expect(record.token)
                await self._deliver(subscription, group, record.copy())
            return
        for record in past:
            self._watch.expect(record.token)
            group.queue.put_nowait(record.copy())

    def _requeue(self, group: _Group, record: _Record) -> None:
        if self.redelivery_delay <= 0 and not self._closed and group.active():
            group.queue.put_nowait(record)
            return
        self._track(asyncio.create_task(self._requeue_later(group, record)))

    async def _requeue_later(self, group: _Group, record: _Record) -> None:
        try:
            if self.redelivery_delay > 0:
                await asyncio.sleep(self.redelivery_delay)
            if self._closed or not group.active():
                self._watch.abandon(record.token)
                return
            group.queue.put_nowait(record)
        except asyncio.CancelledError:
            self._watch.abandon(record.token)
            raise

    async def _dead_letter(self, record: _Record, subscription: _Subscription) -> None:
        topic = subscription.options.dead_letter_topic or f"{subscription.group.topic}.dlq"
        await self.send_message(
            Message(
                message_name="dead_letter",
                topic=topic,
                payload=record.body,
                payload_bytes=record.body,
                headers=dict(record.headers) or None,
                content_type=record.content_type,
                key=record.key,
            ),
        )

    def _hand_off(self, subscription: _Subscription, group: _Group, record: _Record) -> None:
        others = [item for item in group.active() if item is not subscription]
        if not others:
            self._watch.abandon(record.token)
            return
        if self.delivery == "inline":
            self._own(others[0], asyncio.create_task(self._deliver(others[0], group, record)))
            return
        self._requeue(group, record)

    def _groups_for(self, topic: str) -> list[_Group]:
        return [group for group in self._groups.values() if group.topic == topic and group.active()]

    def _check_open(self, operation: str) -> None:
        if self._closed:
            raise TransportClosed(operation)

    def _describe(self) -> str:
        parts: list[str] = []
        for group in self._groups.values():
            inflight = sum(subscriber.inflight for subscriber in group.subscribers if subscriber.active)
            queued = group.queue.qsize()
            if queued or inflight:
                parts.append(
                    f"topic={group.topic} group={group.name} queue_length={queued} in_flight={inflight}",
                )
        if not parts:
            parts.append(f"topic=* group=* queue_length=0 in_flight={self._watch.inflight}")
        return "drain timed out: " + "; ".join(parts)

    def _track(self, task: asyncio.Task[None]) -> asyncio.Task[None]:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _own(self, subscription: _Subscription, task: asyncio.Task[None]) -> asyncio.Task[None]:
        self._track(task)
        subscription.handlers.add(task)
        task.add_done_callback(subscription.handlers.discard)
        return task


def _record(message: Message) -> _Record:
    return _Record(
        message_wire_bytes(message),
        dict(message.headers) if message.headers else {},
        message.content_type,
        message.key,
        uuid.uuid4().hex,
    )


def create_transport(url: str) -> InMemoryTransport:
    """
    Build an ``InMemoryTransport`` from a ``memory://`` URL.

    ``memory://`` and ``memory://?delivery=inline&redelivery_delay=0`` are
    supported. A wrong scheme or an invalid query raises ``TransportError`` or
    ``UnsupportedCapability``.
    """

    parsed = urllib.parse.urlparse(url)
    if not parsed.scheme:
        raise TransportError(f"Transport URL {url!r} has no scheme")
    if parsed.scheme != "memory":
        raise TransportError(
            f"InMemoryTransport URL scheme must be 'memory', got {parsed.scheme!r}",
        )
    query = urllib.parse.parse_qs(parsed.query)
    delivery = query.get("delivery", ["queued"])[0]
    if delivery not in ("queued", "inline"):
        raise UnsupportedCapability(
            f"InMemoryTransport does not support delivery={delivery!r}",
        )
    raw_delay = query.get("redelivery_delay", ["0"])[0]
    try:
        delay = float(raw_delay)
    except ValueError as exc:
        raise TransportError(f"Invalid redelivery_delay {raw_delay!r}") from exc
    if delivery == "inline":
        return InMemoryTransport(delivery="inline", redelivery_delay=delay)
    return InMemoryTransport(delivery="queued", redelivery_delay=delay)


__all__ = ("InMemoryTransport", "create_transport")
