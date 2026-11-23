"""
In-memory transport for tests and a single local process.

[AI Instruction]
Construct ``InMemoryTransport`` per test. It does not share messages with
another instance. ``queued`` (the default) returns from ``send_message``
after the enqueue. ``inline`` returns after this message and anything the
handlers published before they finished have been acked, dead-lettered, or
dropped by ``close``. Call ``drain`` to wait. Call ``close`` to stop.
Handlers must ``ack`` or ``nack``. An exception is a ``nack(requeue=True)``
and does not escape ``send_message``.

The topic log is unbounded. The instance lives for one test or one local
process and keeps every published message until ``close``.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from typing import ClassVar, Literal

from cqrs.message_brokers.protocol import Message
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

Delivery = Literal["queued", "inline"]
_Attempt = Callable[[], Awaitable[None]]

_handling: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "cqrs_transport_handling",
    default=False,
)
_batch_var: contextvars.ContextVar[_Batch | None] = contextvars.ContextVar(
    "cqrs_transport_batch",
    default=None,
)


class _Batch:
    """Unfinished deliveries for one inline ``send_message`` and its children.

    Each ``send_message`` has its own counter. A nested send links its batch
    to the caller's batch, so the caller also waits for that work, but a
    sibling delivery cannot satisfy the nested wait. The nested send does
    not wait for the handler that called it: that handler is still running.
    """

    def __init__(self, parent: _Batch | None = None) -> None:
        self.parent = parent
        self.remaining = 0
        self.progress = asyncio.Event()
        self.progress.set()

    def add(self) -> None:
        self.remaining += 1
        self.progress.clear()
        if self.parent is not None:
            self.parent.add()

    def finish_one(self) -> None:
        if self.remaining <= 0:
            self.remaining = 0
            self.progress.set()
            return
        self.remaining -= 1
        self.progress.set()
        if self.parent is not None:
            self.parent.finish_one()

    def force(self) -> None:
        released = self.remaining
        self.remaining = 0
        self.progress.set()
        if self.parent is None:
            return
        for _ in range(released):
            self.parent.finish_one()

    async def wait_until(self, target: int) -> None:
        while self.remaining > target:
            self.progress.clear()
            if self.remaining <= target:
                return
            await self.progress.wait()


class _Record:
    def __init__(
        self,
        body: bytes,
        headers: dict[str, str],
        content_type: str | None,
        key: str | None,
    ) -> None:
        self.body = body
        self.headers = headers
        self.content_type = content_type
        self.key = key


class _Item:
    def __init__(
        self,
        *,
        body: bytes,
        headers: dict[str, str],
        content_type: str | None,
        key: str | None,
        delivery_attempt: int,
        batch: _Batch | None,
    ) -> None:
        self.body = body
        self.headers = headers
        self.content_type = content_type
        self.key = key
        self.delivery_attempt = delivery_attempt
        self.batch = batch
        self.sub: _Subscription | None = None
        self.completed = False
        self.finished = asyncio.Event()


class _Lane:
    def __init__(self) -> None:
        self.waiting: deque[_Item] = deque()
        self.task: asyncio.Task[None] | None = None


class _Group:
    def __init__(self, topic: str, name: str) -> None:
        self.topic = topic
        self.name = name
        self.queue: asyncio.Queue[_Item] = asyncio.Queue()
        self.subscriptions: list[_Subscription] = []
        self.lanes: dict[str, _Lane] = {}
        self.in_flight = 0
        self.cursor = 0


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
        self.workers: list[asyncio.Task[None]] = []
        self.inflight: set[asyncio.Task[None]] = set()
        self.slots = asyncio.Semaphore(options.concurrency * options.prefetch)

    async def close(self) -> None:
        if not self.active:
            return
        self.active = False
        workers = list(self.workers)
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        pending = [task for task in self.inflight if not task.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        # A replacement can join while the handlers above are still running.
        # The check from before that wait would drop the work it just admitted.
        if any(subscription.active for subscription in self.group.subscriptions):
            return
        while not self.group.queue.empty():
            self._transport._finish_item(self.group.queue.get_nowait())
        for lane in self.group.lanes.values():
            while lane.waiting:
                self._transport._finish_item(lane.waiting.popleft())
        self.group.lanes.clear()


class _MemoryIncoming(IncomingMessage):
    def __init__(
        self,
        transport: InMemoryTransport,
        group: _Group,
        sub: _Subscription,
        item: _Item,
        attempt: _Attempt,
    ) -> None:
        self._transport = transport
        self._group = group
        self._sub = sub
        self._item = item
        self._attempt = attempt
        self._headers = dict(item.headers)
        self.settled = False

    @property
    def topic(self) -> str:
        return self._group.topic

    @property
    def body(self) -> bytes:
        return self._item.body

    @property
    def headers(self) -> dict[str, str]:
        return self._headers

    @property
    def content_type(self) -> str | None:
        return self._item.content_type

    @property
    def key(self) -> str | None:
        return self._item.key

    @property
    def delivery_attempt(self) -> int:
        return self._item.delivery_attempt

    async def ack(self) -> None:
        if self.settled:
            return
        self.settled = True
        self._transport._finish_item(self._item)

    async def nack(self, *, requeue: bool = True) -> None:
        if self.settled:
            return
        self.settled = True
        await self._transport._reject(self._sub, self._item, self._attempt, requeue=requeue)


class InMemoryTransport(Transport):
    """
    Process-local transport.

    State belongs to the instance: two transports do not see each other's
    messages. ``key is None`` is not ordered beyond the group queue. A key
    that is set runs one delivery at a time for that ``(topic, group, key)``
    until ack, a dead-letter nack, or the attempt limit. Do not wait, inside
    a handler, for a later message that uses the same key: that message waits
    for this delivery to finish.

    There is no ``__bool__`` and no ``__len__``.
    """

    capabilities: ClassVar[TransportCapabilities] = TransportCapabilities(
        ordering_per_key=True,
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
        self._tasks: set[asyncio.Task[None]] = set()
        self._batches: set[_Batch] = set()
        self._delayed = 0
        self._idle = asyncio.Event()
        self._idle.set()

    async def send_message(self, message: Message) -> None:
        await self._emit(message, delivery_attempt=1)

    async def subscribe(
        self,
        topic: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> Subscription:
        if self._closed:
            raise TransportClosed("subscribe")
        self._check_options(options)
        group, created = self._ensure_group(topic, options.group)
        subscription = _Subscription(self, group, handler, options)
        group.subscriptions.append(subscription)
        if created and options.start_from == "earliest":
            for record in self._log.get(topic, []):
                group.queue.put_nowait(
                    _Item(
                        body=record.body,
                        headers=dict(record.headers),
                        content_type=record.content_type,
                        key=record.key,
                        delivery_attempt=1,
                        batch=None,
                    ),
                )
            if self._log.get(topic):
                self._note_work()
        for _ in range(options.concurrency):
            worker = asyncio.create_task(self._worker(subscription, group))
            subscription.workers.append(worker)
            self._track(worker)
        return subscription

    async def drain(self, timeout: float = 5) -> None:
        """
        Wait until queues, handlers, key lanes, and delayed redelivery are idle.

        Raises ``TimeoutError`` with the topic, group, queue length, and
        in-flight count.
        """

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
            for subscription in group.subscriptions:
                subscription.active = False
        tasks = [task for task in self._tasks if not task.done()]
        for task in tasks:
            task.cancel()
        for group in self._groups.values():
            while not group.queue.empty():
                self._finish_item(group.queue.get_nowait())
            for lane in group.lanes.values():
                while lane.waiting:
                    self._finish_item(lane.waiting.popleft())
            group.lanes.clear()
        for batch in list(self._batches):
            batch.force()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._delayed = 0
        self._note_progress()

    async def _emit(self, message: Message, *, delivery_attempt: int) -> None:
        if self._closed:
            raise TransportClosed("send_message")
        batch: _Batch | None = None
        token: contextvars.Token[_Batch | None] | None = None
        if self.delivery == "inline":
            batch = _Batch(parent=_batch_var.get())
            token = _batch_var.set(batch)
            self._batches.add(batch)
        try:
            self._fanout(message, delivery_attempt, batch)
            if batch is not None and batch.remaining > 0:
                await batch.wait_until(0)
        finally:
            if batch is not None:
                self._batches.discard(batch)
            if token is not None:
                _batch_var.reset(token)

    def _fanout(
        self,
        message: Message,
        delivery_attempt: int,
        batch: _Batch | None,
    ) -> None:
        headers = dict(message.headers) if message.headers else {}
        record = _Record(
            body=message_wire_bytes(message),
            headers=headers,
            content_type=message.content_type,
            key=message.key,
        )
        self._log.setdefault(message.topic, []).append(record)
        inside_handler = _handling.get()
        for group in self._groups.values():
            if group.topic != message.topic or not _active(group):
                continue
            item = _Item(
                body=record.body,
                headers=dict(record.headers),
                content_type=record.content_type,
                key=record.key,
                delivery_attempt=delivery_attempt,
                batch=batch,
            )
            if batch is not None:
                batch.add()
            if self.delivery == "inline" and inside_handler:
                self._dispatch_now(group, item)
            else:
                group.queue.put_nowait(item)
            self._note_work()

    def _dispatch_now(self, group: _Group, item: _Item) -> None:
        subscription = self._pick(group)
        if subscription is None:
            self._finish_item(item)
            return
        item.sub = subscription
        if item.key is None:
            task = asyncio.create_task(self._deliver(group, subscription, item))
            self._watch(subscription, task)
            return
        self._enqueue_key(group, subscription, item)

    async def _worker(self, subscription: _Subscription, group: _Group) -> None:
        outstanding: set[asyncio.Task[None]] = set()
        try:
            while subscription.active and not self._closed:
                while len(outstanding) >= subscription.options.prefetch:
                    _, pending = await asyncio.wait(
                        outstanding,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    outstanding = set(pending)
                item = await group.queue.get()
                if self._closed or not subscription.active:
                    self._finish_item(item)
                    continue
                task = asyncio.create_task(self._consume(group, subscription, item))
                self._watch(subscription, task)
                outstanding.add(task)
                task.add_done_callback(outstanding.discard)
        except asyncio.CancelledError:
            return

    async def _consume(self, group: _Group, subscription: _Subscription, item: _Item) -> None:
        item.sub = subscription
        if item.key is None:
            await self._deliver(group, subscription, item)
            return
        self._enqueue_key(group, subscription, item)

    def _enqueue_key(self, group: _Group, subscription: _Subscription, item: _Item) -> None:
        assert item.key is not None
        item.sub = subscription
        lane = group.lanes.setdefault(item.key, _Lane())
        lane.waiting.append(item)
        self._ensure_lane(group, lane, item.key)

    def _ensure_lane(self, group: _Group, lane: _Lane, key: str) -> None:
        if lane.task is not None and not lane.task.done():
            return
        lane.task = asyncio.create_task(self._run_lane(group, lane, key))
        self._track(lane.task)
        subscription = lane.waiting[0].sub if lane.waiting else None
        if subscription is not None:
            self._watch(subscription, lane.task)

    async def _run_lane(self, group: _Group, lane: _Lane, key: str) -> None:
        try:
            while lane.waiting and not self._closed:
                item = lane.waiting[0]
                subscription = item.sub
                if subscription is None:
                    lane.waiting.popleft()
                    self._finish_item(item)
                    continue
                await self._deliver(group, subscription, item)
                if lane.waiting and lane.waiting[0] is item:
                    lane.waiting.popleft()
                self._note_progress()
        except asyncio.CancelledError:
            while lane.waiting:
                self._finish_item(lane.waiting.popleft())
            raise
        finally:
            lane.task = None
            if lane.waiting and not self._closed:
                self._ensure_lane(group, lane, key)
            elif not lane.waiting:
                group.lanes.pop(key, None)
            self._note_progress()

    async def _deliver(self, group: _Group, subscription: _Subscription, item: _Item) -> None:
        if item.completed:
            return

        async def attempt() -> None:
            if self._closed or item.completed:
                self._finish_item(item)
                return
            incoming = _MemoryIncoming(self, group, subscription, item, attempt)
            try:
                await self._invoke(subscription, incoming)
            except asyncio.CancelledError:
                if not incoming.settled:
                    incoming.settled = True
                    self._finish_item(item)
                raise
            except Exception:
                logger.exception(
                    "subscriber %s failed for topic %s",
                    subscription.options.group,
                    group.topic,
                )
                if not incoming.settled:
                    await incoming.nack(requeue=True)

        try:
            await attempt()
        except asyncio.CancelledError:
            self._finish_item(item)
            raise
        except Exception:
            logger.exception(
                "delivery failed for topic %s",
                group.topic,
            )
            self._finish_item(item)
        await item.finished.wait()

    async def _invoke(self, subscription: _Subscription, incoming: _MemoryIncoming) -> None:
        nested = _handling.get()
        acquired = False
        if not nested:
            await subscription.slots.acquire()
            acquired = True
        handling_token = _handling.set(True)
        batch_token: contextvars.Token[_Batch | None] | None = None
        if incoming._item.batch is not None:
            batch_token = _batch_var.set(incoming._item.batch)
        incoming._group.in_flight += 1
        try:
            await subscription.handler(incoming)
        finally:
            incoming._group.in_flight = max(0, incoming._group.in_flight - 1)
            if batch_token is not None:
                _batch_var.reset(batch_token)
            _handling.reset(handling_token)
            if acquired:
                subscription.slots.release()
            self._note_progress()

    async def _reject(
        self,
        subscription: _Subscription,
        item: _Item,
        attempt: _Attempt,
        *,
        requeue: bool,
    ) -> None:
        if item.completed or self._closed:
            self._finish_item(item)
            return
        if (not requeue) or item.delivery_attempt >= subscription.options.max_deliveries:
            try:
                await self._publish_dlq(subscription, item)
            except TransportClosed:
                pass
            self._finish_item(item)
            return
        item.delivery_attempt += 1
        self._schedule_redelivery(attempt, item)

    async def _publish_dlq(self, subscription: _Subscription, item: _Item) -> None:
        """Republish onto the dead-letter topic without waiting for its handlers.

        Waiting here would hold the worker that must stay free to finish this
        delivery. The original inline batch, when there is one, already counts
        the dead-letter copy and waits for it.
        """

        if self._closed:
            raise TransportClosed("send_message")
        topic = subscription.options.dead_letter_topic or f"{subscription.group.topic}.dlq"
        self._fanout(
            Message(
                message_name="dead_letter",
                topic=topic,
                payload=item.body,
                payload_bytes=item.body,
                content_type=item.content_type,
                headers=dict(item.headers) or None,
                key=item.key,
            ),
            item.delivery_attempt,
            item.batch,
        )

    def _schedule_redelivery(self, attempt: _Attempt, item: _Item) -> None:
        # Count the delay before yielding so ``drain`` cannot observe a gap
        # where the message is neither queued, in a handler, nor delayed.
        self._delayed += 1
        self._note_work()

        async def wait_and_retry() -> None:
            try:
                await asyncio.sleep(self.redelivery_delay)
                if self._closed or item.completed:
                    self._finish_item(item)
                    return
                await attempt()
            except asyncio.CancelledError:
                self._finish_item(item)
                raise
            finally:
                self._delayed = max(0, self._delayed - 1)
                self._note_progress()

        self._track(asyncio.create_task(wait_and_retry()))

    def _finish_item(self, item: _Item) -> None:
        if not item.completed:
            item.completed = True
            if item.batch is not None:
                item.batch.finish_one()
        item.finished.set()
        self._note_progress()

    def _ensure_group(self, topic: str, name: str) -> tuple[_Group, bool]:
        key = (topic, name)
        created = key not in self._groups
        if created:
            self._groups[key] = _Group(topic, name)
        return self._groups[key], created

    def _pick(self, group: _Group) -> _Subscription | None:
        active = [subscription for subscription in group.subscriptions if subscription.active]
        if not active:
            return None
        subscription = active[group.cursor % len(active)]
        group.cursor += 1
        return subscription

    def _check_options(self, options: SubscribeOptions) -> None:
        if options.start_from not in ("latest", "earliest"):
            raise UnsupportedCapability(
                f"start_from={options.start_from!r} is not supported",
            )
        if options.concurrency < 1 or options.prefetch < 1 or options.max_deliveries < 1:
            raise UnsupportedCapability(
                "concurrency, prefetch, and max_deliveries must be >= 1",
            )
        if not self.capabilities.delayed_redelivery and self.redelivery_delay > 0:
            raise UnsupportedCapability("delayed redelivery is not supported")

    def _watch(self, subscription: _Subscription, task: asyncio.Task[None]) -> None:
        self._track(task)
        subscription.inflight.add(task)
        task.add_done_callback(subscription.inflight.discard)

    def _track(self, task: asyncio.Task[None]) -> None:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _is_idle(self) -> bool:
        if self._delayed:
            return False
        for group in self._groups.values():
            if group.in_flight or not group.queue.empty():
                return False
            for lane in group.lanes.values():
                if lane.waiting:
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
            parked = sum(len(lane.waiting) for lane in group.lanes.values())
            in_flight = group.in_flight + parked
            if group.queue.qsize() or in_flight or self._delayed:
                parts.append(
                    f"topic={group.topic} group={group.name} "
                    f"queue_length={group.queue.qsize()} in_flight={in_flight}",
                )
        if not parts:
            parts.append(f"topic=* group=* queue_length=0 in_flight={self._delayed}")
        if self._delayed:
            parts.append(f"delayed={self._delayed}")
        return "drain timed out: " + "; ".join(parts)


def _active(group: _Group) -> bool:
    return any(subscription.active for subscription in group.subscriptions)


__all__ = ("InMemoryTransport",)
