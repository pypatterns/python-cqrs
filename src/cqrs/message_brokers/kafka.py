"""
Kafka broker and transport.

[AI Instruction]
Import ``KafkaMessageBroker`` from this module. Build its producer with
``cqrs.adapters.kafka.kafka_producer_factory``. Both need the ``kafka``
extra. Pass the broker to bootstrap or to ``cqrs.EventProducer``.

``KafkaTransport`` is the publish/subscribe transport. Construct it with a
parsed URL (``create_transport("kafka://localhost:9092")``), a URL string,
or ``bootstrap_servers``. ``topic`` is the Kafka topic. ``group`` is the
consumer ``group_id``. Do not define ``__bool__`` or ``__len__``.
``send_message`` on ``KafkaMessageBroker`` is unchanged: ``key`` is forwarded
only when it is not ``None``.
"""

from __future__ import annotations

import asyncio
import logging
import typing
import urllib.parse
import uuid
from collections import deque
from typing import TYPE_CHECKING, ClassVar

from cqrs.adapters import protocol as adapters_protocol
from cqrs.message_brokers import protocol
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

if TYPE_CHECKING:
    from cqrs.adapters.kafka import KafkaRecord, KafkaTopicConsumer

logger = logging.getLogger("cqrs")


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


_WIRE_ID = "cqrs-message-id"
_CONTENT_TYPE = "cqrs-content-type"
_ATTEMPT = "cqrs-delivery-attempt"
_INTERNAL_HEADERS = frozenset({_WIRE_ID, _CONTENT_TYPE, _ATTEMPT})


class _Action:
    def __init__(self) -> None:
        self.kind: str | None = None
        self.requeue = True
        self.ready = asyncio.Event()

    def set(self, kind: str, *, requeue: bool = True) -> None:
        if self.kind is not None:
            return
        self.kind = kind
        self.requeue = requeue
        self.ready.set()


class _KafkaIncoming(IncomingMessage):
    def __init__(
        self,
        *,
        topic: str,
        body: bytes,
        headers: dict[str, str],
        content_type: str | None,
        key: str | None,
        delivery_attempt: int,
    ) -> None:
        self._topic = topic
        self._body = body
        self._headers = headers
        self._content_type = content_type
        self._key = key
        self._delivery_attempt = delivery_attempt
        self._action = _Action()

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
        return self._delivery_attempt

    async def ack(self) -> None:
        self._action.set("ack")

    async def nack(self, *, requeue: bool = True) -> None:
        self._action.set("nack", requeue=requeue)


class _Account:
    def __init__(self) -> None:
        self.done = False


class _Waiter:
    def __init__(
        self,
        subscription: _KafkaSubscription,
        record: KafkaRecord,
        attempt: int,
        account: _Account,
    ) -> None:
        self.subscription = subscription
        self.record = record
        self.attempt = attempt
        self.account = account
        self.done = asyncio.Event()


class _Lane:
    def __init__(self) -> None:
        self.waiting: deque[_Waiter] = deque()
        self.task: asyncio.Task[None] | None = None


class _Tracked:
    def __init__(self, offset: int) -> None:
        self.offset = offset
        self.wire_id: str | None = None
        self.account: _Account | None = None
        self.done = False


class _Offsets:
    """Commit through the completed prefix of records that were actually fetched.

    Compacted partitions can skip offsets. Advancing by consecutive integers
    stops at the gap and never commits the records after it.
    """

    def __init__(self) -> None:
        self._rows: dict[int, list[_Tracked]] = {}
        self.owner: dict[int, asyncio.Task[None]] = {}

    def observe(self, partition: int, offset: int) -> None:
        row = self._rows.setdefault(partition, [])
        if any(item.offset == offset for item in row):
            return
        row.append(_Tracked(offset))

    def bind(self, partition: int, offset: int, wire_id: str | None, account: _Account) -> None:
        for item in self._rows.get(partition, ()):
            if item.offset == offset and item.account is None:
                item.wire_id = wire_id
                item.account = account
                return
        tracked = _Tracked(offset)
        tracked.wire_id = wire_id
        tracked.account = account
        self._rows.setdefault(partition, []).append(tracked)

    def mark_done(self, partition: int, offset: int) -> None:
        for item in self._rows.get(partition, ()):
            if item.offset == offset:
                item.done = True
                return

    def commit_point(self, partition: int) -> int | None:
        row = self._rows.get(partition, [])
        if not row or not row[0].done:
            return None
        last = row[0].offset
        for item in row:
            if not item.done:
                break
            last = item.offset
        return last + 1

    def accept(self, partition: int, next_offset: int) -> list[_Tracked]:
        row = self._rows.get(partition, [])
        accepted: list[_Tracked] = []
        while row and row[0].done and row[0].offset < next_offset:
            accepted.append(row.pop(0))
        if not row:
            self._rows.pop(partition, None)
        return accepted


class _KafkaGroup:
    def __init__(self, topic: str, name: str, consumer: KafkaTopicConsumer) -> None:
        self.topic = topic
        self.name = name
        self.consumer = consumer
        self.subscriptions: list[_KafkaSubscription] = []
        self.buffered = asyncio.Semaphore(0)
        self.deliveries: set[asyncio.Task[None]] = set()
        self.offsets = _Offsets()
        self.offset_lock = asyncio.Lock()
        self.lanes: dict[str, _Lane] = {}
        self.cursor = 0
        self.poll_task: asyncio.Task[None] | None = None
        self.stopped = False

    def active(self) -> bool:
        return any(subscription.active for subscription in self.subscriptions)

    def pick(self) -> _KafkaSubscription | None:
        active = [subscription for subscription in self.subscriptions if subscription.active]
        if not active:
            return None
        subscription = active[self.cursor % len(active)]
        self.cursor += 1
        return subscription

    def add_buffer(self, count: int) -> None:
        for _ in range(count):
            self.buffered.release()

    def queued(self) -> int:
        return sum(len(lane.waiting) for lane in self.lanes.values())


class _KafkaSubscription(Subscription):
    def __init__(
        self,
        transport: KafkaTransport,
        group: _KafkaGroup,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> None:
        self._transport = transport
        self.group = group
        self.handler = handler
        self.options = options
        self.active = True
        self.execution = asyncio.Semaphore(options.concurrency)

    async def close(self) -> None:
        await self._transport._close_subscription(self)


class KafkaTransport(Transport):
    """
    Kafka publish/subscribe transport.

    ``topic`` is the Kafka topic and ``group`` is the consumer ``group_id``.
    Subscribers in one group compete. Different groups each get a copy.
    A set ``key`` is the partition key, so ``ordering_per_key`` is true and
    one key is delivered one-at-a-time inside the group. The dead-letter
    topic is emulated by republishing to ``{topic}.dlq`` (``native_dlq`` is
    false). There is no ``__bool__`` and no ``__len__``.

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
        self._groups: dict[tuple[str, str], _KafkaGroup] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._outstanding: dict[str, int] = {}
        self._in_flight = 0
        self._topics: set[str] = set()
        self._topic_lock = asyncio.Lock()
        self._producer_lock = asyncio.Lock()
        self._group_lock = asyncio.Lock()
        self._producer: adapters_protocol.KafkaProducer | None = None
        self._aio_producer: typing.Any = None
        self._commit_errors: dict[tuple[str, str, int], Exception] = {}
        self._idle = asyncio.Event()
        self._idle.set()

    async def start(self) -> None:
        if self._closed:
            return
        await self._ensure_producer()

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
        await self._ensure_producer()
        async with self._group_lock:
            if self._closed:
                raise TransportClosed("subscribe")
            group, created = await self._ensure_group(topic, options)
            subscription = _KafkaSubscription(self, group, handler, options)
            group.subscriptions.append(subscription)
            group.add_buffer(options.concurrency * options.prefetch)
            if created:
                group.poll_task = asyncio.create_task(self._poll(group))
                self._track(group.poll_task)
        return subscription

    async def drain(self, timeout: float = 5) -> None:
        """Wait until produced records have been acked or dead-lettered."""

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
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for group in self._groups.values():
            await _stop_quietly(group.consumer.stop())
        producer = self._aio_producer
        self._aio_producer = None
        self._producer = None
        if producer is not None:
            await _stop_quietly(producer.stop())
        self._outstanding.clear()
        self._in_flight = 0
        self._note()

    async def _close_subscription(self, subscription: _KafkaSubscription) -> None:
        if not subscription.active:
            return
        subscription.active = False
        group = subscription.group
        self._reassign_unstarted(group)
        if group.active() or self._closed:
            return
        async with self._group_lock:
            if group.active() or self._closed:
                return
            poll_task = group.poll_task
            if poll_task is not None and not poll_task.done():
                poll_task.cancel()
                await asyncio.gather(poll_task, return_exceptions=True)
            inflight = [task for task in group.deliveries if not task.done()]
            if inflight:
                await asyncio.gather(*inflight, return_exceptions=True)
            group.stopped = True
            self._groups.pop((group.topic, group.name), None)
            await _stop_quietly(group.consumer.stop())

    def _reassign_unstarted(self, group: _KafkaGroup) -> None:
        for lane in group.lanes.values():
            for waiter in lane.waiting:
                if waiter.subscription.active:
                    continue
                replacement = group.pick()
                if replacement is not None:
                    waiter.subscription = replacement

    async def _produce(self, message: protocol.Message, *, delivery_attempt: int) -> None:
        self._check_open("send_message")
        copies = self._destinations(message.topic)
        wire_id = uuid.uuid4().hex
        if copies:
            self._outstanding[wire_id] = copies
            self._note_work()
        headers = dict(message.headers) if message.headers else {}
        headers[_WIRE_ID] = wire_id
        if message.content_type is not None:
            headers[_CONTENT_TYPE] = message.content_type
        if delivery_attempt > 1:
            headers[_ATTEMPT] = str(delivery_attempt)
        try:
            producer = await self._ensure_producer()
            kwargs: dict[str, typing.Any] = {"headers": headers}
            if message.key is not None:
                kwargs["key"] = message.key
            await producer.produce(message.topic, message_wire_bytes(message), **kwargs)
        except Exception:
            if copies:
                self._outstanding.pop(wire_id, None)
                self._note()
            raise

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

    async def _ensure_group(self, topic: str, options: SubscribeOptions) -> tuple[_KafkaGroup, bool]:
        """Return the group for ``topic``. The caller holds ``_group_lock``."""

        key = (topic, options.group)
        existing = self._groups.get(key)
        if existing is not None and not existing.stopped:
            return existing, False
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
        group = _KafkaGroup(topic, options.group, consumer)
        self._groups[key] = group
        return group, True

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

    async def _poll(self, group: _KafkaGroup) -> None:
        try:
            while not self._closed and group.active():
                try:
                    records = await group.consumer.poll(timeout_ms=100, max_records=10)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("kafka poll failed for topic %s", group.topic)
                    await asyncio.sleep(0.1)
                    continue
                if not records or self._closed:
                    continue
                async with group.offset_lock:
                    for record in records:
                        group.offsets.observe(record.partition, record.offset)
                for record in records:
                    if self._closed or not group.active():
                        self._complete(_wire_id(record))
                        continue
                    await group.buffered.acquire()
                    subscription = group.pick()
                    if subscription is None:
                        group.buffered.release()
                        self._complete(_wire_id(record))
                        continue
                    delivery = asyncio.create_task(self._guarded(group, subscription, record))
                    group.deliveries.add(delivery)
                    delivery.add_done_callback(group.deliveries.discard)
                    self._track(delivery)
        except asyncio.CancelledError:
            return

    async def _guarded(self, group: _KafkaGroup, subscription: _KafkaSubscription, record: KafkaRecord) -> None:
        account = _Account()
        async with group.offset_lock:
            group.offsets.bind(record.partition, record.offset, _wire_id(record), account)
        try:
            if record.key is None:
                await subscription.execution.acquire()
                try:
                    await self._drive(group, subscription, record, _attempt_of(record), account)
                finally:
                    subscription.execution.release()
            else:
                await self._on_lane(group, subscription, record, account)
        except asyncio.CancelledError:
            if not account.done:
                self._finish_account(account, _wire_id(record))
            raise
        finally:
            group.buffered.release()

    async def _on_lane(
        self,
        group: _KafkaGroup,
        subscription: _KafkaSubscription,
        record: KafkaRecord,
        account: _Account,
    ) -> None:
        assert record.key is not None
        lane = group.lanes.setdefault(record.key, _Lane())
        waiter = _Waiter(subscription, record, _attempt_of(record), account)
        lane.waiting.append(waiter)
        self._kick_lane(group, lane, record.key)
        await waiter.done.wait()

    def _kick_lane(self, group: _KafkaGroup, lane: _Lane, key: str) -> None:
        if lane.task is not None and not lane.task.done():
            return
        lane.task = asyncio.create_task(self._run_lane(group, lane, key))
        self._track(lane.task)

    async def _run_lane(self, group: _KafkaGroup, lane: _Lane, key: str) -> None:
        try:
            while lane.waiting and not self._closed:
                waiter = lane.waiting[0]
                try:
                    await waiter.subscription.execution.acquire()
                    try:
                        await self._drive(
                            group,
                            waiter.subscription,
                            waiter.record,
                            waiter.attempt,
                            waiter.account,
                        )
                    finally:
                        waiter.subscription.execution.release()
                finally:
                    if lane.waiting and lane.waiting[0] is waiter:
                        lane.waiting.popleft()
                    waiter.done.set()
                    self._note()
        except asyncio.CancelledError:
            while lane.waiting:
                lane.waiting.popleft().done.set()
            raise
        finally:
            lane.task = None
            if lane.waiting and not self._closed:
                self._kick_lane(group, lane, key)
            elif not lane.waiting:
                group.lanes.pop(key, None)
            self._note()

    async def _drive(
        self,
        group: _KafkaGroup,
        subscription: _KafkaSubscription,
        record: KafkaRecord,
        attempt: int,
        account: _Account,
    ) -> None:
        visible, content_type = _visible(record)
        try:
            while not self._closed:
                incoming = _KafkaIncoming(
                    topic=record.topic,
                    body=record.body,
                    headers=dict(visible),
                    content_type=content_type,
                    key=record.key,
                    delivery_attempt=attempt,
                )
                self._in_flight += 1
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
                    if incoming._action.kind is None:
                        incoming._action.set("nack", requeue=True)
                finally:
                    self._in_flight = max(0, self._in_flight - 1)
                    self._note()
                if incoming._action.kind is None:
                    await incoming._action.ready.wait()
                if incoming._action.kind == "ack":
                    break
                requeue = incoming._action.requeue
                if requeue and attempt < subscription.options.max_deliveries:
                    attempt += 1
                    continue
                await self._dead_letter(subscription, record, visible, content_type, attempt)
                break
            if not self._closed and not group.stopped:
                await self._commit_prefix(group, record)
        except asyncio.CancelledError:
            raise
        finally:
            if not account.done and (self._closed or group.stopped):
                self._finish_account(account, _wire_id(record))

    def _finish_account(self, account: _Account, wire_id: str | None) -> None:
        if account.done:
            return
        account.done = True
        self._complete(wire_id)

    async def _dead_letter(
        self,
        subscription: _KafkaSubscription,
        record: KafkaRecord,
        headers: dict[str, str],
        content_type: str | None,
        attempt: int,
    ) -> None:
        topic = subscription.options.dead_letter_topic or f"{subscription.group.topic}.dlq"
        try:
            await self._produce(
                protocol.Message(
                    message_name="dead_letter",
                    topic=topic,
                    payload=record.body,
                    payload_bytes=record.body,
                    content_type=content_type,
                    headers=dict(headers) or None,
                    key=record.key,
                ),
                delivery_attempt=attempt,
            )
        except TransportClosed:
            return

    async def _commit_prefix(self, group: _KafkaGroup, record: KafkaRecord) -> None:
        partition = record.partition
        async with group.offset_lock:
            group.offsets.mark_done(partition, record.offset)
            owner = group.offsets.owner.get(partition)
            current = asyncio.current_task()
            if owner is not None and not owner.done():
                return
            if current is None:
                return
            group.offsets.owner[partition] = current
        try:
            while not self._closed and not group.stopped:
                async with group.offset_lock:
                    next_offset = group.offsets.commit_point(partition)
                if next_offset is None:
                    return
                if partition not in group.consumer.assigned_partitions():
                    self._commit_errors[(group.topic, group.name, partition)] = TransportError(
                        f"Kafka partition {partition} is not assigned; offset {next_offset} was not committed",
                    )
                    return
                try:
                    await group.consumer.commit(partition, next_offset)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._commit_errors[(group.topic, group.name, partition)] = exc
                    if partition not in group.consumer.assigned_partitions():
                        return
                    logger.exception(
                        "kafka commit failed for topic %s partition %s",
                        group.topic,
                        partition,
                    )
                    await asyncio.sleep(0.05)
                    continue
                self._commit_errors.pop((group.topic, group.name, partition), None)
                async with group.offset_lock:
                    accepted = group.offsets.accept(partition, next_offset)
                for item in accepted:
                    if item.account is not None and not item.account.done:
                        self._finish_account(item.account, item.wire_id)
        finally:
            current = asyncio.current_task()
            async with group.offset_lock:
                if group.offsets.owner.get(partition) is current:
                    group.offsets.owner.pop(partition, None)

    def _destinations(self, topic: str) -> int:
        return sum(1 for group in self._groups.values() if group.topic == topic and group.active())

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

    def _track(self, task: asyncio.Task[None]) -> None:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _drain_message(self) -> str:
        parts = [
            f"topic={group.topic} group={group.name} " f"queue_length={group.queued()} in_flight={self._in_flight}"
            for group in self._groups.values()
        ]
        if not parts:
            parts.append(f"topic=* group=* queue_length=0 in_flight={self._in_flight}")
        parts.append(f"outstanding={sum(self._outstanding.values())}")
        if self._commit_errors:
            failed = ", ".join(str(error) for error in self._commit_errors.values())
            parts.append(f"commit_error={failed}")
        return "drain timed out: " + "; ".join(parts)


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


def _check_options(options: SubscribeOptions) -> None:
    if options.start_from not in ("latest", "earliest"):
        raise UnsupportedCapability(f"start_from={options.start_from!r} is not supported")
    if options.concurrency < 1 or options.prefetch < 1 or options.max_deliveries < 1:
        raise UnsupportedCapability("concurrency, prefetch, and max_deliveries must be >= 1")


def _visible(record: KafkaRecord) -> tuple[dict[str, str], str | None]:
    headers = {key: value for key, value in record.headers.items() if key not in _INTERNAL_HEADERS}
    content_type = record.headers.get(_CONTENT_TYPE) or None
    return headers, content_type


def _attempt_of(record: KafkaRecord) -> int:
    raw = record.headers.get(_ATTEMPT)
    if raw is None:
        return 1
    try:
        return max(1, int(raw))
    except ValueError:
        return 1


def _wire_id(record: KafkaRecord) -> str | None:
    return record.headers.get(_WIRE_ID)


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
