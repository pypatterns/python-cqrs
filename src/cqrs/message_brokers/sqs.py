"""
Amazon SQS transport.

[AI Instruction]
``create_transport`` builds this class from an ``sqs://`` URL. The topic is
the queue name and is not rewritten, except the emulated dead-letter topic
``{topic}.dlq`` which is created as ``{topic}-dlq`` (SQS rejects a dot).
Do not add ``__bool__`` or ``__len__``. Do not import FastAPI or FastStream.
Handler failures stay inside the poller and never leave ``send_message``.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import urllib.parse
from collections.abc import AsyncIterator, Callable
from typing import ClassVar

from cqrs.adapters.sqs import SQSClient, SQSDelivery
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

QueueNamer = Callable[[str], str]


def default_sqs_queue_name(topic: str) -> str:
    """
    SQS queue name for a logical topic.

    The topic is copied through. SQS rejects dots, and this function does not
    strip them. The one exception is the protocol dead-letter topic: a
    trailing ``.dlq`` is created as ``-dlq``. A FIFO dead-letter name
    ``{topic}.fifo.dlq`` becomes ``{topic}-dlq.fifo`` so the queue stays
    legal. Any other dot, including ``order.paid``, is left in place and
    queue creation fails.
    """

    if topic.endswith(".fifo.dlq"):
        return f"{topic[: -len('.fifo.dlq')]}-dlq.fifo"
    if topic.endswith(".dlq.fifo"):
        return f"{topic[: -len('.dlq.fifo')]}-dlq.fifo"
    if topic.endswith(".fifo"):
        return topic
    if topic.endswith(".dlq"):
        return f"{topic[: -len('.dlq')]}-dlq"
    return topic


def physical_queue_name(base: str, group: str) -> str:
    """``{base}-{group}``, with ``.fifo`` kept at the end when ``base`` is FIFO."""

    if base.endswith(".fifo"):
        return f"{base[: -len('.fifo')]}-{group}.fifo"
    return f"{base}-{group}"


class _Subscription(Subscription):
    def __init__(
        self,
        bucket: _Bucket,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> None:
        self.bucket = bucket
        self.handler = handler
        self.options = options
        self.active = True
        self.running = 0
        self.tasks: set[asyncio.Task[None]] = set()

    async def close(self) -> None:
        if not self.active:
            return
        self.active = False
        pending = [task for task in self.tasks if not task.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


class _KeyUse:
    """A per-key lock plus the deliveries waiting on it or holding it."""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.users = 0


class _Bucket:
    def __init__(self, topic: str, group: str, queue_name: str) -> None:
        self.topic = topic
        self.group = group
        self.queue_name = queue_name
        self.queue_url: str | None = None
        self.native_dlq = False
        self.subs: list[_Subscription] = []
        self.poller: asyncio.Task[None] | None = None
        self.outstanding = 0
        self.cursor = 0
        self.room = asyncio.Event()
        self.room.set()
        self.slot = asyncio.Event()
        self.slot.set()
        self._locks: dict[str, _KeyUse] = {}

    def has_active(self) -> bool:
        return any(subscription.active for subscription in self.subs)

    def capacity(self) -> int:
        total = 0
        for subscription in self.subs:
            if subscription.active:
                total += subscription.options.concurrency * subscription.options.prefetch
        return total

    def pick(self) -> _Subscription | None:
        active = [
            subscription
            for subscription in self.subs
            if subscription.active and subscription.running < subscription.options.concurrency
        ]
        if not active:
            return None
        subscription = active[self.cursor % len(active)]
        self.cursor += 1
        subscription.running += 1
        return subscription

    @contextlib.asynccontextmanager
    async def key_lock(self, key: str) -> AsyncIterator[None]:
        entry = self._locks.get(key)
        if entry is None:
            entry = _KeyUse()
            self._locks[key] = entry
        entry.users += 1
        try:
            async with entry.lock:
                yield
        finally:
            entry.users -= 1
            if entry.users == 0 and self._locks.get(key) is entry:
                self._locks.pop(key, None)


class _Incoming(IncomingMessage):
    def __init__(
        self,
        transport: SQSTransport,
        bucket: _Bucket,
        subscription: _Subscription,
        delivery: SQSDelivery,
        attempt: int,
    ) -> None:
        self._transport = transport
        self._bucket = bucket
        self._subscription = subscription
        self._delivery = delivery
        self._attempt = attempt
        self._headers = dict(delivery.headers)
        self.settled = False

    @property
    def topic(self) -> str:
        return self._bucket.topic

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
        return self._delivery.key

    @property
    def delivery_attempt(self) -> int:
        return self._attempt

    async def ack(self) -> None:
        if self.settled:
            return
        self.settled = True
        await self._transport._ack(self)

    async def nack(self, *, requeue: bool = True) -> None:
        if self.settled:
            return
        self.settled = True
        await self._transport._nack(self, requeue=requeue)


class SQSTransport(Transport):
    """
    SQS publish/subscribe transport.

    The topic argument is the logical queue name and is passed to SQS
    unchanged, aside from the emulated dead-letter suffix described by
    :func:`default_sqs_queue_name`. Standard queues are the default:
    ``capabilities.ordering_per_key`` is False and ``key`` is only a message
    attribute. A queue whose base name ends with ``.fifo`` is a FIFO queue.
    On those queues ``key`` is also ``MessageGroupId``, so SQS orders that
    key. ``MessageGroupId`` is never sent to a standard queue. This process
    still runs one delivery at a time per key so a single subscription does
    not overlap itself; that is not an order guarantee across processes.

    Competing consumers of one ``(topic, group)`` share a single queue.
    Broadcast across groups is not native on one SQS queue, so each group
    gets its own queue named ``{queue}-{group}`` (FIFO keeps ``.fifo`` at the
    end: ``{stem}-{group}.fifo``). ``send_message`` fans out one copy to
    every group that has an active subscription on this instance. Same-group
    consumers compete for that copy. Different groups each receive a copy.
    A publish with no active subscription on this instance does not create a
    queue and does not store a copy: the process that publishes is the
    process that subscribed. ``queue_name``, when passed, replaces
    :func:`default_sqs_queue_name` and must itself return a legal SQS name.
    The group suffix is still appended afterwards. ``start_from`` does not
    seek: SQS has no log offset, so ``latest`` and ``earliest`` both consume
    whatever is already in the group queue.

    ``ack`` deletes the message. ``nack(requeue=True)`` changes the
    visibility timeout to ``redelivery_delay`` seconds (zero makes it visible
    again immediately) until ``max_deliveries``. ``nack(requeue=False)`` or
    the attempt limit publishes a copy to the dead-letter topic and deletes
    the original. That copy uses the queue ``{topic}-dlq`` when the logical
    topic is the protocol default ``{topic}.dlq``. ``native_dlq`` on the
    class is False because queues this transport creates have no redrive
    policy. If a queue already has one, a dead-letter nack leaves the
    message for SQS instead of publishing a second copy.

    ``endpoint_url``, ``region`` / ``region_name``, and
    ``aws_access_key_id`` / ``aws_secret_access_key`` / ``aws_session_token``
    are read from the URL query. ``sqs://host:port`` uses
    ``http://host:port`` as the endpoint (LocalStack). A custom endpoint
    without credentials uses the LocalStack pair ``test`` / ``test``.
    A host ending in ``amazonaws.com`` leaves the endpoint empty so boto
    uses the real service and the default credential chain.

    There is no ``__bool__`` and no ``__len__``. There is no module-level bus.
    """

    capabilities: ClassVar[TransportCapabilities] = TransportCapabilities(
        ordering_per_key=False,
        native_dlq=False,
        delayed_redelivery=True,
        headers=True,
        topic="SQS queue name; each group is the queue {name}-{group}, and publish fans out",
    )

    def __init__(
        self,
        source: urllib.parse.ParseResult | str | None = None,
        *,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        queue_name: QueueNamer | None = None,
        visibility_timeout: int | None = None,
        wait_time_seconds: int | None = None,
        redelivery_delay: float | None = None,
    ) -> None:
        settings = _settings_from_source(source)
        if endpoint_url is not None:
            settings = dataclasses.replace(settings, endpoint_url=endpoint_url)
        if region_name is not None:
            settings = dataclasses.replace(settings, region_name=region_name)
        if aws_access_key_id is not None:
            settings = dataclasses.replace(settings, aws_access_key_id=aws_access_key_id)
        if aws_secret_access_key is not None:
            settings = dataclasses.replace(settings, aws_secret_access_key=aws_secret_access_key)
        if aws_session_token is not None:
            settings = dataclasses.replace(settings, aws_session_token=aws_session_token)
        if visibility_timeout is not None:
            settings = dataclasses.replace(settings, visibility_timeout=visibility_timeout)
        if wait_time_seconds is not None:
            settings = dataclasses.replace(settings, wait_time_seconds=wait_time_seconds)
        if redelivery_delay is not None:
            settings = dataclasses.replace(settings, redelivery_delay=redelivery_delay)
        _validate_settings(settings)
        self.endpoint_url = settings.endpoint_url
        self.region_name = settings.region_name
        self.visibility_timeout = settings.visibility_timeout
        self.wait_time_seconds = settings.wait_time_seconds
        self.redelivery_delay = settings.redelivery_delay
        self._access_key = settings.aws_access_key_id
        self._secret_key = settings.aws_secret_access_key
        self._session_token = settings.aws_session_token
        self._queue_name = queue_name
        self._closed = False
        self._client: SQSClient | None = None
        self._connect_lock = asyncio.Lock()
        self._buckets: dict[tuple[str, str], _Bucket] = {}
        self._structure = asyncio.Lock()
        self._tasks: set[asyncio.Task[None]] = set()
        self._attempts: dict[str, int] = {}
        self._unsettled = 0
        self._handlers = 0
        self._idle = asyncio.Event()
        self._idle.set()

    async def send_message(self, message: Message) -> None:
        await self._publish(message, preserved_attempt=None)

    async def subscribe(
        self,
        topic: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> Subscription:
        if self._closed:
            raise TransportClosed("subscribe")
        _check_options(options)
        async with self._structure:
            if self._closed:
                raise TransportClosed("subscribe")
            key = (topic, options.group)
            created = key not in self._buckets
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _Bucket(topic, options.group, self._physical(topic, options.group))
                self._buckets[key] = bucket
            subscription = _Subscription(bucket, handler, options)
            try:
                await self._ensure(bucket)
            except Exception:
                if created:
                    self._buckets.pop(key, None)
                raise
            bucket.subs.append(subscription)
            self._start_poller(bucket)
            return subscription

    async def drain(self, timeout: float = 5) -> None:
        """
        Wait until fan-out copies have been acked, dead-lettered, or released
        and no handler is still running.

        Raises ``TimeoutError`` with the topic, group, outstanding receipts,
        and in-flight handler count.
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

    async def start(self) -> None:
        if self._closed:
            raise TransportClosed("start")
        await self._connect()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for bucket in self._buckets.values():
            for subscription in bucket.subs:
                subscription.active = False
        tasks = [task for task in self._tasks if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        client = self._client
        self._client = None
        if client is not None:
            try:
                await client.close()
            except Exception:
                logger.exception("sqs client close failed")
        self._note_progress()

    def _physical(self, topic: str, group: str) -> str:
        base = self._queue_name(topic) if self._queue_name is not None else default_sqs_queue_name(topic)
        return physical_queue_name(base, group)

    async def _connect(self) -> SQSClient:
        if self._client is not None:
            return self._client
        async with self._connect_lock:
            if self._closed:
                raise TransportClosed("send_message")
            if self._client is None:
                client = SQSClient(
                    endpoint_url=self.endpoint_url,
                    region_name=self.region_name,
                    aws_access_key_id=self._access_key,
                    aws_secret_access_key=self._secret_key,
                    aws_session_token=self._session_token,
                    read_timeout=max(60.0, float(self.wait_time_seconds) + 5.0),
                )
                await client.start()
                self._client = client
            return self._client

    async def _ensure(self, bucket: _Bucket) -> None:
        if bucket.queue_url is not None:
            return
        client = await self._connect()
        bucket.queue_url = await client.ensure_queue(bucket.queue_name)
        bucket.native_dlq = await client.has_redrive_policy(bucket.queue_url)

    def _start_poller(self, bucket: _Bucket) -> None:
        if bucket.poller is not None and not bucket.poller.done():
            return
        bucket.poller = asyncio.create_task(self._poll(bucket))
        self._track(bucket.poller)

    async def _publish(self, message: Message, *, preserved_attempt: int | None) -> None:
        if self._closed:
            raise TransportClosed("send_message")
        body = message_wire_bytes(message)
        headers = dict(message.headers) if message.headers else {}
        targets = [
            bucket
            for bucket in self._buckets.values()
            if bucket.topic == message.topic and bucket.has_active() and bucket.queue_url is not None
        ]
        if not targets:
            return
        client = await self._connect()
        for bucket in targets:
            queue_url = bucket.queue_url
            if queue_url is None:
                continue
            self._unsettled += 1
            self._note_work()
            try:
                await client.send(
                    queue_url,
                    body=body,
                    headers=headers,
                    content_type=message.content_type,
                    key=message.key,
                    preserved_attempt=preserved_attempt,
                    fifo=bucket.queue_name.endswith(".fifo"),
                )
            except Exception:
                self._settle()
                raise

    async def _poll(self, bucket: _Bucket) -> None:
        try:
            while not self._closed and bucket.has_active():
                capacity = bucket.capacity()
                if capacity <= 0:
                    return
                if bucket.outstanding >= capacity:
                    bucket.room.clear()
                    if bucket.outstanding < bucket.capacity():
                        continue
                    await bucket.room.wait()
                    continue
                client = self._client
                queue_url = bucket.queue_url
                if client is None or queue_url is None:
                    await asyncio.sleep(0.05)
                    continue
                slots = capacity - bucket.outstanding
                try:
                    deliveries = await client.receive(
                        queue_url,
                        max_messages=slots,
                        wait_time_seconds=self.wait_time_seconds,
                        visibility_timeout=self.visibility_timeout,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("sqs receive failed for %s", bucket.queue_name)
                    if self._closed:
                        return
                    await asyncio.sleep(0.2)
                    continue
                if self._closed or not bucket.has_active():
                    for delivery in deliveries:
                        await self._release(bucket, delivery)
                    return
                if not deliveries:
                    if self.wait_time_seconds == 0:
                        await asyncio.sleep(0.05)
                    continue
                ordered = sorted(deliveries, key=lambda item: (item.sent_timestamp, item.message_id))
                for delivery in ordered:
                    self._dispatch(bucket, delivery)
        except asyncio.CancelledError:
            return

    def _dispatch(self, bucket: _Bucket, delivery: SQSDelivery) -> None:
        bucket.outstanding += 1
        task = asyncio.create_task(self._deliver(bucket, delivery))
        self._track(task)

        def _done(finished: asyncio.Task[None]) -> None:
            del finished
            self._release_slot(bucket)

        if task.done():
            _done(task)
        else:
            task.add_done_callback(_done)

    def _release_slot(self, bucket: _Bucket) -> None:
        bucket.outstanding = max(0, bucket.outstanding - 1)
        bucket.room.set()
        self._note_progress()

    async def _deliver(self, bucket: _Bucket, delivery: SQSDelivery) -> None:
        try:
            if not delivery.key:
                await self._handle(bucket, delivery)
                return
            async with bucket.key_lock(delivery.key):
                if self._closed or not bucket.has_active():
                    await self._release(bucket, delivery)
                    return
                await self._handle(bucket, delivery)
        except asyncio.CancelledError:
            raise

    async def _handle(self, bucket: _Bucket, delivery: SQSDelivery) -> None:
        subscription = await self._pick(bucket)
        if subscription is None:
            await self._release(bucket, delivery)
            return
        attempt = self._attempt_for(delivery)
        incoming = _Incoming(self, bucket, subscription, delivery, attempt)
        task = asyncio.current_task()
        if task is not None:
            subscription.tasks.add(task)
        self._handlers += 1
        self._note_work()
        try:
            try:
                await subscription.handler(incoming)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "subscriber %s failed for topic %s",
                    subscription.options.group,
                    bucket.topic,
                )
                if not incoming.settled:
                    try:
                        await incoming.nack(requeue=True)
                    except Exception:
                        logger.exception(
                            "sqs nack failed for topic %s",
                            bucket.topic,
                        )
        finally:
            subscription.running = max(0, subscription.running - 1)
            self._handlers = max(0, self._handlers - 1)
            if task is not None:
                subscription.tasks.discard(task)
            bucket.slot.set()
            self._note_progress()

    async def _pick(self, bucket: _Bucket) -> _Subscription | None:
        while not self._closed and bucket.has_active():
            subscription = bucket.pick()
            if subscription is not None:
                return subscription
            # A handler may free a slot between the failed pick and the wait.
            # Pick again so clearing the event does not drop that reservation.
            bucket.slot.clear()
            subscription = bucket.pick()
            if subscription is not None:
                return subscription
            await bucket.slot.wait()
        return None

    def _attempt_for(self, delivery: SQSDelivery) -> int:
        base = delivery.preserved_attempt if delivery.preserved_attempt is not None else delivery.receive_count
        local = self._attempts.get(delivery.message_id)
        if local is None:
            return base
        return max(base, local)

    async def _ack(self, incoming: _Incoming) -> None:
        if self._closed:
            return
        client = self._client
        queue_url = incoming._bucket.queue_url
        if client is None or queue_url is None:
            return
        await client.delete(queue_url, incoming._delivery.receipt_handle)
        self._attempts.pop(incoming._delivery.message_id, None)
        self._settle()

    async def _nack(self, incoming: _Incoming, *, requeue: bool) -> None:
        if self._closed:
            return
        final = (not requeue) or incoming.delivery_attempt >= incoming._subscription.options.max_deliveries
        if not final:
            self._attempts[incoming._delivery.message_id] = incoming.delivery_attempt + 1
            await self._change_visibility(incoming, self._delay_seconds())
            return
        if incoming._bucket.native_dlq:
            await self._change_visibility(incoming, 0)
            self._attempts.pop(incoming._delivery.message_id, None)
            self._settle()
            return
        await self._publish_dlq(incoming)
        await self._delete_quiet(incoming)
        self._attempts.pop(incoming._delivery.message_id, None)
        self._settle()

    async def _publish_dlq(self, incoming: _Incoming) -> None:
        topic = incoming._subscription.options.dead_letter_topic or f"{incoming._bucket.topic}.dlq"
        await self._publish(
            Message(
                message_name="dead_letter",
                topic=topic,
                payload=incoming.body,
                payload_bytes=incoming.body,
                content_type=incoming.content_type,
                headers=dict(incoming.headers) or None,
                key=incoming.key,
            ),
            preserved_attempt=incoming.delivery_attempt,
        )

    async def _delete_quiet(self, incoming: _Incoming) -> None:
        client = self._client
        queue_url = incoming._bucket.queue_url
        if client is None or queue_url is None or self._closed:
            return
        await client.delete(queue_url, incoming._delivery.receipt_handle)

    async def _change_visibility(self, incoming: _Incoming, timeout: int) -> None:
        client = self._client
        queue_url = incoming._bucket.queue_url
        if client is None or queue_url is None or self._closed:
            return
        await client.change_visibility(queue_url, incoming._delivery.receipt_handle, timeout)

    async def _release(self, bucket: _Bucket, delivery: SQSDelivery) -> None:
        client = self._client
        queue_url = bucket.queue_url
        if client is None or queue_url is None or self._closed:
            return
        try:
            await client.change_visibility(queue_url, delivery.receipt_handle, 0)
        except Exception:
            logger.exception("could not release sqs message on %s", bucket.queue_name)

    def _delay_seconds(self) -> int:
        if self.redelivery_delay <= 0:
            return 0
        return min(43200, int(self.redelivery_delay))

    def _settle(self) -> None:
        self._unsettled = max(0, self._unsettled - 1)
        self._note_progress()

    def _is_idle(self) -> bool:
        if self._handlers:
            return False
        if self._closed:
            return True
        if self._unsettled:
            return False
        return all(bucket.outstanding == 0 for bucket in self._buckets.values())

    def _note_work(self) -> None:
        self._idle.clear()

    def _note_progress(self) -> None:
        if self._is_idle():
            self._idle.set()

    def _drain_message(self) -> str:
        parts: list[str] = []
        for bucket in self._buckets.values():
            running = sum(subscription.running for subscription in bucket.subs)
            if bucket.outstanding or running:
                parts.append(
                    f"topic={bucket.topic} group={bucket.group} "
                    f"queue_length={bucket.outstanding} in_flight={running}",
                )
        if not parts:
            parts.append(
                f"topic=* group=* queue_length=0 in_flight={self._handlers} unsettled={self._unsettled}",
            )
        elif self._unsettled:
            parts.append(f"unsettled={self._unsettled}")
        return "drain timed out: " + "; ".join(parts)

    def _track(self, task: asyncio.Task[None]) -> None:
        self._tasks.add(task)

        def _done(finished: asyncio.Task[None]) -> None:
            self._tasks.discard(finished)
            if finished.cancelled():
                return
            error = finished.exception()
            if error is not None:
                logger.error("sqs transport task failed", exc_info=error)

        task.add_done_callback(_done)


@dataclasses.dataclass
class _Settings:
    endpoint_url: str | None
    region_name: str
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None
    visibility_timeout: int
    wait_time_seconds: int
    redelivery_delay: float


def _settings_from_source(source: urllib.parse.ParseResult | str | None) -> _Settings:
    if source is None:
        parsed = urllib.parse.urlparse("sqs://")
    elif isinstance(source, str):
        parsed = urllib.parse.urlparse(source)
    else:
        parsed = source
    query = urllib.parse.parse_qs(parsed.query)

    def one(name: str) -> str | None:
        values = query.get(name)
        if not values or values[0] == "":
            return None
        return values[0]

    endpoint = one("endpoint_url")
    hostname = parsed.hostname
    port = parsed.port
    if endpoint is None and hostname:
        if not hostname.endswith(".amazonaws.com"):
            scheme = "https" if (one("use_ssl") or "").lower() in {"1", "true", "yes"} else "http"
            if port is not None:
                endpoint = f"{scheme}://{hostname}:{port}"
            else:
                endpoint = f"{scheme}://{hostname}"

    region = one("region") or one("region_name")
    if region is None and hostname and hostname.endswith(".amazonaws.com"):
        parts = hostname.split(".")
        if len(parts) >= 4 and parts[0] == "sqs":
            region = parts[1]
    if region is None:
        region = "us-east-1"

    access = urllib.parse.unquote(parsed.username) if parsed.username else None
    secret = urllib.parse.unquote(parsed.password) if parsed.password else None
    if one("aws_access_key_id"):
        access = one("aws_access_key_id")
    if one("aws_secret_access_key"):
        secret = one("aws_secret_access_key")
    token = one("aws_session_token")
    if endpoint and access is None and secret is None:
        access = "test"
        secret = "test"

    visibility = _parse_int(one("visibility_timeout"), 30, "visibility_timeout")
    wait_time = _parse_int(one("wait_time_seconds"), 1, "wait_time_seconds")
    delay = _parse_float(one("redelivery_delay"), 0.0, "redelivery_delay")
    return _Settings(
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        aws_session_token=token,
        visibility_timeout=visibility,
        wait_time_seconds=wait_time,
        redelivery_delay=delay,
    )


def _parse_int(raw: str | None, default: int, name: str) -> int:
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise TransportError(f"Invalid {name} {raw!r}") from exc


def _parse_float(raw: str | None, default: float, name: str) -> float:
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise TransportError(f"Invalid {name} {raw!r}") from exc


def _validate_settings(settings: _Settings) -> None:
    if settings.visibility_timeout < 1 or settings.visibility_timeout > 43200:
        raise UnsupportedCapability("visibility_timeout must be between 1 and 43200 seconds")
    if settings.wait_time_seconds < 0 or settings.wait_time_seconds > 20:
        raise UnsupportedCapability("wait_time_seconds must be between 0 and 20")
    if settings.redelivery_delay < 0 or settings.redelivery_delay > 43200:
        raise UnsupportedCapability("redelivery_delay must be between 0 and 43200 seconds")
    if not settings.region_name:
        raise TransportError("SQS region is empty")


def _check_options(options: SubscribeOptions) -> None:
    if not options.group:
        raise UnsupportedCapability("group is required")
    if options.start_from not in ("latest", "earliest"):
        raise UnsupportedCapability(f"start_from={options.start_from!r} is not supported")
    if options.concurrency < 1 or options.prefetch < 1 or options.max_deliveries < 1:
        raise UnsupportedCapability("concurrency, prefetch, and max_deliveries must be >= 1")


__all__ = (
    "SQSTransport",
    "default_sqs_queue_name",
)
