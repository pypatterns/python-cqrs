"""
Amazon SQS transport.

[AI Instruction]
``create_transport`` in this module builds this class from an ``sqs://`` URL.
Import this module to call it; install the ``sqs`` extra
(``pip install "python-cqrs[sqs]"``). An ``ImportError`` here means that
extra is missing. The topic is
the queue name and is not rewritten, except the dead-letter topic
``{topic}.dlq`` which is created as ``{topic}-dlq`` (SQS rejects a dot).
Each ``subscribe`` call is one receive loop. ``prefetch`` is
``MaxNumberOfMessages``. ``delivery_attempt`` is ``ApproximateReceiveCount``.
Do not keep a local attempt counter or a per-key lock. A FIFO queue name
still sends ``key`` as ``MessageGroupId``. A standard queue stores ``key``
as a message attribute only. Do not add ``__bool__`` or ``__len__``. Do not
import FastAPI or FastStream. Handler failures stay inside the receiver and
never leave ``send_message``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import urllib.parse
import uuid
from collections.abc import Callable
from typing import ClassVar

from cqrs.adapters.sqs import SQSClient, SQSDelivery
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


class _Bucket:
    def __init__(self, topic: str, group: str, queue_name: str) -> None:
        self.topic = topic
        self.group = group
        self.queue_name = queue_name
        self.queue_url: str | None = None
        self.native_dlq = False
        self.subscriptions: list[_SQSSubscription] = []

    def active(self) -> bool:
        return any(subscription.active for subscription in self.subscriptions)


class _SQSSubscription(Subscription):
    def __init__(
        self,
        transport: SQSTransport,
        bucket: _Bucket,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> None:
        self._transport = transport
        self.bucket = bucket
        self.handler = handler
        self.options = options
        self.active = True
        self.task: asyncio.Task[None] | None = None

    async def close(self) -> None:
        if not self.active:
            return
        self.active = False
        task = self.task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


class _Incoming(IncomingMessage):
    def __init__(
        self,
        transport: SQSTransport,
        subscription: _SQSSubscription,
        delivery: SQSDelivery,
    ) -> None:
        self._transport = transport
        self._subscription = subscription
        self._delivery = delivery
        self._headers = dict(delivery.headers)
        self._settled = False

    @property
    def topic(self) -> str:
        return self._subscription.bucket.topic

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
        return self._delivery.receive_count

    async def ack(self) -> None:
        if self._settled:
            return
        self._settled = True
        await self._transport._delete(self)
        self._transport._watch.settle(self._delivery.message_id)

    async def nack(self, *, requeue: bool = True) -> None:
        if self._settled:
            return
        self._settled = True
        await self._transport._nack(self, requeue=requeue)


class SQSTransport(Transport):
    """
    SQS publish/subscribe transport.

    The topic argument is the logical queue name. Each group gets a queue
    named ``{queue}-{group}`` (FIFO keeps ``.fifo`` at the end).
    ``send_message`` fans out one copy to every group that has an active
    subscription on this instance. A second subscription in that group is
    another receive loop on the same queue. ``key`` is a message attribute.
    On a FIFO queue it is also ``MessageGroupId``. This process does not
    lock by key.

    ``ack`` deletes the message. ``nack(requeue=True)`` makes it visible
    again until ``ApproximateReceiveCount`` reaches ``max_deliveries``.
    The final nack publishes one copy to the dead-letter topic and deletes
    the original, unless the queue already has a redrive policy. There is
    no ``__bool__`` and no ``__len__``.
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
        self._watch = HandlerWatch()

    async def send_message(self, message: Message) -> None:
        if self._closed:
            raise TransportClosed("send_message")
        body = message_wire_bytes(message)
        headers = dict(message.headers) if message.headers else {}
        targets = self._targets(message.topic)
        if not targets:
            return
        client = await self._connect()
        for bucket in targets:
            queue_url = bucket.queue_url
            if queue_url is None:
                continue
            token = uuid.uuid4().hex
            self._watch.reserve(token, 1)
            try:
                message_id = await client.send(
                    queue_url,
                    body=body,
                    headers=headers,
                    content_type=message.content_type,
                    key=message.key,
                    fifo=bucket.queue_name.endswith(".fifo"),
                )
            except BaseException:
                self._watch.abandon(f"~{token}", 1)
                raise
            self._watch.bind(token, message_id)

    async def subscribe(
        self,
        topic: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> Subscription:
        if self._closed:
            raise TransportClosed("subscribe")
        require_options(options)
        if options.start_from == "earliest":
            raise UnsupportedCapability("SQSTransport has no log to replay; start_from must be latest")
        async with self._structure:
            if self._closed:
                raise TransportClosed("subscribe")
            key = (topic, options.group)
            created = key not in self._buckets
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _Bucket(topic, options.group, self._physical(topic, options.group))
                self._buckets[key] = bucket
            subscription = _SQSSubscription(self, bucket, handler, options)
            try:
                await self._ensure(bucket)
            except Exception:
                if created:
                    self._buckets.pop(key, None)
                raise
            bucket.subscriptions.append(subscription)
            subscription.task = self._track(asyncio.create_task(self._poll(subscription)))
            return subscription

    async def drain(self, timeout: float = 5) -> None:
        """Wait until handlers for published copies have finished."""

        await self._watch.wait(timeout, self._describe)

    async def start(self) -> None:
        if self._closed:
            raise TransportClosed("start")
        await self._connect()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for bucket in self._buckets.values():
            for subscription in bucket.subscriptions:
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
        self._watch.abandon_all()
        self._watch.inflight = 0
        self._watch.poke()

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

    async def _poll(self, subscription: _SQSSubscription) -> None:
        bucket = subscription.bucket
        try:
            while subscription.active and not self._closed:
                client = self._client
                queue_url = bucket.queue_url
                if client is None or queue_url is None:
                    return
                try:
                    deliveries = await client.receive(
                        queue_url,
                        max_messages=subscription.options.prefetch,
                        wait_time_seconds=self.wait_time_seconds,
                        visibility_timeout=self.visibility_timeout,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("sqs receive failed for topic %s", bucket.topic)
                    await asyncio.sleep(0.1)
                    continue
                for delivery in deliveries:
                    if not subscription.active or self._closed:
                        return
                    try:
                        if bucket.native_dlq and delivery.receive_count > subscription.options.max_deliveries:
                            await self._defer(bucket, delivery)
                            continue
                        await self._one(subscription, delivery)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("sqs delivery failed for topic %s", bucket.topic)
        except asyncio.CancelledError:
            return

    async def _one(self, subscription: _SQSSubscription, delivery: SQSDelivery) -> None:
        incoming = _Incoming(self, subscription, delivery)
        self._watch.begin()
        try:
            try:
                await subscription.handler(incoming)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "subscriber %s failed for topic %s",
                    subscription.options.group,
                    subscription.bucket.topic,
                )
                if not incoming._settled:
                    await incoming.nack(requeue=True)
        finally:
            self._watch.end()

    async def _delete(self, incoming: _Incoming) -> None:
        client = self._client
        queue_url = incoming._subscription.bucket.queue_url
        if client is None or queue_url is None or self._closed:
            return
        await client.delete(queue_url, incoming._delivery.receipt_handle)

    async def _nack(self, incoming: _Incoming, *, requeue: bool) -> None:
        limit = incoming._subscription.options.max_deliveries
        final = (not requeue) or incoming.delivery_attempt >= limit
        client = self._client
        bucket = incoming._subscription.bucket
        queue_url = bucket.queue_url
        if client is None or queue_url is None:
            return
        if not final:
            await client.change_visibility(
                queue_url,
                incoming._delivery.receipt_handle,
                int(self.redelivery_delay),
            )
            return
        if bucket.native_dlq:
            await self._defer(bucket, incoming._delivery)
            self._watch.settle(incoming._delivery.message_id)
            return
        try:
            await self._publish_dead_letter(incoming)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("sqs dead-letter publish failed for topic %s", incoming.topic)
            return
        await client.delete(queue_url, incoming._delivery.receipt_handle)
        self._watch.settle(incoming._delivery.message_id)

    def _targets(self, topic: str) -> list[_Bucket]:
        return [
            bucket
            for bucket in self._buckets.values()
            if bucket.topic == topic and bucket.active() and bucket.queue_url is not None
        ]

    async def _defer(self, bucket: _Bucket, delivery: SQSDelivery) -> None:
        client = self._client
        if client is None or bucket.queue_url is None or self._closed:
            return
        await client.change_visibility(bucket.queue_url, delivery.receipt_handle, self.visibility_timeout)

    async def _publish_dead_letter(self, incoming: _Incoming) -> None:
        topic = incoming._subscription.options.dead_letter_topic or f"{incoming.topic}.dlq"
        if self._targets(topic):
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
            return
        client = await self._connect()
        queue_name = self._queue_name(topic) if self._queue_name is not None else default_sqs_queue_name(topic)
        queue_url = await client.ensure_queue(queue_name)
        await client.send(
            queue_url,
            body=incoming.body,
            headers=dict(incoming.headers),
            content_type=incoming.content_type,
            key=incoming.key,
            fifo=queue_name.endswith(".fifo"),
        )

    def _describe(self) -> str:
        parts: list[str] = []
        for bucket in self._buckets.values():
            if not bucket.active():
                continue
            parts.append(
                f"topic={bucket.topic} group={bucket.group} pending={self._watch.owed} in_flight={self._watch.inflight}",
            )
        if not parts:
            parts.append(f"topic=* group=* queue_length=0 in_flight={self._watch.inflight}")
        return "drain timed out: " + "; ".join(parts)

    def _track(self, task: asyncio.Task[None]) -> asyncio.Task[None]:
        self._tasks.add(task)

        def _done(finished: asyncio.Task[None]) -> None:
            self._tasks.discard(finished)
            if finished.cancelled():
                return
            error = finished.exception()
            if error is not None:
                logger.error("sqs transport task failed", exc_info=error)

        task.add_done_callback(_done)
        return task


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


def create_transport(url: str) -> SQSTransport:
    """Build ``SQSTransport`` from an ``sqs://`` URL."""

    parsed = urllib.parse.urlparse(url)
    if not parsed.scheme:
        raise TransportError(f"Transport URL {url!r} has no scheme")
    if parsed.scheme != "sqs":
        raise TransportError(
            f"SQSTransport URL scheme must be 'sqs', got {parsed.scheme!r}",
        )
    return SQSTransport(parsed)


__all__ = (
    "SQSTransport",
    "create_transport",
    "default_sqs_queue_name",
)
