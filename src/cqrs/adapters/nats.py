"""
NATS JetStream client used by ``NATSTransport``.

[AI Instruction]
Import ``JetStreamSession`` from this module. It needs the ``nats`` extra
(``nats-py``). Application code publishes through
``cqrs.message_brokers.nats.NATSTransport``, not through this session.
This module does not import FastAPI or FastStream.
"""

from __future__ import annotations

import asyncio
import logging

import nats
import nats.errors
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType
from nats.js.client import JetStreamContext
from nats.js.errors import APIError, NotFoundError

from cqrs.transport.errors import TransportError

logger = logging.getLogger("cqrs")

_STREAM_ALREADY_IN_USE = 10058


class JetStreamMessage:
    """One JetStream delivery. Ack, nak, or term it through the transport."""

    def __init__(self, message: Msg) -> None:
        self._message = message

    @property
    def data(self) -> bytes:
        return self._message.data

    @property
    def subject(self) -> str:
        return self._message.subject

    @property
    def headers(self) -> dict[str, str]:
        raw = self._message.headers
        if not raw:
            return {}
        return {str(key): str(value) for key, value in raw.items()}

    @property
    def delivery_count(self) -> int:
        """JetStream ``num_delivered``. The first delivery is 1."""

        return self._message.metadata.num_delivered

    async def ack(self) -> None:
        await self._message.ack()

    async def nak(self) -> None:
        await self._message.nak()

    async def term(self) -> None:
        await self._message.term()


class JetStreamConsumer:
    """A durable pull consumer bound to one subject."""

    def __init__(self, subscription: JetStreamContext.PullSubscription) -> None:
        self._subscription = subscription

    async def fetch(self, *, timeout: float) -> list[JetStreamMessage]:
        """
        Return the next message, or an empty list when the timeout expires.

        An empty timeout result is the idle poll. Other client errors propagate.
        """

        try:
            messages = await self._subscription.fetch(1, timeout=timeout)
        except nats.errors.TimeoutError:
            return []
        return [JetStreamMessage(message) for message in messages]

    async def unsubscribe(self) -> None:
        try:
            await self._subscription.unsubscribe()
        except Exception:
            logger.debug("nats pull unsubscribe failed", exc_info=True)


class JetStreamSession:
    """
    One connection and the streams this session ensured on it.

    ``storage`` is ``file`` or ``memory`` on the JetStream server. ``close``
    deletes a stream only when ``delete_streams`` is set and this session
    created that stream. A stream that already existed is left in place.
    """

    def __init__(self, servers: str, *, connect_timeout: int = 2, storage: str = "file") -> None:
        if storage not in ("file", "memory"):
            raise TransportError(f"NATS storage must be 'file' or 'memory', got {storage!r}")
        self._servers = servers
        self._connect_timeout = connect_timeout
        self._storage = StorageType.MEMORY if storage == "memory" else StorageType.FILE
        self._nc: NATSClient | None = None
        self._js: JetStreamContext | None = None
        self._lock: asyncio.Lock | None = None
        self._ensured: set[str] = set()
        self._owned: set[str] = set()

    async def connect(self) -> None:
        async with self._mutex():
            if self._nc is not None and not self._nc.is_closed:
                return
            self._nc = await nats.connect(
                self._servers,
                connect_timeout=self._connect_timeout,
                name="python-cqrs",
            )
            self._js = self._nc.jetstream()

    async def ensure_stream(self, stream: str, subject: str) -> None:
        """Create ``stream`` for ``subject``, or keep it when it already captures that subject."""

        await self.connect()
        if stream in self._ensured:
            return
        async with self._mutex():
            if stream in self._ensured:
                return
            jetstream = self._jetstream()
            created = False
            try:
                info = await jetstream.stream_info(stream)
            except NotFoundError:
                info = None
            if info is None:
                try:
                    await jetstream.add_stream(
                        name=stream,
                        subjects=[subject],
                        storage=self._storage,
                    )
                except APIError as exc:
                    if exc.err_code != _STREAM_ALREADY_IN_USE and "already in use" not in (exc.description or ""):
                        raise
                else:
                    created = True
            else:
                subjects = list(info.config.subjects or [])
                if subject not in subjects:
                    raise TransportError(
                        f"JetStream stream {stream!r} does not include subject {subject!r}",
                    )
            self._ensured.add(stream)
            if created:
                self._owned.add(stream)

    async def publish(self, subject: str, body: bytes, headers: dict[str, str] | None) -> None:
        jetstream = self._jetstream()
        await jetstream.publish(subject, body, headers=dict(headers) if headers else None)

    async def pull(
        self,
        *,
        subject: str,
        stream: str,
        durable: str,
        start_from: str,
        max_deliver: int,
    ) -> JetStreamConsumer:
        """
        Bind a durable pull consumer.

        The first caller creates it. A later caller with the same durable joins
        that consumer; JetStream keeps the original delivery limit and start
        position. ``group`` on the transport is this durable name.
        """

        await self.connect()
        policy = DeliverPolicy.ALL if start_from == "earliest" else DeliverPolicy.NEW
        config = ConsumerConfig(
            durable_name=durable,
            ack_policy=AckPolicy.EXPLICIT,
            deliver_policy=policy,
            max_deliver=max_deliver,
            ack_wait=30,
            max_ack_pending=1024,
            filter_subject=subject,
        )
        subscription = await self._jetstream().pull_subscribe(
            subject,
            durable=durable,
            stream=stream,
            config=config,
        )
        return JetStreamConsumer(subscription)

    async def close(self, *, delete_streams: bool) -> None:
        jetstream = self._js
        connection = self._nc
        streams = list(self._owned)
        self._owned.clear()
        self._ensured.clear()
        if delete_streams and jetstream is not None:
            for stream in streams:
                try:
                    await jetstream.delete_stream(stream)
                except NotFoundError:
                    continue
                except Exception:
                    logger.debug("nats delete stream %s failed", stream, exc_info=True)
        if connection is not None and not connection.is_closed:
            await connection.close()
        self._nc = None
        self._js = None

    def _jetstream(self) -> JetStreamContext:
        if self._js is None:
            raise TransportError("NATS JetStream session is not connected")
        return self._js

    def _mutex(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock


__all__ = (
    "JetStreamConsumer",
    "JetStreamMessage",
    "JetStreamSession",
)
