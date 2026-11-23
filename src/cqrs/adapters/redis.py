"""
Redis Streams client.

[AI Instruction]
Import ``RedisStreamClient`` from this module. ``RedisTransport`` in
``cqrs.message_brokers.redis`` is the broker that speaks the transport
contract. Both need the ``redis`` extra (``redis.asyncio``). Do not import
FastAPI or FastStream here. Application code publishes with
``send_message`` on the transport, not with this client.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from typing import Any

import redis.asyncio
from redis.exceptions import ResponseError

_BUSY_GROUP = "BUSYGROUP"


@dataclasses.dataclass(frozen=True)
class StreamEntry:
    """One stream record as Redis returned it."""

    message_id: bytes
    fields: Mapping[bytes, bytes]


class RedisStreamClient:
    """
    Async Redis Streams commands used by ``RedisTransport``.

    The client does not keep consumer state. Each transport owns one client.
    ``decode_responses`` stays off so message bodies remain raw bytes.
    """

    def __init__(self, url: str) -> None:
        self._redis = redis.asyncio.Redis.from_url(
            url,
            decode_responses=False,
            socket_connect_timeout=1.0,
        )

    async def ping(self) -> None:
        """Open the connection. Raises when Redis is not reachable."""

        await self._redis.ping()

    async def add(self, stream: str, fields: Mapping[str, bytes | str]) -> None:
        """Append one record with ``XADD``."""

        # redis-py types the field dict invariantly. Values are bytes or str.
        payload: Any = dict(fields)
        await self._redis.xadd(stream, payload)

    async def create_group(self, stream: str, group: str, start_id: str) -> None:
        """
        Create a consumer group.

        ``start_id`` is ``$`` for messages that arrive after this call and
        ``0`` for the existing log. A group that already exists is left as it is.
        """

        try:
            await self._redis.xgroup_create(stream, group, id=start_id, mkstream=True)
        except ResponseError as exc:
            if _BUSY_GROUP not in str(exc):
                raise

    async def read_group(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        count: int,
        block_ms: int,
    ) -> list[StreamEntry]:
        """``XREADGROUP`` for ids the group has not delivered yet (``>``)."""

        raw = await self._redis.xreadgroup(
            group,
            consumer,
            {stream: ">"},
            count=count,
            block=block_ms,
        )
        return _entries_from_read(raw)

    async def ack(self, stream: str, group: str, message_id: bytes) -> None:
        """``XACK`` one id. A repeat ack is a no-op on the server."""

        await self._redis.xack(stream, group, message_id)

    async def claim(
        self,
        stream: str,
        group: str,
        consumer: str,
        message_id: bytes,
    ) -> StreamEntry | None:
        """
        ``XCLAIM`` one pending id immediately.

        Redis increments the pending delivery count. ``min_idle_time=0``
        accepts a message that was just delivered, which is how ``nack``
        requeues without waiting out the stuck-message idle time.
        """

        raw = await self._redis.xclaim(
            stream,
            group,
            consumer,
            min_idle_time=0,
            message_ids=[message_id],
        )
        entries = _entries(raw)
        if not entries:
            return None
        return entries[0]

    async def autoclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_ms: int,
        *,
        count: int,
    ) -> list[StreamEntry]:
        """
        ``XAUTOCLAIM`` pending ids idle at least ``min_idle_ms``.

        Redis increments each claimed message's delivery count. Messages
        newer than the idle time stay with the consumer that is working on them.
        """

        raw = await self._redis.xautoclaim(
            stream,
            group,
            consumer,
            min_idle_time=min_idle_ms,
            start_id="0-0",
            count=count,
        )
        return _entries_from_autoclaim(raw)

    async def times_delivered(self, stream: str, group: str, message_id: bytes) -> int:
        """
        Delivery count from ``XPENDING`` for one id.

        The first ``XREADGROUP`` reports 1. ``XCLAIM`` and ``XAUTOCLAIM``
        increase it. An id that is no longer pending reports 1 so a caller
        that lost the race still has a 1-based attempt.
        """

        rows = await self._redis.xpending_range(
            stream,
            group,
            min=message_id,
            max=message_id,
            count=1,
        )
        if not rows:
            return 1
        return max(1, int(rows[0]["times_delivered"]))

    async def backlog(self, stream: str, group: str) -> tuple[int, int]:
        """
        ``(pending, lag)`` for one group.

        ``pending`` is the PEL size from ``XPENDING``. ``lag`` is entries not
        yet delivered, from ``XINFO GROUPS``. A missing group counts as caught up.
        """

        try:
            summary = await self._redis.xpending(stream, group)
            groups = await self._redis.xinfo_groups(stream)
        except ResponseError as exc:
            if "NOGROUP" in str(exc):
                return (0, 0)
            raise
        pending = int(summary["pending"] or 0)
        wanted = group.encode()
        for info in groups:
            name = info.get("name")
            if name != group and name != wanted:
                continue
            lag = info.get("lag")
            if lag is None:
                return (pending, 0)
            return (pending, int(lag))
        return (pending, 0)

    async def aclose(self) -> None:
        """Close the client and its pool."""

        await self._redis.aclose()


def _entries_from_read(raw: object) -> list[StreamEntry]:
    if not isinstance(raw, list):
        return []
    found: list[StreamEntry] = []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        found.extend(_entries(item[1]))
    return found


def _entries_from_autoclaim(raw: object) -> list[StreamEntry]:
    if not isinstance(raw, (list, tuple)) or len(raw) < 2:
        return []
    return _entries(raw[1])


def _entries(raw: object) -> list[StreamEntry]:
    if not isinstance(raw, list):
        return []
    found: list[StreamEntry] = []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        message_id = item[0]
        fields = item[1]
        if message_id is None or not isinstance(fields, dict):
            continue
        found.append(StreamEntry(_as_bytes(message_id), _field_map(fields)))
    return found


def _field_map(fields: dict[object, object]) -> dict[bytes, bytes]:
    mapped: dict[bytes, bytes] = {}
    for key, value in fields.items():
        if value is None:
            continue
        mapped[_as_bytes(key)] = _as_bytes(value)
    return mapped


def _as_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return str(value).encode()


__all__ = (
    "RedisStreamClient",
    "StreamEntry",
)
