"""Redis reader keeps polling after a transient delivery error. No broker."""

from __future__ import annotations

import urllib.parse

import pytest

pytest.importorskip("redis")

from cqrs.adapters.redis import StreamEntry  # noqa: E402
from cqrs.message_brokers.redis import RedisTransport, _Group, _RedisSubscription  # noqa: E402
from cqrs.transport.protocol import IncomingMessage, SubscribeOptions  # noqa: E402


async def test_reader_continues_after_a_transient_delivery_error() -> None:
    parsed = urllib.parse.urlparse("redis://localhost:6379/0")
    transport = RedisTransport(parsed)
    group = _Group("orders", "billing", "orders")
    seen: list[bytes] = []

    async def handler(message: IncomingMessage) -> None:
        seen.append(bytes(message.body))
        subscription.active = False
        await message.ack()

    options = SubscribeOptions(group="billing")
    subscription = _RedisSubscription(transport, group, handler, options)
    reads = 0

    class _Client:
        async def read_group(
            self,
            stream: str,
            name: str,
            consumer: str,
            *,
            count: int,
            block_ms: int,
        ) -> list[StreamEntry]:
            nonlocal reads
            reads += 1
            if reads == 1:
                raise ConnectionError("transient")
            return [
                StreamEntry(
                    message_id=b"1-0",
                    fields={
                        b"body": b"paid",
                        b"headers": b"{}",
                        b"content_type": b"",
                    },
                ),
            ]

        async def times_delivered(self, stream: str, name: str, message_id: bytes) -> int:
            return 1

        async def ack(self, stream: str, name: str, message_id: bytes) -> None:
            return None

    transport._client = _Client()  # type: ignore[assignment]

    await transport._reader(subscription, group, "billing-1", 1)

    assert seen == [b"paid"]


async def test_reader_handles_a_claim_while_new_records_are_arriving() -> None:
    parsed = urllib.parse.urlparse("redis://localhost:6379/0")
    transport = RedisTransport(parsed)
    group = _Group("orders", "billing", "orders")
    seen: list[bytes] = []

    async def handler(message: IncomingMessage) -> None:
        seen.append(bytes(message.body))
        if len(seen) == 2:
            subscription.active = False
        await message.ack()

    options = SubscribeOptions(group="billing")
    subscription = _RedisSubscription(transport, group, handler, options)

    class _Client:
        async def read_group(
            self,
            stream: str,
            name: str,
            consumer: str,
            *,
            count: int,
            block_ms: int,
        ) -> list[StreamEntry]:
            return [
                StreamEntry(
                    message_id=b"2-0",
                    fields={b"body": b"new", b"headers": b"{}", b"content_type": b""},
                ),
            ]

        async def autoclaim(
            self,
            stream: str,
            name: str,
            consumer: str,
            min_idle_ms: int,
            *,
            count: int,
        ) -> list[StreamEntry]:
            return [
                StreamEntry(
                    message_id=b"1-0",
                    fields={b"body": b"pending", b"headers": b"{}", b"content_type": b""},
                ),
            ]

        async def times_delivered(self, stream: str, name: str, message_id: bytes) -> int:
            return 1

        async def ack(self, stream: str, name: str, message_id: bytes) -> None:
            return None

    transport._client = _Client()  # type: ignore[assignment]
    await transport._reader(subscription, group, "billing-1", 1)
    assert seen == [b"new", b"pending"]
