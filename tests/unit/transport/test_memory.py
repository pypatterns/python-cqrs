"""InMemoryTransport behavior beyond the shared contract."""

from __future__ import annotations

import asyncio

import pytest

from cqrs.message_brokers.protocol import Message
from cqrs.transport import InMemoryTransport, IncomingMessage, SubscribeOptions, UnsupportedCapability


def _message(topic: str, body: bytes, *, key: str | None = None) -> Message:
    return Message(
        message_name="memory",
        topic=topic,
        payload=body,
        payload_bytes=body,
        key=key,
    )


async def test_transport_is_truthy_without_bool_or_len() -> None:
    transport = InMemoryTransport()
    assert transport
    assert bool(transport) is True
    assert not hasattr(InMemoryTransport, "__bool__")
    assert not hasattr(InMemoryTransport, "__len__")
    await transport.close()


async def test_queued_send_returns_before_the_handler_finishes() -> None:
    transport = InMemoryTransport(delivery="queued")
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(message: IncomingMessage) -> None:
        started.set()
        await release.wait()
        await message.ack()

    await transport.subscribe("orders", handler, SubscribeOptions(group="billing"))
    await transport.send_message(_message("orders", b"1"))
    assert not started.is_set()
    await asyncio.wait_for(started.wait(), timeout=1)
    release.set()
    await transport.drain(timeout=2)
    await transport.close()


async def test_inline_send_waits_for_nested_publish() -> None:
    transport = InMemoryTransport(delivery="inline")
    order: list[str] = []

    async def on_child(message: IncomingMessage) -> None:
        order.append("child")
        await message.ack()

    async def on_parent(message: IncomingMessage) -> None:
        order.append("parent")
        await transport.send_message(_message("child", b"c"))
        order.append("parent-after")
        await message.ack()

    await transport.subscribe("parent", on_parent, SubscribeOptions(group="g"))
    await transport.subscribe("child", on_child, SubscribeOptions(group="g"))

    await asyncio.wait_for(transport.send_message(_message("parent", b"p")), timeout=2)
    assert order == ["parent", "child", "parent-after"]
    await transport.close()


async def test_double_ack_and_nack_do_nothing() -> None:
    transport = InMemoryTransport()
    seen: list[int] = []

    async def handler(message: IncomingMessage) -> None:
        seen.append(message.delivery_attempt)
        await message.ack()
        await message.ack()
        await message.nack(requeue=True)

    await transport.subscribe("orders", handler, SubscribeOptions(group="billing", max_deliveries=2))
    await transport.send_message(_message("orders", b"1"))
    await transport.drain(timeout=2)
    await asyncio.sleep(0)
    assert seen == [1]
    await transport.close()


async def test_nack_without_requeue_dead_letters_immediately() -> None:
    transport = InMemoryTransport()
    attempts: list[int] = []
    dead: list[int] = []

    async def handler(message: IncomingMessage) -> None:
        attempts.append(message.delivery_attempt)
        await message.nack(requeue=False)

    async def dead_letter(message: IncomingMessage) -> None:
        dead.append(message.delivery_attempt)
        await message.ack()

    await transport.subscribe("orders", handler, SubscribeOptions(group="billing", max_deliveries=5))
    await transport.subscribe("orders.dlq", dead_letter, SubscribeOptions(group="dead"))
    await transport.send_message(_message("orders", b"row", key="k"))
    await transport.drain(timeout=2)
    assert attempts == [1]
    assert dead == [1]
    await transport.close()


async def test_same_key_redelivery_stays_ahead_of_the_next_message() -> None:
    transport = InMemoryTransport()
    order: list[tuple[bytes, int]] = []

    async def handler(message: IncomingMessage) -> None:
        order.append((message.body, message.delivery_attempt))
        if message.body == b"1" and message.delivery_attempt == 1:
            await message.nack(requeue=True)
            return
        await message.ack()

    await transport.subscribe("orders", handler, SubscribeOptions(group="billing", concurrency=2))
    await transport.send_message(_message("orders", b"1", key="same"))
    await transport.send_message(_message("orders", b"2", key="same"))
    await transport.drain(timeout=2)
    assert order == [(b"1", 1), (b"1", 2), (b"2", 1)]
    await transport.close()


async def test_latest_skips_the_past_and_earliest_replays_it() -> None:
    transport = InMemoryTransport()
    latest: list[bytes] = []
    earliest: list[bytes] = []

    async def collect(sink: list[bytes], message: IncomingMessage) -> None:
        sink.append(message.body)
        await message.ack()

    await transport.send_message(_message("orders", b"old"))
    await transport.subscribe("orders", lambda message: collect(latest, message), SubscribeOptions(group="live"))
    await transport.subscribe(
        "orders",
        lambda message: collect(earliest, message),
        SubscribeOptions(group="replay", start_from="earliest"),
    )
    await transport.send_message(_message("orders", b"new"))
    await transport.drain(timeout=2)
    assert latest == [b"new"]
    assert earliest == [b"old", b"new"]
    await transport.close()


async def test_group_copies_ack_independently() -> None:
    transport = InMemoryTransport()
    stable: list[int] = []
    retried: list[int] = []

    async def ack_once(message: IncomingMessage) -> None:
        stable.append(message.delivery_attempt)
        await message.ack()

    async def retry_once(message: IncomingMessage) -> None:
        retried.append(message.delivery_attempt)
        if message.delivery_attempt == 1:
            await message.nack(requeue=True)
            return
        await message.ack()

    await transport.subscribe("orders", ack_once, SubscribeOptions(group="stable"))
    await transport.subscribe("orders", retry_once, SubscribeOptions(group="retry"))
    await transport.send_message(_message("orders", b"1"))
    await transport.drain(timeout=2)
    assert stable == [1]
    assert retried == [1, 2]
    await transport.close()


async def test_drain_timeout_names_topic_group_and_inflight() -> None:
    transport = InMemoryTransport()

    async def handler(message: IncomingMessage) -> None:
        await asyncio.Event().wait()

    await transport.subscribe("orders", handler, SubscribeOptions(group="billing"))
    await transport.send_message(_message("orders", b"1"))
    with pytest.raises(TimeoutError, match=r"topic=orders group=billing queue_length=0 in_flight=1"):
        await transport.drain(timeout=0.05)
    await transport.close()


async def test_handler_exception_on_inline_send_does_not_propagate() -> None:
    transport = InMemoryTransport(delivery="inline")

    async def handler(message: IncomingMessage) -> None:
        raise RuntimeError("boom")

    await transport.subscribe("orders", handler, SubscribeOptions(group="billing", max_deliveries=1))
    await asyncio.wait_for(transport.send_message(_message("orders", b"1")), timeout=2)
    await transport.close()


async def test_rejected_subscribe_options() -> None:
    transport = InMemoryTransport()
    with pytest.raises(UnsupportedCapability):
        await transport.subscribe(
            "orders",
            lambda message: _unused(message),
            SubscribeOptions(group="billing", concurrency=0),
        )
    await transport.close()


async def _unused(message: IncomingMessage) -> None:
    await message.ack()


async def test_empty_headers_when_the_message_has_none() -> None:
    transport = InMemoryTransport()
    seen: list[IncomingMessage] = []

    async def handler(message: IncomingMessage) -> None:
        seen.append(message)
        await message.ack()

    await transport.subscribe("orders", handler, SubscribeOptions(group="billing"))
    await transport.send_message(
        Message(message_name="memory", topic="orders", payload={"n": 1}),
    )
    await transport.drain(timeout=2)
    assert seen[0].headers == {}
    assert seen[0].content_type is None
    assert seen[0].key is None
    await transport.close()
