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


async def test_inline_nested_send_waits_for_the_child() -> None:
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


async def test_inline_sibling_does_not_finish_the_nested_send() -> None:
    transport = InMemoryTransport(delivery="inline")
    child_started = asyncio.Event()
    release_child = asyncio.Event()
    nested_returned = asyncio.Event()

    async def billing(message: IncomingMessage) -> None:
        await transport.send_message(_message("paid", b"child"))
        nested_returned.set()
        await message.ack()

    async def warehouse(message: IncomingMessage) -> None:
        await message.ack()

    async def child(message: IncomingMessage) -> None:
        child_started.set()
        await release_child.wait()
        await message.ack()

    await transport.subscribe("orders", billing, SubscribeOptions(group="billing"))
    await transport.subscribe("orders", warehouse, SubscribeOptions(group="warehouse"))
    await transport.subscribe("paid", child, SubscribeOptions(group="billing"))

    send = asyncio.create_task(transport.send_message(_message("orders", b"order")))
    try:
        await asyncio.wait_for(child_started.wait(), timeout=1)
        await asyncio.sleep(0.05)
        nested_was_blocked = not nested_returned.is_set()
        release_child.set()
        await asyncio.wait_for(send, timeout=2)
    finally:
        if not release_child.is_set():
            release_child.set()
        await transport.close()
    assert nested_was_blocked
    assert nested_returned.is_set()


async def test_closing_the_last_subscription_unblocks_drain() -> None:
    transport = InMemoryTransport()
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(message: IncomingMessage) -> None:
        started.set()
        await release.wait()
        await message.ack()

    subscription = await transport.subscribe("orders", handler, SubscribeOptions(group="billing"))
    await transport.send_message(_message("orders", b"1"))
    await asyncio.wait_for(started.wait(), timeout=1)
    await transport.send_message(_message("orders", b"2"))
    await transport.send_message(_message("orders", b"3"))
    closing = asyncio.create_task(subscription.close())
    await asyncio.sleep(0)
    release.set()
    await asyncio.wait_for(closing, timeout=1)
    await transport.drain(timeout=1)
    await transport.close()


async def test_closing_one_of_two_subscribers_keeps_the_queue() -> None:
    transport = InMemoryTransport()
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    release = asyncio.Event()
    third = asyncio.Event()
    seen: list[bytes] = []

    async def record(started: asyncio.Event, message: IncomingMessage) -> None:
        started.set()
        await release.wait()
        seen.append(message.body)
        if message.body == b"3":
            third.set()
        await message.ack()

    subscription = await transport.subscribe(
        "orders",
        lambda message: record(first_started, message),
        SubscribeOptions(group="billing"),
    )
    await transport.subscribe(
        "orders",
        lambda message: record(second_started, message),
        SubscribeOptions(group="billing"),
    )
    await transport.send_message(_message("orders", b"1"))
    await transport.send_message(_message("orders", b"2"))
    await asyncio.wait_for(first_started.wait(), timeout=1)
    await asyncio.wait_for(second_started.wait(), timeout=1)
    await transport.send_message(_message("orders", b"3"))
    closing = asyncio.create_task(subscription.close())
    await asyncio.sleep(0)
    release.set()
    await asyncio.wait_for(closing, timeout=1)
    await asyncio.wait_for(third.wait(), timeout=1)
    await transport.close()
    assert seen.count(b"3") == 1


async def test_subscribe_after_close_receives_the_next_message() -> None:
    transport = InMemoryTransport()
    release = asyncio.Event()
    seen: list[bytes] = []

    async def first(message: IncomingMessage) -> None:
        await release.wait()
        await message.ack()

    async def second(message: IncomingMessage) -> None:
        seen.append(bytes(message.body))
        await message.ack()

    subscription = await transport.subscribe("orders", first, SubscribeOptions(group="billing"))
    await transport.send_message(_message("orders", b"1"))
    await asyncio.sleep(0)
    release.set()
    await subscription.close()
    await transport.subscribe("orders", second, SubscribeOptions(group="billing"))
    await transport.send_message(_message("orders", b"2"))
    await transport.drain(timeout=1)
    await transport.close()
    assert seen == [b"2"]


async def test_double_ack_and_nack_do_nothing() -> None:
    transport = InMemoryTransport()
    seen: list[int] = []

    async def handler(message: IncomingMessage) -> None:
        seen.append(message.delivery_attempt)
        await message.ack()
        await message.ack()
        await message.nack(requeue=True)

    await transport.subscribe("orders", handler, SubscribeOptions(group="billing"))
    await transport.send_message(_message("orders", b"1"))
    await transport.drain(timeout=2)
    await asyncio.sleep(0)
    assert seen == [1]
    await transport.close()


async def test_nack_without_requeue_publishes_once() -> None:
    transport = InMemoryTransport()
    dead: list[bytes] = []

    async def handler(message: IncomingMessage) -> None:
        await message.nack(requeue=False)

    async def dead_letter(message: IncomingMessage) -> None:
        dead.append(message.body)
        await message.ack()

    await transport.subscribe("orders", handler, SubscribeOptions(group="billing"))
    await transport.subscribe("orders.dlq", dead_letter, SubscribeOptions(group="dead"))
    await transport.send_message(_message("orders", b"row", key="k"))
    await transport.drain(timeout=2)
    assert dead == [b"row"]
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


async def test_group_copies_settle_independently() -> None:
    transport = InMemoryTransport()
    stable: list[bytes] = []
    retried: list[bytes] = []

    async def ack_once(message: IncomingMessage) -> None:
        stable.append(message.body)
        await message.ack()

    async def retry_once(message: IncomingMessage) -> None:
        retried.append(message.body)
        if len(retried) == 1:
            await message.nack(requeue=True)
            return
        await message.ack()

    await transport.subscribe("orders", ack_once, SubscribeOptions(group="stable"))
    await transport.subscribe("orders", retry_once, SubscribeOptions(group="retry"))
    await transport.send_message(_message("orders", b"1"))
    await transport.drain(timeout=2)
    assert stable == [b"1"]
    assert retried == [b"1", b"1"]
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


async def test_closing_the_handler_returns_the_message_to_the_group() -> None:
    transport = InMemoryTransport()
    started = asyncio.Event()
    seen: list[bytes] = []

    async def first(message: IncomingMessage) -> None:
        started.set()
        await asyncio.Event().wait()
        await message.ack()

    async def second(message: IncomingMessage) -> None:
        seen.append(bytes(message.body))
        await message.ack()

    subscription = await transport.subscribe("orders", first, SubscribeOptions(group="billing"))
    await transport.send_message(_message("orders", b"1"))
    await asyncio.wait_for(started.wait(), timeout=1)
    await transport.subscribe("orders", second, SubscribeOptions(group="billing"))
    await subscription.close()
    await transport.drain(timeout=1)
    await transport.close()
    assert seen == [b"1"]


async def test_a_group_keeps_the_start_position_of_its_first_subscriber() -> None:
    transport = InMemoryTransport()
    await transport.subscribe("orders", _unused, SubscribeOptions(group="billing"))
    with pytest.raises(UnsupportedCapability, match="already consumes from latest"):
        await transport.subscribe(
            "orders",
            _unused,
            SubscribeOptions(group="billing", start_from="earliest"),
        )
    await transport.close()


async def test_rejected_subscribe_options() -> None:
    transport = InMemoryTransport()
    with pytest.raises(UnsupportedCapability):
        await transport.subscribe(
            "orders",
            lambda message: _unused(message),
            SubscribeOptions(group="billing", prefetch=0),
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
