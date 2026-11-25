"""
Transport contract.

Later backends run this module against their own ``transport`` fixture.
PR1 wires that fixture to ``InMemoryTransport`` in ``conftest.py``.
"""

from __future__ import annotations

import asyncio

import pytest

from cqrs.message_brokers.protocol import Message
from cqrs.serializers.default import message_wire_bytes
from cqrs.transport import IncomingMessage, SubscribeOptions, TransportClosed
from cqrs.transport.protocol import Transport


def _message(
    topic: str,
    *,
    payload: object | None = None,
    payload_bytes: bytes | None = None,
    headers: dict[str, str] | None = None,
    content_type: str | None = None,
    key: str | None = None,
) -> Message:
    return Message(
        message_name="contract",
        topic=topic,
        payload={"n": 1} if payload is None else payload,
        payload_bytes=payload_bytes,
        headers=headers,
        content_type=content_type,
        key=key,
    )


async def _collect(
    transport: Transport,
    topic: str,
    options: SubscribeOptions,
    sink: list[IncomingMessage],
) -> None:
    async def handler(message: IncomingMessage) -> None:
        sink.append(message)
        await message.ack()

    await transport.subscribe(topic, handler, options)


async def test_round_trip_body_headers_content_type_and_key(transport: Transport) -> None:
    received: list[IncomingMessage] = []
    message = _message(
        "orders",
        payload={"sku": "book"},
        headers={"source": "test"},
        content_type="application/json",
        key="order-1",
    )
    await _collect(transport, "orders", SubscribeOptions(group="billing"), received)

    await transport.send_message(message)
    await transport.drain(timeout=2)

    assert len(received) == 1
    incoming = received[0]
    assert incoming.body == message_wire_bytes(message)
    assert not isinstance(incoming.body, Message)
    assert incoming.headers == {"source": "test"}
    assert incoming.content_type == "application/json"
    assert incoming.key == "order-1"
    assert incoming.topic == "orders"
    assert incoming.delivery_attempt == 1


async def test_groups_broadcast_and_compete(transport: Transport) -> None:
    billing: list[bytes] = []
    warehouse: list[bytes] = []
    rival: list[bytes] = []

    async def collect(sink: list[bytes], message: IncomingMessage) -> None:
        sink.append(message.body)
        await message.ack()

    await transport.subscribe("orders", lambda message: collect(billing, message), SubscribeOptions(group="billing"))
    await transport.subscribe(
        "orders",
        lambda message: collect(warehouse, message),
        SubscribeOptions(group="warehouse"),
    )
    await transport.subscribe("orders", lambda message: collect(rival, message), SubscribeOptions(group="billing"))
    message = _message("orders", payload_bytes=b"one")

    await transport.send_message(message)
    await transport.drain(timeout=2)

    assert billing + rival == [b"one"]
    assert sorted((billing, rival), key=len)[1] == [b"one"]
    assert warehouse == [b"one"]
    assert len(billing) + len(rival) == 1


async def test_ack_does_not_redeliver(transport: Transport) -> None:
    seen: list[int] = []

    async def handler(message: IncomingMessage) -> None:
        seen.append(message.delivery_attempt)
        await message.ack()

    await transport.subscribe("orders", handler, SubscribeOptions(group="billing"))
    await transport.send_message(_message("orders"))
    await transport.drain(timeout=2)
    await asyncio.sleep(0)
    assert seen == [1]


async def test_handler_exception_does_not_escape_send(transport: Transport) -> None:
    async def handler(message: IncomingMessage) -> None:
        raise RuntimeError("subscriber failed")

    await transport.subscribe(
        "orders",
        handler,
        SubscribeOptions(group="billing", max_deliveries=1),
    )
    await transport.send_message(_message("orders"))
    await transport.drain(timeout=2)


async def test_close_with_in_flight_message_then_rejects_send(transport: Transport) -> None:
    started = asyncio.Event()

    async def handler(message: IncomingMessage) -> None:
        started.set()
        await asyncio.Event().wait()

    await transport.subscribe("orders", handler, SubscribeOptions(group="billing"))
    await transport.send_message(_message("orders"))
    await asyncio.wait_for(started.wait(), timeout=1)

    await asyncio.wait_for(transport.close(), timeout=1)
    with pytest.raises(TransportClosed):
        await transport.send_message(_message("orders"))
    with pytest.raises(TransportClosed):
        await transport.subscribe("orders", handler, SubscribeOptions(group="other"))


async def test_two_instances_do_not_share_messages() -> None:
    from cqrs.transport import InMemoryTransport

    left = InMemoryTransport()
    right = InMemoryTransport()
    left_seen: list[bytes] = []
    right_seen: list[bytes] = []

    async def collect(sink: list[bytes], message: IncomingMessage) -> None:
        sink.append(message.body)
        await message.ack()

    try:
        await left.subscribe("orders", lambda message: collect(left_seen, message), SubscribeOptions(group="g"))
        await right.subscribe("orders", lambda message: collect(right_seen, message), SubscribeOptions(group="g"))
        await left.send_message(_message("orders", payload_bytes=b"left"))
        await left.drain(timeout=2)
        await right.drain(timeout=2)
        assert left_seen == [b"left"]
        assert right_seen == []
    finally:
        await left.close()
        await right.close()
