"""AMQP queue identity and header view. No broker."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

pytest.importorskip("aio_pika")

from cqrs.adapters.amqp import AMQPDelivery, NeutralAMQPClient  # noqa: E402
from cqrs.message_brokers.amqp import (  # noqa: E402
    AMQPTransport,
    _AMQPSubscription,
    _queue_name,
    _view,
)
from cqrs.message_brokers.protocol import Message  # noqa: E402
from cqrs.transport.protocol import SubscribeOptions  # noqa: E402


@pytest.mark.parametrize(
    ("exchange", "topic", "group", "other_exchange", "other_topic", "other_group"),
    [
        ("cqrs", "orders", "workers", "cqrs", "payments", "workers"),
        ("cqrs", "a.b", "c", "cqrs", "a", "b.c"),
        ("cqrs", "orders", "workers", "cqrs", "orders", "billing"),
        ("billing", "orders", "workers", "warehouse", "orders", "workers"),
    ],
)
def test_queue_name_changes_when_exchange_topic_or_group_changes(
    exchange: str,
    topic: str,
    group: str,
    other_exchange: str,
    other_topic: str,
    other_group: str,
) -> None:
    assert _queue_name(exchange, topic, group) != _queue_name(other_exchange, other_topic, other_group)


def test_queue_name_is_stable_for_one_exchange_topic_and_group() -> None:
    assert _queue_name("cqrs", "orders", "workers") == _queue_name("cqrs", "orders", "workers")


def test_user_key_header_is_not_the_ordering_key() -> None:
    headers, key = _view(
        {
            "key": "business-value",
            "cqrs-key": "order-1",
            "other": "kept",
        },
    )
    assert key == "order-1"
    assert headers == {"key": "business-value", "other": "kept"}


def test_missing_transport_key_stays_none() -> None:
    headers, key = _view({"key": "business-value"})
    assert headers == {"key": "business-value"}
    assert key is None


async def test_competing_subscribers_pass_one_queue_name() -> None:
    transport = AMQPTransport("amqp://guest:guest@localhost:5672/", exchange_name="billing")
    seen: list[str] = []

    async def connect() -> None:
        return None

    async def consume(*args: object, **kwargs: object) -> SimpleNamespace:
        seen.append(str(kwargs["queue_name"]))

        async def close() -> None:
            return None

        return SimpleNamespace(close=close)

    transport._client.connect = connect  # type: ignore[method-assign]
    transport._client.consume = consume  # type: ignore[method-assign]

    async def handler(message: object) -> None:
        return None

    options = SubscribeOptions(group="workers")
    await transport.subscribe("orders", handler, options)  # type: ignore[arg-type]
    await transport.subscribe("orders", handler, options)  # type: ignore[arg-type]

    assert seen == [
        _queue_name("billing", "orders", "workers"),
        _queue_name("billing", "orders", "workers"),
    ]


async def test_failed_consume_leaves_no_subscription() -> None:
    transport = AMQPTransport("amqp://guest:guest@localhost:5672/")

    async def connect() -> None:
        return None

    async def consume(*args: object, **kwargs: object) -> None:
        raise RuntimeError("consumer setup failed")

    transport._client.connect = connect  # type: ignore[method-assign]
    transport._client.consume = consume  # type: ignore[method-assign]

    async def handler(message: object) -> None:
        return None

    with pytest.raises(RuntimeError, match="consumer setup failed"):
        await transport.subscribe("orders", handler, SubscribeOptions(group="billing"))  # type: ignore[arg-type]

    assert transport._subscriptions == []


async def test_a_subscription_counts_only_after_setup() -> None:
    transport = AMQPTransport("amqp://guest:guest@localhost:5672/")
    counted: list[int] = []

    async def connect() -> None:
        return None

    async def consume(*args: object, **kwargs: object) -> SimpleNamespace:
        counted.append(transport._group_count("orders"))
        if len(counted) == 2:
            raise RuntimeError("setup failed")

        async def close() -> None:
            return None

        return SimpleNamespace(close=close)

    transport._client.connect = connect  # type: ignore[method-assign]
    transport._client.consume = consume  # type: ignore[method-assign]

    async def handler(message: object) -> None:
        return None

    await transport.subscribe("orders", handler, SubscribeOptions(group="billing"))  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="setup failed"):
        await transport.subscribe("orders", handler, SubscribeOptions(group="warehouse"))  # type: ignore[arg-type]

    assert counted == [0, 1]
    assert transport._group_count("orders") == 1


def test_binary_header_replaces_invalid_utf8() -> None:
    message = SimpleNamespace(
        body=b"x",
        routing_key="orders",
        message_id="1",
        content_type=None,
        headers={"trace": b"\xff"},
    )

    delivery = AMQPDelivery(message)  # type: ignore[arg-type]

    assert delivery.headers["trace"] == "\ufffd"


async def test_concurrent_connect_opens_one_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    opened = 0

    class _Connection:
        is_closed = False

        async def close(self) -> None:
            self.is_closed = True

    async def connect_robust(url: str) -> _Connection:
        nonlocal opened
        opened += 1
        await asyncio.sleep(0.05)
        return _Connection()

    monkeypatch.setattr("cqrs.adapters.amqp.aio_pika.connect_robust", connect_robust)
    client = NeutralAMQPClient("amqp://localhost")

    await asyncio.gather(client.connect(), client.connect())

    assert opened == 1
    assert client._connection is not None


async def test_connect_drops_a_connection_that_finishes_after_close(monkeypatch: pytest.MonkeyPatch) -> None:
    started = asyncio.Event()
    closed = asyncio.Event()

    class _Connection:
        def __init__(self) -> None:
            self.is_closed = False

        async def close(self) -> None:
            self.is_closed = True
            closed.set()

    async def connect_robust(url: str) -> _Connection:
        started.set()
        await asyncio.sleep(0.05)
        return _Connection()

    monkeypatch.setattr("cqrs.adapters.amqp.aio_pika.connect_robust", connect_robust)
    client = NeutralAMQPClient("amqp://localhost")
    task = asyncio.create_task(client.connect())
    await started.wait()
    await client.close()
    await task

    assert closed.is_set()
    assert client._connection is None


async def test_cancelled_send_drops_the_drain_reservation() -> None:
    transport = AMQPTransport("amqp://guest:guest@localhost:5672/")
    transport._subscriptions.append(
        _AMQPSubscription(
            transport,
            topic="orders",
            group="billing",
            handler=_unused,
            options=SubscribeOptions(group="billing"),
        ),
    )

    async def connect() -> None:
        return None

    async def publish(**kwargs: object) -> None:
        await asyncio.Future()

    transport._client.connect = connect  # type: ignore[method-assign]
    transport._client.publish = publish  # type: ignore[method-assign]
    send = asyncio.create_task(
        transport.send_message(Message(message_name="order", topic="orders", payload=b"1")),
    )
    for _ in range(50):
        if transport._watch.owed:
            break
        await asyncio.sleep(0)
    assert transport._watch.owed == 1

    send.cancel()
    with pytest.raises(asyncio.CancelledError):
        await send
    await transport.drain(timeout=0.2)


async def _unused(message: object) -> None:
    return None
