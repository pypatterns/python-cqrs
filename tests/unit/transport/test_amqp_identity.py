"""AMQP queue identity, header view, and per-group retry state. No broker."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

pytest.importorskip("aio_pika")

from cqrs.message_brokers.amqp import (  # noqa: E402
    AMQPTransport,
    _AMQPGroup,
    _AMQPIncoming,
    _AMQPSubscription,
    _queue_name,
    _view,
)
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
    headers, key, attempt, wire_id = _view(
        {
            "key": "business-value",
            "cqrs-key": "order-1",
            "cqrs-message-id": "abc",
            "cqrs-delivery-attempt": "2",
            "other": "kept",
        },
    )
    assert key == "order-1"
    assert headers == {"key": "business-value", "other": "kept"}
    assert attempt == 2
    assert wire_id == "abc"


def test_missing_transport_key_stays_none() -> None:
    headers, key, attempt, wire_id = _view({"key": "business-value"})
    assert headers == {"key": "business-value"}
    assert key is None
    assert attempt is None
    assert wire_id is None


def test_attempt_state_is_scoped_to_the_group() -> None:
    transport = AMQPTransport("amqp://guest:guest@localhost:5672/")
    transport._attempts[("orders", "billing", "same")] = 2
    assert transport._attempt_for("orders", "billing", "same", None) == 2
    assert transport._attempt_for("orders", "warehouse", "same", None) == 1
    assert transport._attempt_for("payments", "billing", "same", None) == 1


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

    assert transport._groups == {}


async def test_concurrent_final_nack_publishes_one_dead_letter() -> None:
    published = 0
    entered = asyncio.Event()
    release = asyncio.Event()

    class _Delivery:
        topic = "orders"
        body = b"body"
        content_type = None
        message_id = "m1"

        def __init__(self) -> None:
            self.settled = 0

        async def ack(self) -> None:
            self.settled += 1

        async def nack(self, *, requeue: bool) -> None:
            self.settled += 1

    class _Broker:
        async def _dead_letter(self, incoming: object) -> None:
            nonlocal published
            published += 1
            entered.set()
            await release.wait()

        def _forget(self, attempt_key: object) -> None:
            return None

        def _finish_incoming(self, incoming: object) -> None:
            return None

    broker = _Broker()
    group = _AMQPGroup("orders", "billing")
    options = SubscribeOptions(group="billing")
    subscription = _AMQPSubscription(broker, group, lambda message: asyncio.sleep(0), options)  # type: ignore[arg-type]
    delivery = _Delivery()
    incoming = _AMQPIncoming(
        broker,  # type: ignore[arg-type]
        subscription,
        delivery,  # type: ignore[arg-type]
        headers={},
        key=None,
        delivery_attempt=1,
    )

    first = asyncio.create_task(incoming.nack(requeue=False))
    await entered.wait()
    second = asyncio.create_task(incoming.nack(requeue=False))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)

    assert published == 1
    assert delivery.settled == 1
