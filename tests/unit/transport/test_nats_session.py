"""JetStream stream ownership and final dead-letter recovery. No broker."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

pytest.importorskip("nats")

from nats.js.errors import APIError, NotFoundError  # noqa: E402

from cqrs.adapters.nats import JetStreamSession  # noqa: E402
from cqrs.message_brokers.nats import (  # noqa: E402
    NATSTransport,
    _Group,
    _NATSIncoming,
    _Parsed,
    _Slot,
)
from cqrs.transport.errors import TransportError  # noqa: E402
from cqrs.transport.protocol import SubscribeOptions  # noqa: E402


class _Open:
    is_closed = False


class _Stream:
    def __init__(self, subjects: list[str], description: str | None) -> None:
        self.config = SimpleNamespace(subjects=list(subjects), description=description)


class _JetStream:
    def __init__(self, shared: dict[str, _Stream] | None = None, *, idempotent: bool = False) -> None:
        self.shared = shared if shared is not None else {}
        self.idempotent = idempotent

    async def stream_info(self, name: str) -> _Stream:
        try:
            return self.shared[name]
        except KeyError:
            raise NotFoundError() from None

    async def add_stream(self, **kwargs: object) -> _Stream:
        name = str(kwargs["name"])
        if name in self.shared:
            if self.idempotent:
                return self.shared[name]
            raise APIError(err_code=10058, description="stream name already in use")
        info = _Stream(list(kwargs["subjects"]), str(kwargs.get("description")))  # type: ignore[arg-type]
        self.shared[name] = info
        return info


def _session(jetstream: _JetStream) -> JetStreamSession:
    session = JetStreamSession("nats://localhost:4222")
    session._nc = _Open()  # type: ignore[assignment]
    session._js = jetstream  # type: ignore[assignment]
    return session


async def test_ensure_stream_checks_each_subject_before_caching() -> None:
    session = _session(_JetStream())

    await session.ensure_stream("orders", "orders")
    with pytest.raises(TransportError, match="payments"):
        await session.ensure_stream("orders", "payments")

    assert ("orders", "orders") in session._ensured
    assert ("orders", "payments") not in session._ensured


async def test_created_stream_is_owned_only_with_this_sessions_token() -> None:
    session = _session(_JetStream())

    await session.ensure_stream("orders", "orders")

    assert session._owned == {"orders"}


async def test_idempotent_add_does_not_own_an_existing_stream() -> None:
    class _Idempotent(_JetStream):
        async def stream_info(self, name: str) -> _Stream:
            raise NotFoundError()

        async def add_stream(self, **kwargs: object) -> _Stream:
            return _Stream(["orders"], "cqrs:someone-else")

    session = _session(_Idempotent())

    await session.ensure_stream("orders", "orders")

    assert session._owned == set()
    assert ("orders", "orders") in session._ensured


class _Wire:
    def __init__(self, *, delivered: int = 1, key: str | None = None) -> None:
        self.data = b"body"
        self.delivery_count = delivered
        self.headers = {} if key is None else {"cqrs-key": key}
        self.actions: list[str] = []

    async def ack(self) -> None:
        self.actions.append("ack")

    async def nak(self) -> None:
        self.actions.append("nak")

    async def term(self) -> None:
        self.actions.append("term")


def _incoming(
    transport: NATSTransport,
    *,
    delivered: int,
    group_limit: int,
    slot_limit: int,
) -> tuple[_NATSIncoming, _Wire, _Group]:
    group = _Group("orders", "billing", "orders", "orders")
    group.delivery_limit = group_limit
    options = SubscribeOptions(group="billing", max_deliveries=slot_limit)
    slot = _Slot(lambda message: asyncio.sleep(0), options)  # type: ignore[arg-type]
    wire = _Wire(delivered=delivered)
    parsed = _Parsed({}, None, None, None)
    incoming = _NATSIncoming(transport, group, slot, wire, parsed)  # type: ignore[arg-type]
    return incoming, wire, group


async def test_requeue_uses_the_durable_limit_not_the_joining_slot() -> None:
    transport = NATSTransport("nats://localhost:4222")
    incoming, wire, _group = _incoming(transport, delivered=1, group_limit=1, slot_limit=5)
    published: list[object] = []

    async def dead_letter(message: _NATSIncoming) -> None:
        published.append(message)

    transport._dead_letter = dead_letter  # type: ignore[method-assign]

    await incoming.nack(requeue=True)

    assert published == [incoming]
    assert wire.actions == ["term"]
    assert incoming.settled


async def test_final_dead_letter_failure_is_reported_by_drain() -> None:
    transport = NATSTransport("nats://localhost:4222")
    incoming, wire, group = _incoming(transport, delivered=1, group_limit=1, slot_limit=1)
    group.outstanding = 1

    async def dead_letter(message: _NATSIncoming) -> None:
        raise RuntimeError("dlq down")

    transport._dead_letter = dead_letter  # type: ignore[method-assign]

    await incoming.nack(requeue=False)

    assert wire.actions == []
    assert not incoming.settled
    assert group.outstanding == 1
    with pytest.raises(TransportError, match="dead-letter publish failed"):
        await transport.drain(timeout=0.2)


async def test_drain_retries_a_held_dead_letter_without_the_handler() -> None:
    transport = NATSTransport("nats://localhost:4222")
    incoming, wire, group = _incoming(transport, delivered=1, group_limit=1, slot_limit=1)
    group.outstanding = 1
    calls = 0

    async def dead_letter(message: _NATSIncoming) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("dlq down")

    transport._dead_letter = dead_letter  # type: ignore[method-assign]
    await incoming.nack(requeue=False)

    await transport.drain(timeout=0.2)

    assert calls == 2
    assert wire.actions == ["term"]
    assert incoming.settled
    assert group.outstanding == 0


async def test_closed_slot_reassigns_a_keyed_delivery() -> None:
    transport = NATSTransport("nats://localhost:4222")
    group = _Group("orders", "billing", "orders", "orders")
    group.delivery_limit = 5
    options = SubscribeOptions(group="billing")
    closed = _Slot(lambda message: asyncio.sleep(0), options)  # type: ignore[arg-type]
    closed.active = False
    ran: list[bytes] = []

    async def handler(message: object) -> None:
        ran.append(bytes(getattr(message, "body")))
        await getattr(message, "ack")()

    active = _Slot(handler, options)  # type: ignore[arg-type]
    group.slots.extend([closed, active])
    wire = _Wire(key="order-1")

    await transport._dispatch(group, closed, wire)  # type: ignore[arg-type]

    assert ran == [b"body"]
    assert wire.actions == ["ack"]
