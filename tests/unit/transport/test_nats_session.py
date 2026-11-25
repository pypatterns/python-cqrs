"""JetStream stream ownership and nack. No broker."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("nats")

from nats.js.errors import APIError, NotFoundError  # noqa: E402

from cqrs.adapters.nats import JetStreamSession  # noqa: E402
from cqrs.message_brokers.nats import NATSTransport, _NATSIncoming  # noqa: E402
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
        self.stream_seq = 1
        self.headers = {} if key is None else {"cqrs-key": key}
        self.actions: list[str] = []

    async def ack(self) -> None:
        self.actions.append("ack")

    async def nak(self) -> None:
        self.actions.append("nak")

    async def term(self) -> None:
        self.actions.append("term")


def _incoming(transport: NATSTransport, *, delivered: int, limit: int) -> tuple[_NATSIncoming, _Wire]:
    options = SubscribeOptions(group="billing", max_deliveries=limit)
    subscription = SimpleNamespace(topic="orders", options=options, delivery_limit=limit)
    wire = _Wire(delivered=delivered)
    incoming = _NATSIncoming(
        transport,
        subscription,  # type: ignore[arg-type]
        wire,  # type: ignore[arg-type]
        headers={},
        content_type=None,
        key=None,
    )
    return incoming, wire


async def test_requeue_naks_before_the_broker_limit() -> None:
    transport = NATSTransport("nats://localhost:4222")
    incoming, wire = _incoming(transport, delivered=1, limit=5)

    await incoming.nack(requeue=True)

    assert wire.actions == ["nak"]
    assert incoming._settled


async def test_requeue_at_the_broker_limit_terms() -> None:
    transport = NATSTransport("nats://localhost:4222")
    incoming, wire = _incoming(transport, delivered=2, limit=2)

    await incoming.nack(requeue=True)

    assert wire.actions == ["term"]
    assert incoming._settled


async def test_nack_without_requeue_terms() -> None:
    transport = NATSTransport("nats://localhost:4222")
    incoming, wire = _incoming(transport, delivered=1, limit=5)

    await incoming.nack(requeue=False)

    assert wire.actions == ["term"]
    assert incoming._settled
