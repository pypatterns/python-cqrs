"""
NATS JetStream transport against the shared contract.

The fixture uses ``nats://localhost:4222`` and a unique subject prefix so two
tests, or two transports, do not see each other's subjects on one server.
If ``nats-py`` is missing or nothing is listening, the cases skip.
"""

from __future__ import annotations

import socket
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from cqrs.message_brokers.protocol import Message
from cqrs.transport.protocol import IncomingMessage, SubscribeOptions, Transport
from tests.unit.transport import test_contract as contract

_NATS_URL = "nats://localhost:4222"
_ContractCase = Callable[[Transport], Awaitable[None]]

_CONTRACT_CASES: tuple[_ContractCase, ...] = (
    contract.test_round_trip_body_headers_content_type_and_key,
    contract.test_groups_broadcast_and_compete,
    contract.test_same_key_stays_ordered_when_concurrency_is_high,
    contract.test_different_keys_run_side_by_side,
    contract.test_ack_does_not_redeliver,
    contract.test_nack_increases_attempt_then_dead_letters,
    contract.test_handler_exception_does_not_escape_send,
    contract.test_close_with_in_flight_message_then_rejects_send,
)


def _nats_port_open() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 4222), timeout=0.3):
            return True
    except OSError:
        return False


def _skip_without_nats() -> None:
    pytest.importorskip("nats")
    if not _nats_port_open():
        pytest.skip("NATS is not listening on nats://localhost:4222")


@pytest.fixture
async def transport() -> AsyncIterator[Transport]:
    """One JetStream transport isolated by a fresh subject prefix."""

    _skip_without_nats()
    from cqrs.message_brokers.nats import NATSTransport

    prefix = f"cqrs.{uuid.uuid4().hex}."
    broker = NATSTransport(
        _NATS_URL,
        subject_prefix=prefix,
        storage="memory",
        delete_stream_on_close=True,
    )
    await broker.start()
    try:
        yield broker
    finally:
        await broker.close()


@pytest.mark.parametrize("case", _CONTRACT_CASES, ids=lambda case: case.__name__)
async def test_nats_jetstream_contract(transport: Transport, case: _ContractCase) -> None:
    await case(transport)


async def test_two_instances_do_not_share_messages() -> None:
    """The shared contract builds two in-memory transports. It does not need NATS."""

    await contract.test_two_instances_do_not_share_messages()


def test_factory_reads_nats_url_without_connecting() -> None:
    pytest.importorskip("nats")
    from cqrs.message_brokers.nats import NATSTransport
    from cqrs.transport import create_transport

    transport = create_transport("nats://localhost:4222?subject_prefix=cqrs.factory.")
    assert isinstance(transport, NATSTransport)
    assert transport.subject_prefix == "cqrs.factory."


def test_nats_transport_is_truthy_without_bool_or_len() -> None:
    pytest.importorskip("nats")
    from cqrs.message_brokers.nats import NATSTransport

    broker = NATSTransport("nats://localhost:4222", subject_prefix="cqrs.unused.")
    assert broker
    assert bool(broker) is True
    assert not hasattr(NATSTransport, "__bool__")
    assert not hasattr(NATSTransport, "__len__")


async def test_subject_prefix_isolates_two_transports_on_one_server() -> None:
    _skip_without_nats()
    from cqrs.message_brokers.nats import NATSTransport

    def _open() -> NATSTransport:
        return NATSTransport(
            _NATS_URL,
            subject_prefix=f"cqrs.{uuid.uuid4().hex}.",
            storage="memory",
            delete_stream_on_close=True,
        )

    left = _open()
    right = _open()
    left_seen: list[bytes] = []
    right_seen: list[bytes] = []

    async def collect(sink: list[bytes], message: IncomingMessage) -> None:
        sink.append(message.body)
        await message.ack()

    await left.start()
    await right.start()
    try:
        await left.subscribe(
            "orders",
            lambda message: collect(left_seen, message),
            SubscribeOptions(group="billing"),
        )
        await right.subscribe(
            "orders",
            lambda message: collect(right_seen, message),
            SubscribeOptions(group="billing"),
        )
        await left.send_message(
            Message(
                message_name="nats",
                topic="orders",
                payload=b"left",
                payload_bytes=b"left",
            ),
        )
        await left.drain(timeout=2)
        await right.drain(timeout=2)
        assert left_seen == [b"left"]
        assert right_seen == []
    finally:
        await left.close()
        await right.close()
