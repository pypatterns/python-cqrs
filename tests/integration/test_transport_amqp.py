"""
AMQP transport contract.

Reuses ``tests/unit/transport/test_contract.py``. Skips when nothing is accepting
connections on ``localhost:5672``. Does not start Docker.
"""

from __future__ import annotations

import socket
from collections.abc import AsyncIterator

import pytest

from cqrs.transport.protocol import Transport
from tests.unit.transport import test_contract as contract


def _port_open(host: str, port: int) -> bool:
    sock = socket.socket()
    sock.settimeout(0.3)
    try:
        sock.connect((host, port))
    except OSError:
        return False
    else:
        return True
    finally:
        sock.close()


@pytest.fixture
async def transport() -> AsyncIterator[Transport]:
    pytest.importorskip("aio_pika")
    if not _port_open("localhost", 5672):
        pytest.skip("AMQP is not reachable at amqp://guest:guest@localhost:5672/")
    from cqrs.message_brokers.amqp import AMQPTransport

    broker = AMQPTransport("amqp://guest:guest@localhost:5672/")
    try:
        await broker.start()
    except Exception as exc:
        await broker.close()
        pytest.skip(f"AMQP is not reachable at amqp://guest:guest@localhost:5672/: {exc}")
    try:
        yield broker
    finally:
        await broker.close()


def test_create_transport_builds_amqp_transport() -> None:
    pytest.importorskip("aio_pika")
    from cqrs.message_brokers.amqp import AMQPTransport
    from cqrs.transport import create_transport

    broker = create_transport("amqp://guest:guest@localhost:5672/")
    assert isinstance(broker, AMQPTransport)
    assert broker.capabilities.ordering_per_key is False
    assert broker.capabilities.native_dlq is False
    assert broker.capabilities.delayed_redelivery is False
    assert broker.capabilities.headers is True
    assert not hasattr(AMQPTransport, "__bool__")
    assert not hasattr(AMQPTransport, "__len__")


async def test_round_trip_body_headers_content_type_and_key(transport: Transport) -> None:
    await contract.test_round_trip_body_headers_content_type_and_key(transport)


async def test_groups_broadcast_and_compete(transport: Transport) -> None:
    await contract.test_groups_broadcast_and_compete(transport)


async def test_same_key_stays_ordered_when_concurrency_is_high(transport: Transport) -> None:
    if not transport.capabilities.ordering_per_key:
        pytest.skip(
            "AMQPTransport.capabilities.ordering_per_key is False; "
            "a direct exchange does not order deliveries by message key",
        )
    await contract.test_same_key_stays_ordered_when_concurrency_is_high(transport)


async def test_different_keys_run_side_by_side(transport: Transport) -> None:
    await contract.test_different_keys_run_side_by_side(transport)


async def test_ack_does_not_redeliver(transport: Transport) -> None:
    await contract.test_ack_does_not_redeliver(transport)


async def test_nack_increases_attempt_then_dead_letters(transport: Transport) -> None:
    await contract.test_nack_increases_attempt_then_dead_letters(transport)


async def test_handler_exception_does_not_escape_send(transport: Transport) -> None:
    await contract.test_handler_exception_does_not_escape_send(transport)


async def test_close_with_in_flight_message_then_rejects_send(transport: Transport) -> None:
    await contract.test_close_with_in_flight_message_then_rejects_send(transport)


async def test_two_instances_do_not_share_messages() -> None:
    """The shared contract constructs ``InMemoryTransport`` itself.

    ``AMQPTransport`` instances do not share a process-global bus. This case
    does not build them, so it stays the in-memory check from ``test_contract.py``.
    """

    await contract.test_two_instances_do_not_share_messages()
