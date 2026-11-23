"""
Kafka transport contract.

Reuses ``tests/unit/transport/test_contract.py``. Skips when nothing is accepting
connections on ``localhost:9092``. Does not start Docker.
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
    pytest.importorskip("aiokafka")
    if not _port_open("localhost", 9092):
        pytest.skip("Kafka is not reachable at localhost:9092")
    from cqrs.message_brokers.kafka import KafkaTransport

    await _commit_shared_groups_at_end()
    broker = KafkaTransport("kafka://localhost:9092")
    try:
        await broker.start()
    except Exception as exc:
        await broker.close()
        pytest.skip(f"Kafka is not reachable at localhost:9092: {exc}")
    try:
        yield broker
    finally:
        await broker.close()


async def _commit_shared_groups_at_end() -> None:
    """Move the contract's reused groups to the log end.

    ``start_from="latest"`` resumes a committed offset. These tests publish to
    the same topics and groups on a broker that keeps that offset, so each case
    commits the end before it subscribes and then sees only its own records.
    """

    import aiokafka

    pairs = (
        ("orders", "billing"),
        ("orders", "warehouse"),
        ("orders", "other"),
        ("orders.dlq", "dead"),
    )
    for topic, group in pairs:
        consumer = aiokafka.AIOKafkaConsumer(
            topic,
            bootstrap_servers="localhost:9092",
            group_id=group,
            enable_auto_commit=False,
            auto_offset_reset="latest",
        )
        try:
            await consumer.start()
            await _wait_until_assigned(consumer, topic)
            positioned = await consumer.seek_to_end()
            if positioned is False:
                raise TimeoutError(f"Kafka did not seek topic {topic} for group {group}")
            offsets = {partition: await consumer.position(partition) for partition in consumer.assignment()}
            if offsets:
                await consumer.commit(offsets)
        finally:
            await consumer.stop()


async def _wait_until_assigned(consumer: object, topic: str) -> None:
    import asyncio

    assigned = getattr(consumer, "assignment")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 30
    while not assigned():
        if loop.time() >= deadline:
            raise TimeoutError(f"Kafka assigned no partitions for topic {topic}")
        await asyncio.sleep(0.05)


def test_create_transport_builds_kafka_transport() -> None:
    pytest.importorskip("aiokafka")
    from cqrs.message_brokers.kafka import KafkaTransport
    from cqrs.transport import create_transport

    broker = create_transport("kafka://localhost:9092")
    assert isinstance(broker, KafkaTransport)
    assert broker.capabilities.ordering_per_key is True
    assert broker.capabilities.native_dlq is False
    assert broker.capabilities.delayed_redelivery is False
    assert broker.capabilities.headers is True
    assert not hasattr(KafkaTransport, "__bool__")
    assert not hasattr(KafkaTransport, "__len__")


async def test_round_trip_body_headers_content_type_and_key(transport: Transport) -> None:
    await contract.test_round_trip_body_headers_content_type_and_key(transport)


async def test_groups_broadcast_and_compete(transport: Transport) -> None:
    await contract.test_groups_broadcast_and_compete(transport)


async def test_same_key_stays_ordered_when_concurrency_is_high(transport: Transport) -> None:
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

    ``KafkaTransport`` instances do not share a process-global bus. This case
    does not build them, so it stays the in-memory check from ``test_contract.py``.
    """

    await contract.test_two_instances_do_not_share_messages()
