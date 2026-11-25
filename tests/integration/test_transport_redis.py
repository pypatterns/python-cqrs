"""Redis Streams transport against the shared contract."""

from __future__ import annotations

import asyncio
import socket
import uuid
import urllib.parse
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from cqrs.message_brokers.protocol import Message
from cqrs.message_brokers.redis import RedisTransport, create_transport
from cqrs.transport import IncomingMessage, SubscribeOptions
from cqrs.transport.protocol import TransportCapabilities
from tests.unit.transport import test_contract as contract

_URL = "redis://localhost:6379/0"
_PARSED = urllib.parse.urlparse(_URL)


def _port_open(host: str = "localhost", port: int = 6379) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.2):
            return True
    except OSError:
        return False


def _skip_if_redis_down() -> None:
    if not _port_open():
        pytest.skip("Redis is not listening on localhost:6379")


async def _delete_prefix(prefix: str) -> None:
    client = Redis.from_url(_URL, socket_connect_timeout=1.0)
    try:
        keys = await client.keys(prefix + "*")
        if keys:
            await client.delete(*keys)
    except (OSError, RedisError):
        return
    finally:
        await client.aclose()


@pytest.fixture
async def transport() -> AsyncIterator[RedisTransport]:
    """One isolated Redis transport. Skips when port 6379 is closed."""

    _skip_if_redis_down()
    prefix = f"cqrs:{uuid.uuid4().hex}:"
    broker = RedisTransport(_PARSED, prefix=prefix)
    try:
        await broker.start()
    except (OSError, RedisConnectionError, RedisTimeoutError) as exc:
        await broker.close()
        pytest.skip(f"Redis is not reachable: {exc}")
    try:
        yield broker
    finally:
        await broker.close()
        await _delete_prefix(prefix)


async def test_round_trip_body_headers_content_type_and_key(transport: RedisTransport) -> None:
    await contract.test_round_trip_body_headers_content_type_and_key(transport)


async def test_groups_broadcast_and_compete(transport: RedisTransport) -> None:
    await contract.test_groups_broadcast_and_compete(transport)


async def test_ack_does_not_redeliver(transport: RedisTransport) -> None:
    await contract.test_ack_does_not_redeliver(transport)


async def test_handler_exception_does_not_escape_send(transport: RedisTransport) -> None:
    await contract.test_handler_exception_does_not_escape_send(transport)


async def test_close_with_in_flight_message_then_rejects_send(transport: RedisTransport) -> None:
    await contract.test_close_with_in_flight_message_then_rejects_send(transport)


async def test_two_instances_do_not_share_messages() -> None:
    # The shared contract builds InMemoryTransport itself. Redis isolation is
    # test_prefixes_isolate_streams_on_one_redis below.
    await contract.test_two_instances_do_not_share_messages()


async def test_create_transport_builds_redis_transport() -> None:
    broker = create_transport("redis://localhost:6379/0?prefix=cqrs-factory:")
    try:
        assert isinstance(broker, RedisTransport)
        capabilities: TransportCapabilities = broker.capabilities
        assert capabilities.ordering_per_key is False
        assert capabilities.native_dlq is False
        assert capabilities.delayed_redelivery is True
        assert capabilities.headers is True
        assert capabilities.topic == "Redis stream key"
        assert not hasattr(RedisTransport, "__bool__")
        assert not hasattr(RedisTransport, "__len__")
    finally:
        await broker.close()


async def test_prefixes_isolate_streams_on_one_redis() -> None:
    _skip_if_redis_down()
    left_prefix = f"cqrs:{uuid.uuid4().hex}:"
    right_prefix = f"cqrs:{uuid.uuid4().hex}:"
    left = RedisTransport(_PARSED, prefix=left_prefix)
    right = RedisTransport(_PARSED, prefix=right_prefix)
    left_seen: list[bytes] = []
    right_seen: list[bytes] = []

    async def collect(sink: list[bytes], message: IncomingMessage) -> None:
        sink.append(message.body)
        await message.ack()

    try:
        try:
            await left.start()
            await right.start()
        except (OSError, RedisConnectionError, RedisTimeoutError) as exc:
            pytest.skip(f"Redis is not reachable: {exc}")
        await left.subscribe("orders", lambda message: collect(left_seen, message), SubscribeOptions(group="g"))
        await right.subscribe("orders", lambda message: collect(right_seen, message), SubscribeOptions(group="g"))
        await left.send_message(
            Message(message_name="contract", topic="orders", payload=b"left", payload_bytes=b"left"),
        )
        await left.drain(timeout=2)
        await right.drain(timeout=2)
        assert left_seen == [b"left"]
        assert right_seen == []
    finally:
        await left.close()
        await right.close()
        await _delete_prefix(left_prefix)
        await _delete_prefix(right_prefix)


async def test_xautoclaim_redelivers_a_stuck_message() -> None:
    _skip_if_redis_down()
    prefix = f"cqrs:{uuid.uuid4().hex}:"
    broker = RedisTransport(_PARSED, prefix=prefix, claim_idle_ms=80, block_ms=40)
    attempts: list[int] = []

    async def handler(message: IncomingMessage) -> None:
        attempts.append(message.delivery_attempt)
        if len(attempts) == 1:
            return
        await message.ack()

    try:
        try:
            await broker.start()
        except (OSError, RedisConnectionError, RedisTimeoutError) as exc:
            pytest.skip(f"Redis is not reachable: {exc}")
        await broker.subscribe(
            "orders",
            handler,
            SubscribeOptions(group="billing"),
        )
        await broker.send_message(
            Message(message_name="contract", topic="orders", payload=b"stuck", payload_bytes=b"stuck"),
        )

        async def saw_second() -> None:
            while 2 not in attempts:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(saw_second(), timeout=3)
        await broker.drain(timeout=2)
    finally:
        await broker.close()
        await _delete_prefix(prefix)
    assert attempts[0] == 1
    assert 2 in attempts
