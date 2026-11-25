"""Per-module create_transport and lazy broker imports."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from cqrs.transport.errors import TransportError, UnsupportedCapability
from cqrs.transport.memory import InMemoryTransport, create_transport


def _pythonpath() -> dict[str, str]:
    env = os.environ.copy()
    src = Path(__file__).resolve().parents[3] / "src"
    env["PYTHONPATH"] = str(src) + os.pathsep + env.get("PYTHONPATH", "")
    return env


def test_import_cqrs_does_not_load_broker_clients() -> None:
    script = """
import sys
import cqrs
import cqrs.transport
banned = ("aio_pika", "aiokafka", "redis", "nats", "aioboto3")
loaded = [name for name in banned if name in sys.modules]
if loaded:
    raise SystemExit("loaded " + ",".join(loaded))
assert cqrs.InMemoryTransport is not None
assert not hasattr(cqrs, "create_transport")
lazy = (
    "KafkaTransport",
    "AMQPTransport",
    "RedisTransport",
    "NATSTransport",
    "SQSTransport",
)
for name in lazy:
    assert name not in cqrs.__all__
    assert not hasattr(cqrs, name)
    assert name in cqrs.transport._LAZY_TRANSPORTS
loaded = [name for name in banned if name in sys.modules]
if loaded:
    raise SystemExit("loaded after listing lazy names " + ",".join(loaded))
"""
    subprocess.run([sys.executable, "-c", script], check=True, env=_pythonpath())


def test_lazy_transport_attribute_imports_its_client() -> None:
    script = """
import sys
import cqrs.transport
assert "aio_pika" not in sys.modules
try:
    broker = cqrs.transport.AMQPTransport
except ImportError as exc:
    raise SystemExit("reading AMQPTransport did not import the amqp module: " + str(exc))
assert broker.__name__ == "AMQPTransport"
assert "aio_pika" in sys.modules
"""
    subprocess.run([sys.executable, "-c", script], check=True, env=_pythonpath())


def test_importing_memory_does_not_load_broker_clients() -> None:
    script = """
import sys
import cqrs.transport.memory
banned = ("aio_pika", "aiokafka", "redis", "nats", "aioboto3")
loaded = [name for name in banned if name in sys.modules]
if loaded:
    raise SystemExit("loaded " + ",".join(loaded))
from cqrs.transport.memory import create_transport
assert create_transport("memory://") is not None
"""
    subprocess.run([sys.executable, "-c", script], check=True, env=_pythonpath())


async def test_memory_url_and_query() -> None:
    default = create_transport("memory://")
    configured = create_transport("memory://?delivery=inline&redelivery_delay=0")
    try:
        assert isinstance(default, InMemoryTransport)
        assert default.delivery == "queued"
        assert isinstance(configured, InMemoryTransport)
        assert configured.delivery == "inline"
        assert configured.redelivery_delay == 0
    finally:
        await default.close()
        await configured.close()


def test_memory_rejects_foreign_scheme() -> None:
    with pytest.raises(TransportError, match="scheme must be 'memory'"):
        create_transport("mqtt://localhost")


def test_memory_url_without_scheme() -> None:
    with pytest.raises(TransportError, match="has no scheme"):
        create_transport("not-a-url")


def test_invalid_memory_delivery() -> None:
    with pytest.raises(UnsupportedCapability):
        create_transport("memory://?delivery=fast")


def test_kafka_create_transport_rejects_foreign_scheme() -> None:
    pytest.importorskip("aiokafka")
    from cqrs.message_brokers.kafka import create_transport as create_kafka

    with pytest.raises(TransportError, match="scheme must be 'kafka'"):
        create_kafka("amqp://localhost")


def test_kafka_create_transport_rejects_missing_scheme() -> None:
    pytest.importorskip("aiokafka")
    from cqrs.message_brokers.kafka import create_transport as create_kafka

    with pytest.raises(TransportError, match="has no scheme"):
        create_kafka("not-a-url")


def test_amqp_create_transport_rejects_wrong_scheme() -> None:
    pytest.importorskip("aio_pika")
    from cqrs.message_brokers.amqp import create_transport as create_amqp

    with pytest.raises(TransportError, match="scheme must be 'amqp'"):
        create_amqp("kafka://localhost")
    with pytest.raises(TransportError, match="has no scheme"):
        create_amqp("not-a-url")


def test_redis_create_transport_rejects_wrong_scheme() -> None:
    pytest.importorskip("redis")
    from cqrs.message_brokers.redis import create_transport as create_redis

    with pytest.raises(TransportError, match="scheme must be 'redis'"):
        create_redis("kafka://localhost")
    with pytest.raises(TransportError, match="has no scheme"):
        create_redis("not-a-url")


def test_nats_create_transport_rejects_wrong_scheme() -> None:
    pytest.importorskip("nats")
    from cqrs.message_brokers.nats import create_transport as create_nats

    with pytest.raises(TransportError, match="scheme must be 'nats'"):
        create_nats("kafka://localhost")
    with pytest.raises(TransportError, match="has no scheme"):
        create_nats("not-a-url")


def test_sqs_create_transport_rejects_wrong_scheme() -> None:
    pytest.importorskip("aioboto3")
    from cqrs.message_brokers.sqs import create_transport as create_sqs

    with pytest.raises(TransportError, match="scheme must be 'sqs'"):
        create_sqs("kafka://localhost")
    with pytest.raises(TransportError, match="has no scheme"):
        create_sqs("not-a-url")
