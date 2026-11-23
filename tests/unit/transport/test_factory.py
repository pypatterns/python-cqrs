"""create_transport, registration, and lazy broker imports."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from cqrs.transport import (
    InMemoryTransport,
    TransportError,
    UnsupportedCapability,
    create_transport,
    register_transport,
)
from cqrs.transport import factory as transport_factory


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
assert cqrs.create_transport is not None
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


def test_importing_factory_does_not_load_broker_clients() -> None:
    script = """
import sys
import cqrs.transport.factory
banned = ("aio_pika", "aiokafka", "redis", "nats", "aioboto3")
loaded = [name for name in banned if name in sys.modules]
if loaded:
    raise SystemExit("loaded " + ",".join(loaded))
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


def test_unknown_scheme_lists_known_schemes() -> None:
    with pytest.raises(TransportError, match="Unknown transport scheme 'mqtt'") as caught:
        create_transport("mqtt://localhost")
    message = str(caught.value)
    for scheme in ("memory", "amqp", "kafka", "redis", "nats", "sqs"):
        assert scheme in message


def test_future_scheme_reports_the_missing_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(
        transport_factory._FUTURE_SCHEMES,
        "futureextra",
        ("cqrs_missing_transport_futureextra", "FutureExtraTransport", "futureextra"),
    )
    with pytest.raises(
        TransportError,
        match=r'Transport \'futureextra\' requires: pip install "python-cqrs\[futureextra\]"',
    ):
        create_transport("futureextra://localhost")


def test_missing_extra_uses_the_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(
        transport_factory._FUTURE_SCHEMES,
        "demo",
        ("cqrs_missing_transport_demo", "DemoTransport", "demo"),
    )
    with pytest.raises(
        TransportError,
        match=r'Transport \'demo\' requires: pip install "python-cqrs\[demo\]"',
    ):
        create_transport("demo://bus")


def test_invalid_memory_delivery() -> None:
    with pytest.raises(UnsupportedCapability):
        create_transport("memory://?delivery=fast")


async def test_register_transport_replaces_the_previous_factory() -> None:
    created: list[InMemoryTransport] = []

    def first(parsed: object) -> InMemoryTransport:
        transport = InMemoryTransport(delivery="queued")
        created.append(transport)
        return transport

    def second(parsed: object) -> InMemoryTransport:
        transport = InMemoryTransport(delivery="inline")
        created.append(transport)
        return transport

    register_transport("localmem", first)
    try:
        first_transport = create_transport("localmem://")
        register_transport("localmem", second)
        second_transport = create_transport("localmem://again")
        assert isinstance(first_transport, InMemoryTransport)
        assert isinstance(second_transport, InMemoryTransport)
        assert first_transport.delivery == "queued"
        assert second_transport.delivery == "inline"
    finally:
        transport_factory._FACTORIES.pop("localmem", None)
        for transport in created:
            await transport.close()
