"""
Publish and subscribe transport.

[AI Instruction]
Import the ports and ``InMemoryTransport`` from here or from ``cqrs``.
``create_transport`` for a backend lives in that backend's module
(``cqrs.transport.memory`` or ``cqrs.message_brokers.<name>``). ``KafkaTransport``,
``AMQPTransport``, ``RedisTransport``, ``NATSTransport``, and
``SQSTransport`` are lazy attributes of this package. They are not
attributes of ``cqrs`` and they are not in ``cqrs.__all__``. Importing
``cqrs`` or this package does not import ``aio_pika``, ``aiokafka``,
``redis``, ``nats``, or ``aioboto3``. Publish with
``MessageBroker.send_message`` only.
"""

from __future__ import annotations

import typing

from cqrs.transport.errors import TransportClosed, TransportError, UnsupportedCapability
from cqrs.transport.memory import InMemoryTransport
from cqrs.transport.protocol import (
    IncomingHandler,
    IncomingMessage,
    MessageSubscriber,
    SubscribeOptions,
    Subscription,
    Transport,
    TransportCapabilities,
)

__all__ = (
    "InMemoryTransport",
    "IncomingHandler",
    "IncomingMessage",
    "MessageSubscriber",
    "SubscribeOptions",
    "Subscription",
    "Transport",
    "TransportCapabilities",
    "TransportClosed",
    "TransportError",
    "UnsupportedCapability",
)

# Imported on attribute access. Importing this package does not load a broker client.
_LAZY_TRANSPORTS: dict[str, tuple[str, str]] = {
    "KafkaTransport": ("cqrs.message_brokers.kafka", "KafkaTransport"),
    "AMQPTransport": ("cqrs.message_brokers.amqp", "AMQPTransport"),
    "RedisTransport": ("cqrs.message_brokers.redis", "RedisTransport"),
    "NATSTransport": ("cqrs.message_brokers.nats", "NATSTransport"),
    "SQSTransport": ("cqrs.message_brokers.sqs", "SQSTransport"),
}


def __getattr__(name: str) -> typing.Any:
    if name in _LAZY_TRANSPORTS:
        module_name, attr = _LAZY_TRANSPORTS[name]
        module = __import__(module_name, fromlist=[attr])
        return getattr(module, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
