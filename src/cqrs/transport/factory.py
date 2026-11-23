"""
Build a transport from a URL.

[AI Instruction]
``create_transport("memory://")`` is the in-memory backend. ``register_transport``
adds a scheme for a backend this process supplies. Importing this module does
not import ``aio_pika``, ``aiokafka``, ``redis``, ``nats``, or ``aioboto3``.
``amqp``, ``kafka``, ``redis``, ``nats``, and ``sqs`` are declared here and
imported only when that URL is created.
"""

from __future__ import annotations

import importlib
import urllib.parse
from collections.abc import Callable

from cqrs.transport.errors import TransportError, UnsupportedCapability
from cqrs.transport.memory import InMemoryTransport
from cqrs.transport.protocol import Transport

_Factory = Callable[[urllib.parse.ParseResult], Transport]

# User registrations win over the built-in table, including a second call.
_FACTORIES: dict[str, _Factory] = {}

# (module, attribute, extra). The attribute is called with the parsed URL.
# The modules are not imported until create_transport sees that scheme.
_FUTURE_SCHEMES: dict[str, tuple[str, str, str]] = {
    "amqp": ("cqrs.message_brokers.amqp", "AMQPTransport", "rabbit"),
    "kafka": ("cqrs.message_brokers.kafka", "KafkaTransport", "kafka"),
    "redis": ("cqrs.message_brokers.redis", "RedisTransport", "redis"),
    "nats": ("cqrs.message_brokers.nats", "NATSTransport", "nats"),
    "sqs": ("cqrs.message_brokers.sqs", "SQSTransport", "sqs"),
}


def register_transport(scheme: str, factory: _Factory) -> None:
    """
    Register ``scheme`` so ``create_transport`` can build it.

    A second registration of the same scheme replaces the previous factory.
    """

    if not scheme or any(character in scheme for character in ":/"):
        raise TransportError(f"Invalid transport scheme {scheme!r}")
    # urlparse lowercases the scheme, so the registry key has to match.
    _FACTORIES[scheme.lower()] = factory


def create_transport(url: str) -> Transport:
    """
    Build the transport described by ``url``.

    ``memory://`` and ``memory://?delivery=inline&redelivery_delay=0`` create
    an ``InMemoryTransport``. An unknown scheme raises ``TransportError`` and
    lists the schemes this process knows. A declared backend whose module or
    class is missing raises ``TransportError`` naming the extra to install.
    """

    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme
    if not scheme:
        raise TransportError(
            f"Transport URL {url!r} has no scheme. Known schemes: {_known_schemes()}",
        )
    factory = _FACTORIES.get(scheme)
    if factory is not None:
        return factory(parsed)
    if scheme in _FUTURE_SCHEMES:
        return _load_future(scheme, parsed)
    raise TransportError(
        f"Unknown transport scheme {scheme!r}. Known schemes: {_known_schemes()}",
    )


def _known_schemes() -> str:
    names = set(_FACTORIES) | set(_FUTURE_SCHEMES)
    return ", ".join(sorted(names))


def _load_future(scheme: str, parsed: urllib.parse.ParseResult) -> Transport:
    module_name, attr, extra = _FUTURE_SCHEMES[scheme]
    try:
        module = importlib.import_module(module_name)
        factory = getattr(module, attr)
    except (ImportError, AttributeError) as exc:
        raise TransportError(
            f'Transport {scheme!r} requires: pip install "python-cqrs[{extra}]"',
        ) from exc
    return factory(parsed)


def _memory_from_url(parsed: urllib.parse.ParseResult) -> InMemoryTransport:
    query = urllib.parse.parse_qs(parsed.query)
    delivery = query.get("delivery", ["queued"])[0]
    if delivery not in ("queued", "inline"):
        raise UnsupportedCapability(
            f"InMemoryTransport does not support delivery={delivery!r}",
        )
    raw_delay = query.get("redelivery_delay", ["0"])[0]
    try:
        delay = float(raw_delay)
    except ValueError as exc:
        raise TransportError(f"Invalid redelivery_delay {raw_delay!r}") from exc
    if delivery == "inline":
        return InMemoryTransport(delivery="inline", redelivery_delay=delay)
    return InMemoryTransport(delivery="queued", redelivery_delay=delay)


register_transport("memory", _memory_from_url)


__all__ = (
    "create_transport",
    "register_transport",
)
