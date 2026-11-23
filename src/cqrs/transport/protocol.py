"""
Publish and subscribe ports.

[AI Instruction]
A ``Transport`` is a ``MessageBroker``. Publish with ``send_message`` only.
Subscribe with ``subscribe``. Do not add a ``publish`` alias. Do not define
``__bool__`` or ``__len__``: ``EventEmitter`` treats a missing broker as
``if not self._message_broker``.
"""

from __future__ import annotations

import abc
import dataclasses
import types
from collections.abc import Awaitable, Callable, Mapping
from typing import ClassVar, Literal

from cqrs.message_brokers.protocol import MessageBroker


StartFrom = Literal["latest", "earliest"]


@dataclasses.dataclass(frozen=True)
class TransportCapabilities:
    """
    What one backend can promise.

    ``topic`` is the meaning of the name passed to ``send_message`` and
    ``subscribe`` for that backend. The library does not prefix or rewrite it.
    """

    ordering_per_key: bool
    native_dlq: bool
    delayed_redelivery: bool
    headers: bool
    topic: str


@dataclasses.dataclass(frozen=True)
class SubscribeOptions:
    """
    How one subscription consumes a topic.

    ``group`` is required. A second subscription with the same topic and
    group competes for each message. A different group receives its own copy.
    ``dead_letter_topic=None`` means ``f"{topic}.dlq"``. ``start_from`` is
    ``latest`` unless the caller asks for the earlier log.
    """

    group: str
    concurrency: int = 1
    prefetch: int = 1
    max_deliveries: int = 5
    dead_letter_topic: str | None = None
    start_from: StartFrom = "latest"


class IncomingMessage(abc.ABC):
    """
    One delivery handed to a subscriber.

    [AI Instruction]
    This is not a dataclass. Ack or nack it once. A second ack or nack does
    nothing. The body is bytes, not a ``Message`` and not a decoded event.
    The application deserializes ``body``.
    """

    @property
    @abc.abstractmethod
    def topic(self) -> str:
        """Topic the message was published to."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def body(self) -> bytes:
        """Wire bytes. Equal to ``message_wire_bytes`` of the published message."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def headers(self) -> Mapping[str, str]:
        """Copy of the broker headers. Empty when the message had none."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def content_type(self) -> str | None:
        """MIME type from the published message, or ``None``."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def key(self) -> str | None:
        """Ordering key, or ``None`` when the publisher did not set one."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def delivery_attempt(self) -> int:
        """1 on the first delivery. A requeue increases it. The dead-letter copy keeps the last attempt."""
        raise NotImplementedError

    @abc.abstractmethod
    async def ack(self) -> None:
        """Finish this delivery. A repeat call does nothing."""
        raise NotImplementedError

    @abc.abstractmethod
    async def nack(self, *, requeue: bool = True) -> None:
        """
        Reject this delivery.

        ``requeue=True`` tries again until ``max_deliveries``. ``requeue=False``
        dead-letters it now. A repeat call does nothing.
        """
        raise NotImplementedError


class Subscription(abc.ABC):
    """
    One ``subscribe`` call.

    [AI Instruction]
    ``close`` stops this subscription from taking new messages and waits for
    handlers it already started. ``Transport.close`` is the full stop.
    """

    @abc.abstractmethod
    async def close(self) -> None:
        """Cancel this subscription and wait for its current handlers."""
        raise NotImplementedError


IncomingHandler = Callable[[IncomingMessage], Awaitable[None]]


class MessageSubscriber(abc.ABC):
    """
    Consume side of a transport.

    [AI Instruction]
    Application code owns the handler. The library does not map a topic to a
    command or an event. ``options.group`` is required.
    """

    @abc.abstractmethod
    async def subscribe(
        self,
        topic: str,
        handler: IncomingHandler,
        options: SubscribeOptions,
    ) -> Subscription:
        """Register ``handler`` for ``topic`` and return a subscription."""
        raise NotImplementedError


class Transport(MessageBroker, MessageSubscriber):
    """
    A ``MessageBroker`` that can also subscribe.

    [AI Instruction]
    Pass it to ``bootstrap`` or ``test_harness`` as ``message_broker``.
    Publish with ``send_message``. There is no ``publish`` method. Concrete
    broker classes stay lazy on ``cqrs.transport`` and are not attributes of
    ``cqrs``.
    """

    capabilities: ClassVar[TransportCapabilities]

    async def start(self) -> None:
        """Open connections. The in-memory transport has nothing to open."""

    async def drain(self, timeout: float = 5) -> None:
        """
        Wait until queued and in-flight work has settled.

        The default returns immediately. A backend that queues overrides this
        and raises ``TimeoutError`` when the wait exceeds ``timeout``.
        """

    async def close(self) -> None:
        """Stop accepting work. Override to cancel workers."""

    async def __aenter__(self) -> Transport:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        await self.close()


__all__ = (
    "IncomingHandler",
    "IncomingMessage",
    "MessageSubscriber",
    "StartFrom",
    "SubscribeOptions",
    "Subscription",
    "Transport",
    "TransportCapabilities",
)
