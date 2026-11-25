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
import asyncio
import dataclasses
import types
from collections.abc import Awaitable, Callable, Mapping
from typing import ClassVar, Literal

from cqrs.message_brokers.protocol import MessageBroker
from cqrs.transport.errors import UnsupportedCapability


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

    ``group`` is required. Each ``subscribe`` call is one consumer in that
    group. A second process with the same group competes. A different group
    receives its own copy. This process does not run a pool of handlers on
    one consumer.

    ``prefetch`` is the broker window: Kafka ``max_poll_records``, AMQP QoS,
    NATS ``max_ack_pending``, SQS ``MaxNumberOfMessages``, Redis ``COUNT``.

    ``max_deliveries`` is passed through when the broker already counts
    deliveries (NATS ``max_deliver``, SQS receive count, Redis ``XPENDING``).
    It is not a counter this process keeps.

    ``dead_letter_topic`` is the name used when this process publishes a dead
    letter itself. ``None`` means ``f"{topic}.dlq"``. Brokers with their own
    dead letter (AMQP DLX, SQS redrive, NATS ``term``) do not run a second
    state machine.

    ``start_from`` is ``latest`` unless the caller asks for the earlier log.
    """

    group: str
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
        """Broker delivery count when it has one. Otherwise 1."""
        raise NotImplementedError

    @abc.abstractmethod
    async def ack(self) -> None:
        """Finish this delivery. A repeat call does nothing."""
        raise NotImplementedError

    @abc.abstractmethod
    async def nack(self, *, requeue: bool = True) -> None:
        """
        Reject this delivery.

        ``requeue=True`` asks the broker to deliver again. ``requeue=False``
        dead-letters it with the broker, or publishes the bytes once and
        acknowledges where the broker has no dead letter. A repeat call
        does nothing.
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
        Wait until handler tasks in this process have finished.

        The default returns immediately. A backend that queues overrides this
        and raises ``TimeoutError`` when the wait exceeds ``timeout``.
        The wait does not put a correlation id on the message.
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


def require_options(options: SubscribeOptions) -> None:
    """Reject a subscription the transports do not implement."""

    if not options.group:
        raise UnsupportedCapability("group is required")
    if options.start_from not in ("latest", "earliest"):
        raise UnsupportedCapability(f"start_from={options.start_from!r} is not supported")
    if options.prefetch < 1 or options.max_deliveries < 1:
        raise UnsupportedCapability("prefetch and max_deliveries must be >= 1")


class HandlerWatch:
    """
    Publishes from this process whose handlers have not finished.

    The key is an id the broker already assigned (partition offset, stream
    sequence, AMQP ``message_id``, SQS ``MessageId``) or, in memory, an id
    on the queued record. Nothing is added to the message. An ack for any
    other id does not finish this publish.
    """

    def __init__(self) -> None:
        self._pending: dict[str, int] = {}
        self._early: dict[str, int] = {}
        self.inflight = 0
        self.idle = asyncio.Event()
        self.idle.set()

    @property
    def owed(self) -> int:
        return sum(self._pending.values())

    def expect(self, message_id: str, count: int = 1) -> None:
        if count < 1 or not message_id:
            return
        self._pending[message_id] = self._pending.get(message_id, 0) + count
        self.idle.clear()

    def reserve(self, token: str, count: int) -> None:
        """Count a publish before its broker id is known. ``bind`` attaches the id."""

        self.expect(f"~{token}", count)

    def bind(self, token: str, message_id: str) -> None:
        staged = self._pending.pop(f"~{token}", 0)
        early = self._early.pop(message_id, 0) if message_id else 0
        left = staged - min(staged, early)
        if left > 0 and message_id:
            self._pending[message_id] = self._pending.get(message_id, 0) + left
        if not any(key.startswith("~") for key in self._pending):
            self._early.clear()
        self.poke()

    def abandon(self, message_id: str, count: int = 1) -> None:
        self._release(message_id, count)

    def abandon_all(self) -> None:
        self._pending.clear()
        self._early.clear()
        self.poke()

    def begin(self) -> None:
        self.inflight += 1
        self.idle.clear()

    def end(self) -> None:
        self.inflight = max(0, self.inflight - 1)
        self.poke()

    def settle(self, message_id: str) -> None:
        if self._pending.get(message_id, 0) > 0:
            self._release(message_id, 1)
            return
        if message_id and any(key.startswith("~") for key in self._pending):
            self._early[message_id] = self._early.get(message_id, 0) + 1

    def _release(self, message_id: str, count: int) -> None:
        current = self._pending.get(message_id, 0)
        if current < 1 or count < 1:
            return
        left = current - count
        if left > 0:
            self._pending[message_id] = left
        else:
            del self._pending[message_id]
        self.poke()

    def ready(self) -> bool:
        return not self._pending and self.inflight == 0

    def poke(self) -> None:
        if self.ready():
            self.idle.set()

    async def wait(self, timeout: float, describe: Callable[[], str]) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not self.ready():
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(describe())
            self.idle.clear()
            if self.ready():
                return
            try:
                await asyncio.wait_for(self.idle.wait(), remaining)
            except asyncio.TimeoutError:
                if self.ready():
                    return
                raise TimeoutError(describe()) from None


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
