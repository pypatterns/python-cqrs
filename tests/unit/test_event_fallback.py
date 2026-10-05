"""Tests for EventHandlerFallback (without circuit breaker)."""

from collections.abc import Sequence
from typing import Any, TypeVar

import pytest

from cqrs import EventHandlerFallback
from cqrs.container.protocol import Container
from cqrs.models.event import DomainEvent, IEvent
from cqrs.events.event_emitter import EventEmitter
from cqrs.handlers.event import EventHandler
from cqrs.mapping.events import EventMap

T = TypeVar("T")


class SampleEvent(DomainEvent, frozen=True):
    """Event type for fallback tests (name avoids pytest collecting it as a test class)."""

    id: str


class PrimaryEventHandler(EventHandler[SampleEvent]):
    def __init__(self) -> None:
        self._evs: list[IEvent] = []
        self.called = False

    @property
    def events(self) -> Sequence[IEvent]:
        return self._evs.copy()

    async def handle(self, event: SampleEvent) -> None:
        self.called = True
        raise RuntimeError("Primary failed")


class FallbackEventHandler(EventHandler[SampleEvent]):
    def __init__(self) -> None:
        self._evs: list[IEvent] = []
        self.called = False

    @property
    def events(self) -> Sequence[IEvent]:
        return self._evs.copy()

    async def handle(self, event: SampleEvent) -> None:
        self.called = True


class _TestEventContainer:
    """Minimal container for event fallback tests; implements Container protocol."""

    def __init__(self) -> None:
        self._primary = PrimaryEventHandler()
        self._fallback = FallbackEventHandler()
        self._external_container: Any = None

    @property
    def external_container(self) -> Any:
        return self._external_container

    def attach_external_container(self, container: Any) -> None:
        self._external_container = container

    async def resolve(self, type_: type[T]) -> T:
        if type_ is PrimaryEventHandler:
            return self._primary  # type: ignore[return-value]
        if type_ is FallbackEventHandler:
            return self._fallback  # type: ignore[return-value]
        raise KeyError(type_)


@pytest.mark.asyncio
async def test_event_fallback_no_cb_primary_fails_uses_fallback() -> None:
    event_map: EventMap = EventMap()
    event_map.bind(
        SampleEvent,
        EventHandlerFallback(PrimaryEventHandler, FallbackEventHandler),
    )
    container: Container[Any] = _TestEventContainer()
    emitter = EventEmitter(event_map=event_map, container=container)

    follow_ups = await emitter.emit(SampleEvent(id="e1"))

    assert container._primary.called
    assert container._fallback.called
    assert follow_ups == []


@pytest.mark.asyncio
async def test_event_fallback_failure_exceptions_only_matching_triggers_fallback() -> None:
    event_map = EventMap()
    event_map.bind(
        SampleEvent,
        EventHandlerFallback(
            PrimaryEventHandler,
            FallbackEventHandler,
            failure_exceptions=(ValueError,),
        ),
    )
    container: Container[Any] = _TestEventContainer()
    emitter = EventEmitter(event_map=event_map, container=container)

    await emitter.emit(SampleEvent(id="e1"))

    assert container._primary.called
    assert not container._fallback.called


@pytest.mark.asyncio
async def test_event_fallback_matching_filter_triggers_fallback() -> None:
    """When failure_exceptions matches the primary error, fallback is invoked."""
    event_map: EventMap = EventMap()
    event_map.bind(
        SampleEvent,
        EventHandlerFallback(
            PrimaryEventHandler,
            FallbackEventHandler,
            failure_exceptions=(RuntimeError,),
        ),
    )
    container: Container[Any] = _TestEventContainer()
    emitter = EventEmitter(event_map=event_map, container=container)

    follow_ups = await emitter.emit(SampleEvent(id="e1"))

    assert container._primary.called
    assert container._fallback.called
    assert follow_ups == []


# --- Validation tests ---


def test_event_fallback_validation_same_event_type_accepts() -> None:
    """Same event type is accepted."""
    EventHandlerFallback(PrimaryEventHandler, FallbackEventHandler)


def test_event_fallback_validation_different_event_type_raises() -> None:
    """Different event types raise TypeError."""
    from cqrs.models.event import DomainEvent

    class OtherEvent(DomainEvent, frozen=True):
        num: int

    class HandlerOther(EventHandler[OtherEvent]):
        async def handle(self, event: OtherEvent) -> None:
            pass

    with pytest.raises(TypeError, match="same event type"):
        EventHandlerFallback(PrimaryEventHandler, HandlerOther)


def test_event_fallback_validation_not_classes_raises() -> None:
    """Passing non-classes raises TypeError."""
    with pytest.raises(TypeError, match="must be handler classes"):
        EventHandlerFallback(PrimaryEventHandler, FallbackEventHandler())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be handler classes"):
        EventHandlerFallback(PrimaryEventHandler(), FallbackEventHandler)  # type: ignore[arg-type]


def test_event_fallback_validation_primary_not_event_handler_raises() -> None:
    """Primary that is not EventHandler subclass raises TypeError."""
    from cqrs.models.request import Request
    from cqrs.handlers.request import RequestHandler
    from cqrs.models.response import Response

    class NotAnEventHandler:
        pass

    class SomeHandler(RequestHandler[Request, Response]):
        async def handle(self, request: Request) -> Response:
            raise NotImplementedError

    with pytest.raises(TypeError, match="primary.*must be a subclass of EventHandler"):
        EventHandlerFallback(NotAnEventHandler, FallbackEventHandler)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="primary.*must be a subclass of EventHandler"):
        EventHandlerFallback(SomeHandler, FallbackEventHandler)  # pyright: ignore[reportArgumentType]
