"""Unit tests for EventEmitter returning follow-up events from handler.events."""

import asyncio
import typing

import pydantic

from cqrs.events import EventEmitter, EventHandler, EventMap
from cqrs.models.pydantic import PydanticDomainEvent as DomainEvent
from cqrs.models.event import IEvent


class _EventA(DomainEvent, frozen=True):
    id_: str = pydantic.Field(alias="id")
    model_config = pydantic.ConfigDict(populate_by_name=True)


class _EventB(DomainEvent, frozen=True):
    id_: str = pydantic.Field(alias="id")
    model_config = pydantic.ConfigDict(populate_by_name=True)


class _HandlerA(EventHandler[_EventA]):
    def __init__(self) -> None:
        self.processed: list[_EventA] = []
        self._follow_ups: list[IEvent] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        return tuple(self._follow_ups)

    async def handle(self, event: _EventA) -> None:
        self.processed.append(event)
        self._follow_ups.append(_EventB(id="b_from_" + event.id_))


class _HandlerB(EventHandler[_EventB]):
    def __init__(self) -> None:
        self.processed: list[_EventB] = []

    async def handle(self, event: _EventB) -> None:
        self.processed.append(event)


class _Container:
    def __init__(self, handler_a: _HandlerA, handler_b: _HandlerB) -> None:
        self._handler_a = handler_a
        self._handler_b = handler_b

    async def resolve(self, type_: type) -> EventHandler[IEvent]:
        if type_ is _HandlerA:
            return self._handler_a  # type: ignore[return-value]
        if type_ is _HandlerB:
            return self._handler_b  # type: ignore[return-value]
        raise KeyError(type_)


async def test_event_emitter_emit_returns_follow_ups_from_handlers() -> None:
    """Arrange: domain event, handler that returns follow-ups. Act: emit. Assert: returns follow-ups."""
    handler_a = _HandlerA()
    handler_b = _HandlerB()
    event_map = EventMap()
    event_map.bind(_EventA, _HandlerA)
    event_map.bind(_EventB, _HandlerB)
    container = _Container(handler_a, handler_b)

    emitter = EventEmitter(
        event_map=event_map,
        container=container,  # type: ignore[arg-type]
    )

    event_a = _EventA(id="1")
    follow_ups = await emitter.emit(event_a)

    assert len(follow_ups) == 1
    assert isinstance(follow_ups[0], _EventB)
    assert follow_ups[0].id_ == "b_from_1"  # type: ignore[attr-defined]
    assert len(handler_a.processed) == 1
    assert handler_a.processed[0] == event_a


async def test_event_emitter_emit_multiple_handlers_concatenates_follow_ups() -> None:
    """Arrange: one event type, two handlers each returning follow-ups. Act: emit. Assert: all follow-ups returned."""

    class _EventX(DomainEvent, frozen=True):
        x: str = pydantic.Field()

    class _HandlerX1(EventHandler[_EventX]):
        def __init__(self) -> None:
            self._out: list[IEvent] = []

        @property
        def events(self) -> typing.Sequence[IEvent]:
            return tuple(self._out)

        async def handle(self, event: _EventX) -> None:
            self._out.append(_EventX(x=event.x + "_1"))

    class _HandlerX2(EventHandler[_EventX]):
        def __init__(self) -> None:
            self._out: list[IEvent] = []

        @property
        def events(self) -> typing.Sequence[IEvent]:
            return tuple(self._out)

        async def handle(self, event: _EventX) -> None:
            self._out.append(_EventX(x=event.x + "_2"))

    h1 = _HandlerX1()
    h2 = _HandlerX2()
    event_map = EventMap()
    event_map.bind(_EventX, _HandlerX1)
    event_map.bind(_EventX, _HandlerX2)

    class C:
        async def resolve(self, type_: type) -> EventHandler[IEvent]:
            if type_ is _HandlerX1:
                return h1  # type: ignore[return-value]
            return h2  # type: ignore[return-value]

    emitter = EventEmitter(event_map=event_map, container=C())  # type: ignore[arg-type]
    follow_ups = await emitter.emit(_EventX(x="a"))
    assert len(follow_ups) == 2
    xs = [e.x for e in follow_ups]  # type: ignore[attr-defined]
    assert "a_1" in xs and "a_2" in xs


async def test_event_emitter_executes_handlers_in_parallel() -> None:
    """Arrange: one event with two handlers that wait on each other. Act: emit in parallel. Assert: completes without deadlock."""

    class _ParallelEvent(DomainEvent, frozen=True):
        id: str

    handler_1_started = asyncio.Event()
    handler_2_started = asyncio.Event()
    handler_1_completed = asyncio.Event()
    handler_2_completed = asyncio.Event()

    class _ParallelHandler1(EventHandler[_ParallelEvent]):
        async def handle(self, event: _ParallelEvent) -> None:
            handler_1_started.set()
            await asyncio.wait_for(handler_2_started.wait(), timeout=1.0)
            handler_1_completed.set()

    class _ParallelHandler2(EventHandler[_ParallelEvent]):
        async def handle(self, event: _ParallelEvent) -> None:
            handler_2_started.set()
            await asyncio.wait_for(handler_1_started.wait(), timeout=1.0)
            handler_2_completed.set()

    h1 = _ParallelHandler1()
    h2 = _ParallelHandler2()

    event_map = EventMap()
    event_map.bind(_ParallelEvent, _ParallelHandler1)
    event_map.bind(_ParallelEvent, _ParallelHandler2)

    class C:
        async def resolve(self, type_: type) -> EventHandler[IEvent]:
            if type_ is _ParallelHandler1:
                return h1  # type: ignore[return-value]
            return h2  # type: ignore[return-value]

    emitter = EventEmitter(
        event_map=event_map,
        container=C(),  # type: ignore[arg-type]
    )

    # If executed sequentially, this call will deadlock and raise asyncio.TimeoutError
    await emitter.emit(_ParallelEvent(id="p1"))

    assert handler_1_started.is_set()
    assert handler_2_started.is_set()
    assert handler_1_completed.is_set()
    assert handler_2_completed.is_set()


async def test_event_emitter_continues_execution_when_handler_raises_exception() -> None:
    """Arrange: two handlers where handler 1 fails. Act: emit. Assert: handler 2 completes without flow crashing."""

    class _SampleEvent(DomainEvent, frozen=True):
        id: str

    handler_1_executed = asyncio.Event()
    handler_2_executed = asyncio.Event()

    class _FailingHandler(EventHandler[_SampleEvent]):
        async def handle(self, event: _SampleEvent) -> None:
            handler_1_executed.set()
            raise RuntimeError("Database connection lost")

    class _HealthyHandler(EventHandler[_SampleEvent]):
        def __init__(self) -> None:
            self._out: list[IEvent] = []

        @property
        def events(self) -> typing.Sequence[IEvent]:
            return tuple(self._out)

        async def handle(self, event: _SampleEvent) -> None:
            # Brief pause to ensure concurrency while handler 1 fails
            await asyncio.sleep(0.01)
            handler_2_executed.set()
            self._out.append(_EventB(id="success_" + event.id))

    failing_h = _FailingHandler()
    healthy_h = _HealthyHandler()

    event_map = EventMap()
    event_map.bind(_SampleEvent, _FailingHandler)
    event_map.bind(_SampleEvent, _HealthyHandler)

    class Container:
        async def resolve(self, type_: type) -> EventHandler[IEvent]:
            if type_ is _FailingHandler:
                return failing_h  # type: ignore[return-value]
            return healthy_h  # type: ignore[return-value]

    emitter = EventEmitter(
        event_map=event_map,
        container=Container(),  # type: ignore[arg-type]
    )
    follow_ups = await emitter.emit(_SampleEvent(id="123"))

    assert handler_1_executed.is_set()
    assert handler_2_executed.is_set()
    assert len(follow_ups) == 1
    assert isinstance(follow_ups[0], _EventB)
    assert follow_ups[0].id_ == "success_123"  # type: ignore[attr-defined]
