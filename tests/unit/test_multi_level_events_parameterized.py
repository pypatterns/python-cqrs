"""Unit tests: multi-level event handling (follow-ups) for all handler/mediator types.

All tests are parameterized by parallel (True/False) where applicable.
Expected result is the same in both modes: all events must be processed.
"""

import asyncio
import typing

import di
import pydantic
import pytest

import cqrs
from cqrs.events import DomainEvent, EventEmitter, EventHandler, EventMap
from cqrs.events.event import IEvent
from cqrs.events.event_processor import EventProcessor
from cqrs.dispatcher.event import EventDispatcher
from cqrs.container.protocol import Container
from cqrs.requests import bootstrap


# ---- EventProcessor: 1 root -> 3 children (4 events total) ----


class _FanEvent(DomainEvent, frozen=True):
    id_: str = pydantic.Field(alias="id")
    model_config = pydantic.ConfigDict(populate_by_name=True)


class _FanHandler(EventHandler[_FanEvent]):
    def __init__(self) -> None:
        self._follow_ups: list[_FanEvent] = []

    @property
    def events(self) -> tuple[_FanEvent, ...]:
        return tuple(self._follow_ups)

    async def handle(self, event: _FanEvent) -> None:
        self._follow_ups = []
        if event.id_ == "root":
            self._follow_ups = [
                _FanEvent(id="c1"),
                _FanEvent(id="c2"),
                _FanEvent(id="c3"),
            ]
        if event.id_ != "root":
            self._follow_ups = []


class _FanContainer(Container[object]):
    def __init__(self, handler: _FanHandler) -> None:
        self._handler = handler
        self._external: typing.Optional[object] = None

    @property
    def external_container(self) -> object:
        return self._external  # type: ignore[return-value]

    def attach_external_container(self, container: object) -> None:
        self._external = container

    async def resolve(self, type_: type) -> EventHandler[IEvent]:
        if type_ is _FanHandler:
            return self._handler  # type: ignore[return-value]
        raise KeyError(type_)


@pytest.mark.parametrize("parallel", [True, False])
async def test_event_processor_multi_level_all_events_processed(parallel: bool) -> None:
    """EventProcessor: root -> 3 children. Parallel or sequential: all 4 events must be processed."""
    processed: list[_FanEvent] = []
    handler = _FanHandler()
    original_handle = handler.handle

    async def tracking_handle(event: _FanEvent) -> None:
        await original_handle(event)
        processed.append(event)

    handler.handle = tracking_handle  # type: ignore[method-assign]

    event_map = EventMap()
    event_map.bind(_FanEvent, _FanHandler)
    container = _FanContainer(handler)
    emitter = EventEmitter(event_map=event_map, container=container)
    processor = EventProcessor(
        event_map=event_map,
        event_emitter=emitter,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=parallel,
    )
    await processor.emit_events([_FanEvent(id="root")])
    if parallel:
        await asyncio.sleep(0.15)
    assert len(processed) == 4
    ids = {e.id_ for e in processed}
    assert ids == {"root", "c1", "c2", "c3"}


# ---- EventDispatcher: 3-level chain L1 -> L2 -> L3 (always sequential) ----


class _EventL1(DomainEvent, frozen=True):
    name: str = pydantic.Field()


class _EventL2(DomainEvent, frozen=True):
    name: str = pydantic.Field()


class _EventL3(DomainEvent, frozen=True):
    name: str = pydantic.Field()


class _HandlerL1(EventHandler[_EventL1]):
    def __init__(self) -> None:
        self.processed: list[_EventL1] = []
        self._follow_ups: list[IEvent] = []

    @property
    def events(self) -> tuple[IEvent, ...]:
        return tuple(self._follow_ups)

    async def handle(self, event: _EventL1) -> None:
        self.processed.append(event)
        self._follow_ups = [_EventL2(name="L2_from_" + event.name)]


class _HandlerL2(EventHandler[_EventL2]):
    def __init__(self) -> None:
        self.processed: list[_EventL2] = []
        self._follow_ups: list[IEvent] = []

    @property
    def events(self) -> tuple[IEvent, ...]:
        return tuple(self._follow_ups)

    async def handle(self, event: _EventL2) -> None:
        self.processed.append(event)
        self._follow_ups = [_EventL3(name="L3_from_" + event.name)]


class _HandlerL3(EventHandler[_EventL3]):
    def __init__(self) -> None:
        self.processed: list[_EventL3] = []

    async def handle(self, event: _EventL3) -> None:
        self.processed.append(event)


class _DispatcherContainer(Container[object]):
    def __init__(self) -> None:
        self._h1 = _HandlerL1()
        self._h2 = _HandlerL2()
        self._h3 = _HandlerL3()
        self._external: typing.Optional[object] = None

    @property
    def external_container(self) -> object:
        return self._external  # type: ignore[return-value]

    def attach_external_container(self, container: object) -> None:
        self._external = container

    async def resolve(self, type_: type) -> EventHandler[IEvent]:
        if type_ is _HandlerL1:
            return self._h1  # type: ignore[return-value]
        if type_ is _HandlerL2:
            return self._h2  # type: ignore[return-value]
        if type_ is _HandlerL3:
            return self._h3  # type: ignore[return-value]
        raise KeyError(type_)


@pytest.mark.parametrize("parallel", [False])
async def test_event_dispatcher_multi_level_all_events_processed(parallel: bool) -> None:
    """EventDispatcher: L1 -> L2 -> L3. Dispatcher is always sequential; all 3 levels must be processed."""
    event_map = EventMap()
    event_map.bind(_EventL1, _HandlerL1)
    event_map.bind(_EventL2, _HandlerL2)
    event_map.bind(_EventL3, _HandlerL3)
    container = _DispatcherContainer()
    dispatcher = EventDispatcher(event_map=event_map, container=container)
    await dispatcher.dispatch(_EventL1(name="a"))
    assert len(container._h1.processed) == 1
    assert container._h1.processed[0].name == "a"
    assert len(container._h2.processed) == 1
    assert container._h2.processed[0].name == "L2_from_a"
    assert len(container._h3.processed) == 1
    assert container._h3.processed[0].name == "L3_from_L2_from_a"


# ---- RequestMediator (bootstrap): 3-level chain L1 -> L2 -> L3 ----

_MEDIATOR_PROCESSED_L1: list[cqrs.DomainEvent] = []
_MEDIATOR_PROCESSED_L2: list[cqrs.DomainEvent] = []
_MEDIATOR_PROCESSED_L3: list[cqrs.DomainEvent] = []


class _EmitL1Command(cqrs.Request):
    seed: str = pydantic.Field()


class _MediatorEventL1(cqrs.DomainEvent, frozen=True):
    level: int = 1
    seed: str = pydantic.Field()


class _MediatorEventL2(cqrs.DomainEvent, frozen=True):
    level: int = 2
    seed: str = pydantic.Field()


class _MediatorEventL3(cqrs.DomainEvent, frozen=True):
    level: int = 3
    seed: str = pydantic.Field()


class _EmitL1CommandHandler(cqrs.RequestHandler[_EmitL1Command, None]):
    def __init__(self) -> None:
        self._events: list[IEvent] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        return tuple(self._events)

    async def handle(self, request: _EmitL1Command) -> None:
        self._events.append(_MediatorEventL1(seed=request.seed))


class _MediatorHandlerL1(cqrs.EventHandler[_MediatorEventL1]):
    def __init__(self) -> None:
        self._follow_ups: list[IEvent] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        return tuple(self._follow_ups)

    async def handle(self, event: _MediatorEventL1) -> None:
        _MEDIATOR_PROCESSED_L1.append(event)
        self._follow_ups.append(_MediatorEventL2(seed=event.seed))


class _MediatorHandlerL2(cqrs.EventHandler[_MediatorEventL2]):
    def __init__(self) -> None:
        self._follow_ups: list[IEvent] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        return tuple(self._follow_ups)

    async def handle(self, event: _MediatorEventL2) -> None:
        _MEDIATOR_PROCESSED_L2.append(event)
        self._follow_ups.append(_MediatorEventL3(seed=event.seed))


class _MediatorHandlerL3(cqrs.EventHandler[_MediatorEventL3]):
    async def handle(self, event: _MediatorEventL3) -> None:
        _MEDIATOR_PROCESSED_L3.append(event)


def _commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(_EmitL1Command, _EmitL1CommandHandler)


def _events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(_MediatorEventL1, _MediatorHandlerL1)
    mapper.bind(_MediatorEventL2, _MediatorHandlerL2)
    mapper.bind(_MediatorEventL3, _MediatorHandlerL3)


@pytest.mark.parametrize("parallel", [True, False])
async def test_request_mediator_multi_level_all_events_processed(parallel: bool) -> None:
    """RequestMediator: command emits L1 -> L2 -> L3. Parallel or sequential: all 3 levels must be processed."""
    _MEDIATOR_PROCESSED_L1.clear()
    _MEDIATOR_PROCESSED_L2.clear()
    _MEDIATOR_PROCESSED_L3.clear()

    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=_commands_mapper,
        domain_events_mapper=_events_mapper,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=parallel,
    )

    await mediator.send(_EmitL1Command(seed="x"))
    if parallel:
        await asyncio.sleep(0.15)

    assert len(_MEDIATOR_PROCESSED_L1) == 1
    assert _MEDIATOR_PROCESSED_L1[0].seed == "x"  # type: ignore[attr-defined]
    assert len(_MEDIATOR_PROCESSED_L2) == 1
    assert _MEDIATOR_PROCESSED_L2[0].seed == "x"  # type: ignore[attr-defined]
    assert len(_MEDIATOR_PROCESSED_L3) == 1
    assert _MEDIATOR_PROCESSED_L3[0].seed == "x"  # type: ignore[attr-defined]
