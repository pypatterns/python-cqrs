"""Benchmarks: 3-level event chain, volume >> semaphore (parallel follow-ups). Dataclass events."""

import asyncio
import dataclasses
import typing

import pytest

from cqrs.events import DCDomainEvent, EventEmitter, EventHandler, EventMap
from cqrs.events.event import IEvent
from cqrs.events.event_processor import EventProcessor
from cqrs.container.protocol import Container


@dataclasses.dataclass(frozen=True)
class _EventL1(DCDomainEvent):
    id_: str


@dataclasses.dataclass(frozen=True)
class _EventL2(DCDomainEvent):
    id_: str


@dataclasses.dataclass(frozen=True)
class _EventL3(DCDomainEvent):
    id_: str


# Number of follow-ups per level so total events >> semaphore
FAN_OUT_L1 = 10
FAN_OUT_L2 = 5
SEMAPHORE_SIZE = 4


class _HandlerL1(EventHandler[_EventL1]):
    def __init__(self) -> None:
        self._follow_ups: list[IEvent] = []

    @property
    def events(self) -> tuple[IEvent, ...]:
        return tuple(self._follow_ups)

    async def handle(self, event: _EventL1) -> None:
        self._follow_ups = [_EventL2(id_=f"l2_{event.id_}_{i}") for i in range(FAN_OUT_L1)]


class _HandlerL2(EventHandler[_EventL2]):
    def __init__(self) -> None:
        self._follow_ups: list[IEvent] = []

    @property
    def events(self) -> tuple[IEvent, ...]:
        return tuple(self._follow_ups)

    async def handle(self, event: _EventL2) -> None:
        self._follow_ups = [_EventL3(id_=f"l3_{event.id_}_{i}") for i in range(FAN_OUT_L2)]


class _HandlerL3(EventHandler[_EventL3]):
    async def handle(self, event: _EventL3) -> None:
        pass


class _ChainContainer(Container[object]):
    def __init__(self) -> None:
        self._external: typing.Optional[object] = None

    @property
    def external_container(self) -> object:
        return self._external  # type: ignore[return-value]

    def attach_external_container(self, container: object) -> None:
        self._external = container

    async def resolve(self, type_: type) -> EventHandler[IEvent]:
        # Return fresh instances per resolve so stateful handlers (_HandlerL1/_HandlerL2
        # _follow_ups) are not shared across concurrent event handling (parallel benchmarks).
        if type_ is _HandlerL1:
            return _HandlerL1()  # type: ignore[return-value]
        if type_ is _HandlerL2:
            return _HandlerL2()  # type: ignore[return-value]
        if type_ is _HandlerL3:
            return _HandlerL3()  # type: ignore[return-value]
        raise KeyError(type_)


def _make_processor(parallel: bool) -> EventProcessor:
    event_map = EventMap()
    event_map.bind(_EventL1, _HandlerL1)
    event_map.bind(_EventL2, _HandlerL2)
    event_map.bind(_EventL3, _HandlerL3)
    container = _ChainContainer()
    emitter = EventEmitter(event_map=event_map, container=container)
    return EventProcessor(
        event_map=event_map,
        event_emitter=emitter,
        max_concurrent_event_handlers=SEMAPHORE_SIZE,
        concurrent_event_handle_enable=parallel,
    )


@pytest.fixture
def event_processor_chain_parallel() -> EventProcessor:
    """EventProcessor with 3-level chain, parallel, semaphore=4 (dataclass events)."""
    return _make_processor(parallel=True)


@pytest.mark.benchmark
def test_benchmark_event_chain_three_levels_parallel(
    benchmark,
    event_processor_chain_parallel: EventProcessor,
) -> None:
    """Benchmark: 1 root event -> 10 L2 -> 50 L3 (61 total), semaphore 4 (dataclass)."""
    processor = event_processor_chain_parallel

    async def run() -> None:
        await processor.emit_events([_EventL1(id_="root")])

    benchmark(lambda: asyncio.run(run()))


@pytest.fixture
def event_processor_chain_sequential() -> EventProcessor:
    """EventProcessor with 3-level chain, sequential (dataclass events)."""
    return _make_processor(parallel=False)


@pytest.mark.benchmark
def test_benchmark_event_chain_three_levels_sequential(
    benchmark,
    event_processor_chain_sequential: EventProcessor,
) -> None:
    """Benchmark: same 3-level chain, sequential (BFS), dataclass events."""
    processor = event_processor_chain_sequential

    async def run() -> None:
        await processor.emit_events([_EventL1(id_="root")])

    benchmark(lambda: asyncio.run(run()))
