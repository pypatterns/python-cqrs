"""Integration test: 3-level event chain (L1 -> L2 -> L3) via RequestMediator + EventProcessor."""

import asyncio
import typing

import di
import pydantic

import cqrs
from cqrs.models.event import IEvent
from cqrs.bootstrap import requests as bootstrap

PROCESSED_L1: list[cqrs.DomainEvent] = []
PROCESSED_L2: list[cqrs.DomainEvent] = []
PROCESSED_L3: list[cqrs.DomainEvent] = []


class EmitL1Command(cqrs.Request):
    """Command that emits L1 event."""

    seed: str = pydantic.Field()


class EventL1(cqrs.DomainEvent, frozen=True):
    level: int = 1
    seed: str = pydantic.Field()


class EventL2(cqrs.DomainEvent, frozen=True):
    level: int = 2
    seed: str = pydantic.Field()


class EventL3(cqrs.DomainEvent, frozen=True):
    level: int = 3
    seed: str = pydantic.Field()


class EmitL1CommandHandler(cqrs.RequestHandler[EmitL1Command, None]):
    def __init__(self) -> None:
        self._events: list[IEvent] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        return tuple(self._events)

    async def handle(self, request: EmitL1Command) -> None:
        self._events.append(EventL1(seed=request.seed))


class HandlerL1(cqrs.EventHandler[EventL1]):
    def __init__(self) -> None:
        self._follow_ups: list[IEvent] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        return tuple(self._follow_ups)

    async def handle(self, event: EventL1) -> None:
        PROCESSED_L1.append(event)
        self._follow_ups.append(EventL2(seed=event.seed))


class HandlerL2(cqrs.EventHandler[EventL2]):
    def __init__(self) -> None:
        self._follow_ups: list[IEvent] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        return tuple(self._follow_ups)

    async def handle(self, event: EventL2) -> None:
        PROCESSED_L2.append(event)
        self._follow_ups.append(EventL3(seed=event.seed))


class HandlerL3(cqrs.EventHandler[EventL3]):
    async def handle(self, event: EventL3) -> None:
        PROCESSED_L3.append(event)


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(EmitL1Command, EmitL1CommandHandler)


def events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(EventL1, HandlerL1)
    mapper.bind(EventL2, HandlerL2)
    mapper.bind(EventL3, HandlerL3)


async def test_three_level_event_chain_via_request_mediator() -> None:
    """Arrange: command emits L1; L1 handler returns L2; L2 returns L3; L3 returns ().
    Act: mediator.send(command). Assert: L1, L2, L3 all processed."""
    PROCESSED_L1.clear()
    PROCESSED_L2.clear()
    PROCESSED_L3.clear()

    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=commands_mapper,
        domain_events_mapper=events_mapper,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
    )

    await mediator.send(EmitL1Command(seed="x"))
    await asyncio.sleep(0.15)

    assert len(PROCESSED_L1) == 1
    assert PROCESSED_L1[0].seed == "x"  # type: ignore[attr-defined]
    assert len(PROCESSED_L2) == 1
    assert PROCESSED_L2[0].seed == "x"  # type: ignore[attr-defined]
    assert len(PROCESSED_L3) == 1
    assert PROCESSED_L3[0].seed == "x"  # type: ignore[attr-defined]
