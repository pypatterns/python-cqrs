"""Unit tests for EventDispatcher dispatching follow-up events from handler.events."""

import typing

import pydantic

from cqrs.events import DomainEvent, EventHandler, EventMap
from cqrs.models.event import IEvent
from cqrs.dispatcher.event import EventDispatcher


class _EventL1(DomainEvent, frozen=True):
    name: str = pydantic.Field()


class _EventL2(DomainEvent, frozen=True):
    name: str = pydantic.Field()


class _HandlerL1(EventHandler[_EventL1]):
    def __init__(self) -> None:
        self.processed: list[_EventL1] = []
        self._follow_ups: list[IEvent] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        return tuple(self._follow_ups)

    async def handle(self, event: _EventL1) -> None:
        self._follow_ups = []
        self.processed.append(event)
        self._follow_ups.append(_EventL2(name="L2_from_" + event.name))


class _HandlerL2(EventHandler[_EventL2]):
    def __init__(self) -> None:
        self.processed: list[_EventL2] = []

    async def handle(self, event: _EventL2) -> None:
        self.processed.append(event)


class _Container:
    def __init__(self, h1: _HandlerL1, h2: _HandlerL2) -> None:
        self._h1 = h1
        self._h2 = h2

    async def resolve(self, type_: type) -> EventHandler[IEvent]:
        if type_ is _HandlerL1:
            return self._h1  # type: ignore[return-value]
        if type_ is _HandlerL2:
            return self._h2  # type: ignore[return-value]
        raise KeyError(type_)


async def test_event_dispatcher_dispatch_follow_ups() -> None:
    """Arrange: event A, handler A returns [event B], handler B registered. Act: dispatch(A). Assert: A and B handled."""
    handler_l1 = _HandlerL1()
    handler_l2 = _HandlerL2()
    event_map = EventMap()
    event_map.bind(_EventL1, _HandlerL1)
    event_map.bind(_EventL2, _HandlerL2)
    container = _Container(handler_l1, handler_l2)

    dispatcher = EventDispatcher(
        event_map=event_map,
        container=container,  # type: ignore[arg-type]
    )

    await dispatcher.dispatch(_EventL1(name="a"))

    assert len(handler_l1.processed) == 1
    assert handler_l1.processed[0].name == "a"
    assert len(handler_l2.processed) == 1
    assert handler_l2.processed[0].name == "L2_from_a"
