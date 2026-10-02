import asyncio
import typing

import pydantic

import cqrs
from cqrs.events import (
    DomainEvent,
    Event,
    EventEmitter,
    EventHandler,
    EventMap,
)
from cqrs.mediators import RequestMediator
from cqrs.mapping.requests import RequestMap
from cqrs.models.request import Request
from cqrs.handlers.request import RequestHandler


class ProcessItemsCommand(Request):
    item_ids: list[str] = pydantic.Field()


class ProcessItemsCommandHandler(RequestHandler[ProcessItemsCommand, None]):
    def __init__(self) -> None:
        self.called = False
        self._events: list[Event] = []

    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(self, request: ProcessItemsCommand) -> None:
        self.called = True
        for item_id in request.item_ids:
            event = ItemProcessedDomainEvent(item_id=item_id)
            self._events.append(event)


class ItemProcessedDomainEvent(DomainEvent, frozen=True):
    item_id: str = pydantic.Field()


class ItemProcessedEventHandler(EventHandler[ItemProcessedDomainEvent]):
    def __init__(self) -> None:
        self.processed_events: list[ItemProcessedDomainEvent] = []

    async def handle(self, event: ItemProcessedDomainEvent) -> None:
        self.processed_events.append(event)


class Container:
    def __init__(self, handler):
        self._handler = handler

    async def resolve(self, type_):
        return self._handler


async def test_request_mediator_processes_events_parallel() -> None:
    handler = ProcessItemsCommandHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, ProcessItemsCommandHandler)
    container = Container(handler)

    event_handler = ItemProcessedEventHandler()
    event_map = EventMap()
    event_map.bind(ItemProcessedDomainEvent, ItemProcessedEventHandler)

    event_container = Container(event_handler)

    event_emitter = EventEmitter(
        event_map=event_map,
        container=event_container,  # type: ignore
    )

    # Track emit calls
    original_emit = event_emitter.emit
    emit_call_count = 0

    async def tracked_emit(event):
        nonlocal emit_call_count
        emit_call_count += 1
        return await original_emit(event)

    event_emitter.emit = tracked_emit  # type: ignore[assignment]

    mediator = RequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    await mediator.send(request)

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    assert handler.called
    assert len(event_handler.processed_events) == 3
    assert emit_call_count == 3


async def test_request_mediator_processes_events_sequentially() -> None:
    handler = ProcessItemsCommandHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, ProcessItemsCommandHandler)
    container = Container(handler)

    event_handler = ItemProcessedEventHandler()
    event_map = EventMap()
    event_map.bind(ItemProcessedDomainEvent, ItemProcessedEventHandler)

    event_container = Container(event_handler)

    event_emitter = EventEmitter(
        event_map=event_map,
        container=event_container,  # type: ignore
    )

    # Track emit calls
    original_emit = event_emitter.emit
    emit_call_count = 0

    async def tracked_emit(event):
        nonlocal emit_call_count
        emit_call_count += 1
        return await original_emit(event)

    event_emitter.emit = tracked_emit  # type: ignore[assignment]

    mediator = RequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=False,
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    await mediator.send(request)

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    assert handler.called
    assert len(event_handler.processed_events) == 3
    assert emit_call_count == 3
