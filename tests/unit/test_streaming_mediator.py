import asyncio
import typing

import pydantic

from cqrs import events
from cqrs.events import (
    DomainEvent,
    Event,
    EventEmitter,
    EventHandler,
    EventMap,
    NotificationEvent,
)
from cqrs.models.event import IEvent
from cqrs.mediators import StreamingRequestMediator
from cqrs.message_brokers import devnull
from cqrs.mapping.requests import RequestMap
from cqrs.models.request import Request
from cqrs.handlers.request import StreamingRequestHandler
from cqrs.models.response import Response


class ProcessItemsCommand(Request):
    item_ids: list[str] = pydantic.Field()


class ProcessItemResult(Response):
    item_id: str = pydantic.Field()
    status: str = pydantic.Field()


class StreamingHandler(StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult]):
    def __init__(self) -> None:
        self.called = False
        self._events: list[Event] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(
        self,
        request: ProcessItemsCommand,
    ) -> typing.AsyncIterator[ProcessItemResult]:
        self.called = True
        for item_id in request.item_ids:
            result = ProcessItemResult(item_id=item_id, status="processed")
            event = NotificationEvent(
                event_name="ItemProcessed",
                payload={"item_id": item_id},
            )
            self._events.append(event)
            yield result


class Container:
    def __init__(self, handler):
        self._handler = handler

    async def resolve(self, type_):
        return self._handler


async def test_streaming_mediator_stream_returns_async_iterator_consumable_with_async_for() -> None:
    """
    Contract: mediator.stream(request) is called without await
    and returns an AsyncIterator that is consumed with async for.
    """
    handler = StreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, StreamingHandler)
    container = Container(handler)
    event_map = EventMap()
    message_broker = devnull.DevnullMessageBroker()
    event_emitter = EventEmitter(
        event_map=event_map,
        container=container,  # type: ignore
        message_broker=message_broker,
    )
    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
    )
    request = ProcessItemsCommand(item_ids=["a", "b"])
    # stream() is called (no await) and returns async iterator
    async_gen = mediator.stream(request)
    results = []
    async for item in async_gen:
        results.append(item)
    assert len(results) == 2
    assert results[0].item_id == "a"
    assert results[1].item_id == "b"
    assert handler.called


async def test_streaming_mediator_logic() -> None:
    handler = StreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, StreamingHandler)
    container = Container(handler)

    event_map = EventMap()
    message_broker = devnull.DevnullMessageBroker()
    event_emitter = EventEmitter(
        event_map=event_map,
        container=container,  # type: ignore
        message_broker=message_broker,
    )

    # Track emit calls
    original_emit = event_emitter.emit
    emit_call_count = 0

    async def tracked_emit(event):
        nonlocal emit_call_count
        emit_call_count += 1
        return await original_emit(event)

    event_emitter.emit = tracked_emit  # type: ignore[assignment]

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    assert handler.called
    assert len(results) == 3
    assert results[0].item_id == "item1"
    assert results[1].item_id == "item2"
    assert results[2].item_id == "item3"

    assert emit_call_count == 3


async def test_streaming_mediator_without_event_emitter() -> None:
    handler = StreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, StreamingHandler)
    container = Container(handler)

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=None,
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    assert handler.called
    assert len(results) == 2


async def test_streaming_mediator_events_order() -> None:
    handler = StreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, StreamingHandler)
    container = Container(handler)

    emitted_events = []

    async def mock_emit(event: Event):
        emitted_events.append(event)

    message_broker = devnull.DevnullMessageBroker()
    event_emitter = EventEmitter(
        event_map=EventMap(),
        container=container,  # type: ignore
        message_broker=message_broker,
    )
    event_emitter.emit = mock_emit  # type: ignore[assignment]

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)
        # Wait a bit for background task to complete
        await asyncio.sleep(0.05)
        assert len(emitted_events) == len(results)

    assert len(emitted_events) == 2
    assert isinstance(emitted_events[0], events.NotificationEvent)
    assert isinstance(emitted_events[1], events.NotificationEvent)
    assert emitted_events[0].payload["item_id"] == "item1"  # type: ignore
    assert emitted_events[1].payload["item_id"] == "item2"  # type: ignore


class ItemProcessedDomainEvent(DomainEvent, frozen=True):
    item_id: str = pydantic.Field()


class ItemProcessedEventHandler(EventHandler[ItemProcessedDomainEvent]):
    def __init__(self) -> None:
        self.processed_events: list[ItemProcessedDomainEvent] = []

    async def handle(self, event: ItemProcessedDomainEvent) -> None:
        self.processed_events.append(event)


class EventHandlerStreamingHandler(
    StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
):
    def __init__(self) -> None:
        self.called = False
        self._events: list[Event] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(
        self,
        request: ProcessItemsCommand,
    ) -> typing.AsyncIterator[ProcessItemResult]:
        self.called = True
        for item_id in request.item_ids:
            result = ProcessItemResult(item_id=item_id, status="processed")
            event = ItemProcessedDomainEvent(item_id=item_id)
            self._events.append(event)
            yield result


async def test_streaming_mediator_processes_events_parallel() -> None:
    handler = EventHandlerStreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, EventHandlerStreamingHandler)
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

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    assert handler.called
    assert len(results) == 3
    assert len(event_handler.processed_events) == 3
    assert emit_call_count == 3


async def test_streaming_mediator_processes_events_sequentially() -> None:
    handler = EventHandlerStreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, EventHandlerStreamingHandler)
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

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=False,
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    assert handler.called
    assert len(results) == 3
    assert len(event_handler.processed_events) == 3
    assert emit_call_count == 3
