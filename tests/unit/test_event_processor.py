import asyncio
import typing
from unittest import mock

import pydantic

from cqrs import Event
from cqrs.events import (
    DomainEvent,
    EventEmitter,
    EventHandler,
    EventMap,
)
from cqrs.events.event_processor import EventProcessor


class _TestDomainEvent(DomainEvent, frozen=True):
    """Test domain event."""

    item_id: str = pydantic.Field()


class _TestEventHandler(EventHandler[_TestDomainEvent]):
    """Test event handler."""

    def __init__(self) -> None:
        self.processed_events: list[_TestDomainEvent] = []
        self.call_order: list[int] = []

    async def handle(self, event: _TestDomainEvent) -> None:
        """Handle event and track order."""
        self.processed_events.append(event)
        self.call_order.append(int(event.item_id))


class Container:
    """Simple container for testing."""

    def __init__(self, handler: _TestEventHandler) -> None:
        self._handler = handler

    async def resolve(self, type_) -> _TestEventHandler:
        """Resolve handler."""
        return self._handler


async def test_event_processor_processes_events_parallel() -> None:
    """Test that EventProcessor processes events in parallel when enabled."""
    event_handler = _TestEventHandler()
    event_map = EventMap()
    event_map.bind(_TestDomainEvent, _TestEventHandler)
    container = Container(event_handler)

    event_emitter = EventEmitter(
        event_map=event_map,
        container=container,  # type: ignore
    )

    processor = EventProcessor(
        event_map=event_map,
        event_emitter=event_emitter,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
    )

    events: list[Event] = [
        _TestDomainEvent(item_id="1"),
        _TestDomainEvent(item_id="2"),
        _TestDomainEvent(item_id="3"),
    ]
    await processor.emit_events(events)

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    assert len(event_handler.processed_events) == 3
    assert all(event in event_handler.processed_events for event in events)


async def test_event_processor_processes_events_sequentially() -> None:
    """Test that EventProcessor processes events sequentially when disabled."""
    event_handler = _TestEventHandler()
    event_map = EventMap()
    event_map.bind(_TestDomainEvent, _TestEventHandler)
    container = Container(event_handler)

    event_emitter = EventEmitter(
        event_map=event_map,
        container=container,  # type: ignore
    )

    processor = EventProcessor(
        event_map=event_map,
        event_emitter=event_emitter,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=False,
    )

    events: list[Event] = [
        _TestDomainEvent(item_id="1"),
        _TestDomainEvent(item_id="2"),
        _TestDomainEvent(item_id="3"),
    ]

    await processor.emit_events(events)

    assert len(event_handler.processed_events) == 3
    assert all(event in event_handler.processed_events for event in events)


async def test_event_processor_processes_empty_events_list() -> None:
    """Test that EventProcessor handles empty events list gracefully."""
    event_handler = _TestEventHandler()
    event_map = EventMap()
    event_map.bind(_TestDomainEvent, _TestEventHandler)

    processor = EventProcessor(
        event_map=event_map,
    )

    await processor.emit_events([])

    assert len(event_handler.processed_events) == 0


async def test_event_processor_emit_events_with_emitter() -> None:
    """Test that EventProcessor emits events via EventEmitter."""
    event_map = EventMap()

    event_emitter = mock.AsyncMock(spec=EventEmitter)
    event_emitter.emit = mock.AsyncMock(return_value=())

    processor = EventProcessor(
        event_map=event_map,
        event_emitter=event_emitter,
    )

    events = [
        _TestDomainEvent(item_id="1"),
        _TestDomainEvent(item_id="2"),
    ]

    events_copy = events.copy()
    await processor.emit_events(events_copy)  # type: ignore[arg-type]  # type: ignore[arg-type]

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    assert event_emitter.emit.call_count == 2
    # Check that events were passed to emit (order may vary)
    emitted_events = [call[0][0] for call in event_emitter.emit.call_args_list]
    assert events[0] in emitted_events
    assert events[1] in emitted_events
    # Original list should remain unchanged
    assert len(events) == 2
    assert len(events_copy) == 2


async def test_event_processor_emit_events_without_emitter() -> None:
    """Test that EventProcessor does nothing when EventEmitter is None."""
    event_map = EventMap()

    processor = EventProcessor(
        event_map=event_map,
        event_emitter=None,
    )

    events = [
        _TestDomainEvent(item_id="1"),
        _TestDomainEvent(item_id="2"),
    ]

    # Should not raise any exception
    await processor.emit_events(events.copy())  # type: ignore[arg-type]  # type: ignore[arg-type]


async def test_event_processor_process_and_emit_events() -> None:
    """Test that process_and_emit_events combines processing and emitting."""
    event_handler = _TestEventHandler()
    event_map = EventMap()
    event_map.bind(_TestDomainEvent, _TestEventHandler)
    container = Container(event_handler)

    # Create real EventEmitter to process events, but wrap message_broker with mock
    event_emitter = EventEmitter(
        event_map=event_map,
        container=container,  # type: ignore
    )

    processor = EventProcessor(
        event_map=event_map,
        event_emitter=event_emitter,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
    )

    events = [
        _TestDomainEvent(item_id="1"),
        _TestDomainEvent(item_id="2"),
    ]

    await processor.emit_events(events)  # type: ignore[arg-type]  # type: ignore[arg-type]

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    # Events should be processed
    assert len(event_handler.processed_events) == 2
    # Original list should not be modified (copy is used internally)
    assert len(events) == 2


async def test_event_processor_respects_semaphore_limit() -> None:
    """Test that EventProcessor respects semaphore limit for concurrent processing."""
    # Track concurrent executions
    concurrent_count = 0
    max_concurrent = 0
    lock = asyncio.Lock()

    class TrackingEventHandler(EventHandler[_TestDomainEvent]):
        """Event handler that tracks concurrent executions."""

        def __init__(self) -> None:
            self.processed_events: list[_TestDomainEvent] = []

        async def handle(self, event: _TestDomainEvent) -> None:
            """Handle event and track concurrency."""
            nonlocal concurrent_count, max_concurrent
            async with lock:
                concurrent_count += 1
                max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.01)  # Small delay to allow concurrency
            async with lock:
                concurrent_count -= 1
            self.processed_events.append(event)

    event_handler = TrackingEventHandler()
    event_map = EventMap()
    event_map.bind(_TestDomainEvent, TrackingEventHandler)
    container = Container(event_handler)  # type: ignore

    event_emitter = EventEmitter(
        event_map=event_map,
        container=container,  # type: ignore
    )

    # Use semaphore limit of 2
    processor = EventProcessor(
        event_map=event_map,
        event_emitter=event_emitter,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
    )

    events = [_TestDomainEvent(item_id=str(i)) for i in range(5)]
    await processor.emit_events(events)  # type: ignore[arg-type]

    # Wait for background tasks to complete
    await asyncio.sleep(0.2)

    # Max concurrent should not exceed semaphore limit (2)
    assert max_concurrent <= 2
    assert len(event_handler.processed_events) == 5


async def test_event_processor_follow_ups_sequential_bfs() -> None:
    """Arrange: handler that returns follow-up events. Act: emit_events sequential. Assert: follow-ups processed (BFS)."""

    class _ChainedEvent(DomainEvent, frozen=True):
        level: int = pydantic.Field()
        seq: int = pydantic.Field()

    processed: list[_ChainedEvent] = []

    class _ChainedHandler(EventHandler[_ChainedEvent]):
        @property
        def events(self) -> tuple[_ChainedEvent, ...]:
            if not self._last or self._last.level >= 2:
                return ()
            return (_ChainedEvent(level=self._last.level + 1, seq=self._last.seq),)

        def __init__(self) -> None:
            self._last: typing.Optional[_ChainedEvent] = None

        async def handle(self, event: _ChainedEvent) -> None:
            self._last = event
            processed.append(event)

    handler = _ChainedHandler()
    event_map = EventMap()
    event_map.bind(_ChainedEvent, _ChainedHandler)
    container = Container(handler)  # type: ignore[arg-type]
    emitter = EventEmitter(event_map=event_map, container=container)  # type: ignore[arg-type]
    processor = EventProcessor(
        event_map=event_map,
        event_emitter=emitter,
        concurrent_event_handle_enable=False,
    )
    await processor.emit_events([_ChainedEvent(level=0, seq=1)])
    assert len(processed) == 3  # level 0 -> 1 -> 2
    assert processed[0].level == 0 and processed[1].level == 1 and processed[2].level == 2


async def test_event_processor_follow_ups_parallel_under_semaphore() -> None:
    """Arrange: handler returns 3 follow-ups, semaphore 2. Act: emit one event. Assert: all 4 processed, max concurrent <= 2."""

    class _FanEvent(DomainEvent, frozen=True):
        id_: str = pydantic.Field(alias="id")
        model_config = pydantic.ConfigDict(populate_by_name=True)

    concurrent_count = 0
    max_concurrent = 0
    lock = asyncio.Lock()
    processed: list[_FanEvent] = []

    class _FanHandler(EventHandler[_FanEvent]):
        def __init__(self) -> None:
            self._follow_ups: list[_FanEvent] = []

        @property
        def events(self) -> tuple[_FanEvent, ...]:
            return tuple(self._follow_ups)

        async def handle(self, event: _FanEvent) -> None:
            nonlocal concurrent_count, max_concurrent
            self._follow_ups = []
            if event.id_ == "root":
                self._follow_ups = [
                    _FanEvent(id="c1"),
                    _FanEvent(id="c2"),
                    _FanEvent(id="c3"),
                ]
            async with lock:
                concurrent_count += 1
                max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.02)
            async with lock:
                concurrent_count -= 1
            processed.append(event)
            # Clear after handle so emitter always sees follow-ups from this run only
            if event.id_ != "root":
                self._follow_ups = []

    handler = _FanHandler()
    event_map = EventMap()
    event_map.bind(_FanEvent, _FanHandler)
    container = Container(handler)  # type: ignore[arg-type]
    emitter = EventEmitter(event_map=event_map, container=container)  # type: ignore[arg-type]
    processor = EventProcessor(
        event_map=event_map,
        event_emitter=emitter,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
    )
    await processor.emit_events([_FanEvent(id="root")])
    assert len(processed) == 4
    assert max_concurrent <= 2
