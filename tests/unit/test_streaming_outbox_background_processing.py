"""
Test to reproduce the bug: '_asyncio.Future' object has no attribute 'handle'

This test reproduces the scenario where:
1. Events are processed from outbox in background
2. StreamingRequestMediator is used with parallel event processing
3. Events are emitted via EventEmitter which processes them in parallel
4. The error occurs when trying to call .handle() on a Future object

The bug likely occurs in EventProcessor.emit_events() when events are processed
in parallel via asyncio.create_task(), and somewhere the code tries to call
.handle() on the task result instead of the handler object.
"""

import asyncio
import functools
import logging
import typing
import uuid
from collections import defaultdict

import pydantic

import cqrs
from cqrs.events import DomainEvent, EventHandler, Event
from cqrs.events.event import IEvent
from cqrs.message_brokers import devnull
from cqrs.outbox import mock
from cqrs.requests import bootstrap
from cqrs.requests.request import Request
from cqrs.requests.request_handler import StreamingRequestHandler
from cqrs.response import Response

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Mock storage for outbox
OUTBOX_STORAGE = defaultdict[
    uuid.UUID,
    typing.List[cqrs.NotificationEvent],
](lambda: [])


# Test models
class ProcessServiceCommand(Request):
    service_id: str = pydantic.Field()


class ProcessServiceResult(Response):
    service_id: str = pydantic.Field()
    status: str = pydantic.Field()


class ServiceChangedDomainEvent(DomainEvent, frozen=True):
    service_id: str = pydantic.Field()


# Event handler that will be called
class ServiceChangedEventHandler(EventHandler[ServiceChangedDomainEvent]):
    def __init__(self) -> None:
        self.handled_events: list[ServiceChangedDomainEvent] = []
        self.call_count = 0

    async def handle(self, event: ServiceChangedDomainEvent) -> None:
        self.call_count += 1
        self.handled_events.append(event)
        logger.info(f"Handled event for service {event.service_id}")


# Streaming handler that emits events
class ProcessServiceStreamingHandler(
    StreamingRequestHandler[ProcessServiceCommand, ProcessServiceResult],
):
    def __init__(self, outbox: cqrs.OutboxedEventRepository) -> None:
        self.outbox = outbox
        self.called = False
        self._events: list[Event] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(
        self,
        request: ProcessServiceCommand,
    ) -> typing.AsyncIterator[ProcessServiceResult]:
        self.called = True
        logger.info(f"Processing service {request.service_id}")

        # Emit domain event
        event = ServiceChangedDomainEvent(service_id=request.service_id)
        self._events.append(event)

        result = ProcessServiceResult(
            service_id=request.service_id,
            status="processed",
        )
        yield result


# Simple container
class MockContainer:
    def __init__(
        self,
        handler: typing.Optional[ProcessServiceStreamingHandler] = None,
        event_handler: typing.Optional[ServiceChangedEventHandler] = None,
    ) -> None:
        self._handler = handler
        self._event_handler = event_handler
        self._handlers: dict[type, object] = {}

    def register_handler(self, handler_type: type, handler_instance: object) -> None:
        """Register a handler instance for a type."""
        self._handlers[handler_type] = handler_instance

    async def resolve(self, type_):
        # Check registered handlers first
        if type_ in self._handlers:
            return self._handlers[type_]
        # Fallback to old behavior for backward compatibility
        if type_ == ProcessServiceStreamingHandler and self._handler:
            return self._handler
        if type_ == ServiceChangedEventHandler and self._event_handler:
            return self._event_handler
        return None


async def test_streaming_outbox_background_processing_reproduces_bug():
    """
    Test that reproduces the bug when processing events from outbox in background.

    This test simulates the scenario where:
    1. A streaming handler processes a request and emits events
    2. Events are processed in parallel via EventProcessor
    3. Event handlers are called via EventEmitter
    4. The bug occurs when trying to call .handle() on a Future object

    Expected behavior: Events should be processed without errors.
    Actual behavior: Error '_asyncio.Future' object has no attribute 'handle'
    """
    # Setup outbox repository
    mock_repository_factory = functools.partial(
        mock.MockOutboxedEventRepository,
        session_factory=functools.partial(lambda: OUTBOX_STORAGE),
    )
    outbox_repository = mock_repository_factory()

    # Create handlers
    handler = ProcessServiceStreamingHandler(outbox_repository)
    event_handler = ServiceChangedEventHandler()

    # Create container
    container = MockContainer(handler, event_handler)

    # Setup mappers
    def commands_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(ProcessServiceCommand, ProcessServiceStreamingHandler)

    def domain_events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(ServiceChangedDomainEvent, ServiceChangedEventHandler)

    # Create mediator with parallel event processing enabled
    mediator = bootstrap.bootstrap_streaming(
        di_container=container,  # type: ignore
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        message_broker=devnull.DevnullMessageBroker(),
        max_concurrent_event_handlers=3,
        concurrent_event_handle_enable=True,  # Enable parallel processing
    )

    # Process request
    request = ProcessServiceCommand(service_id="service-1")
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    # Wait for background tasks to complete
    # This is where the bug might occur - background tasks might try to call .handle()
    # on a Future object instead of the handler object
    await asyncio.sleep(0.2)

    # Verify results
    assert handler.called
    assert len(results) == 1
    assert results[0].service_id == "service-1"

    # Verify event handler was called
    # This might fail if the bug occurs
    assert event_handler.call_count == 1
    assert len(event_handler.handled_events) == 1
    assert event_handler.handled_events[0].service_id == "service-1"


async def test_streaming_outbox_background_processing_sequential():
    """
    Test with sequential event processing (should work without errors).
    """
    # Setup outbox repository
    mock_repository_factory = functools.partial(
        mock.MockOutboxedEventRepository,
        session_factory=functools.partial(lambda: OUTBOX_STORAGE),
    )
    outbox_repository = mock_repository_factory()

    # Create handlers
    handler = ProcessServiceStreamingHandler(outbox_repository)
    event_handler = ServiceChangedEventHandler()

    # Create container and register handlers
    container = MockContainer(handler, event_handler)
    container.register_handler(ProcessServiceStreamingHandler, handler)
    container.register_handler(ServiceChangedEventHandler, event_handler)

    # Setup mappers
    def commands_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(ProcessServiceCommand, ProcessServiceStreamingHandler)

    def domain_events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(ServiceChangedDomainEvent, ServiceChangedEventHandler)

    # Create mediator with sequential event processing
    mediator = bootstrap.bootstrap_streaming(
        di_container=container,  # type: ignore
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        message_broker=devnull.DevnullMessageBroker(),
        max_concurrent_event_handlers=1,
        concurrent_event_handle_enable=False,  # Sequential processing
    )

    # Process request
    request = ProcessServiceCommand(service_id="service-1")
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    # Verify results
    assert handler.called
    assert len(results) == 1
    assert results[0].service_id == "service-1"

    # Verify event handler was called
    assert event_handler.call_count == 1
    assert len(event_handler.handled_events) == 1
    assert event_handler.handled_events[0].service_id == "service-1"


async def test_streaming_outbox_multiple_events_parallel():
    """
    Test with multiple events processed in parallel (more likely to trigger the bug).
    """
    # Setup outbox repository
    mock_repository_factory = functools.partial(
        mock.MockOutboxedEventRepository,
        session_factory=functools.partial(lambda: OUTBOX_STORAGE),
    )
    outbox_repository = mock_repository_factory()

    # Create a handler that emits multiple events
    class MultiEventStreamingHandler(
        StreamingRequestHandler[ProcessServiceCommand, ProcessServiceResult],
    ):
        def __init__(self, outbox: cqrs.OutboxedEventRepository) -> None:
            self.outbox = outbox
            self.called = False
            self._events: list[Event] = []

        @property
        def events(self) -> typing.Sequence[IEvent]:
            return self._events.copy()

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(
            self,
            request: ProcessServiceCommand,
        ) -> typing.AsyncIterator[ProcessServiceResult]:
            self.called = True
            # Emit multiple events
            for i in range(3):
                event = ServiceChangedDomainEvent(
                    service_id=f"{request.service_id}-{i}",
                )
                self._events.append(event)
                result = ProcessServiceResult(
                    service_id=f"{request.service_id}-{i}",
                    status="processed",
                )
                yield result

    handler = MultiEventStreamingHandler(outbox_repository)
    event_handler = ServiceChangedEventHandler()

    # Create container and register handlers
    container = MockContainer()
    container.register_handler(MultiEventStreamingHandler, handler)
    container.register_handler(ServiceChangedEventHandler, event_handler)

    # Setup mappers
    def commands_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(ProcessServiceCommand, MultiEventStreamingHandler)

    def domain_events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(ServiceChangedDomainEvent, ServiceChangedEventHandler)

    # Create mediator with parallel event processing
    mediator = bootstrap.bootstrap_streaming(
        di_container=container,  # type: ignore
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        message_broker=devnull.DevnullMessageBroker(),
        max_concurrent_event_handlers=5,
        concurrent_event_handle_enable=True,  # Parallel processing
    )

    # Process request
    request = ProcessServiceCommand(service_id="service-1")
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    # Wait for background tasks to complete
    # This is where the bug is most likely to occur
    await asyncio.sleep(0.3)

    # Verify results
    assert handler.called
    assert len(results) == 3

    # Verify event handler was called for all events
    # This might fail if the bug occurs
    assert event_handler.call_count == 3
    assert len(event_handler.handled_events) == 3
