"""
Tests for bootstrap_streaming with DependencyInjectorCQRSContainer integration.

This test suite validates that bootstrap_streaming correctly works with
DependencyInjectorCQRSContainer, ensuring that:
- Handlers can be resolved from dependency-injector containers
- Dependencies are properly injected into handlers
- Events are processed correctly
- Streaming functionality works as expected
"""

import asyncio
import typing
from abc import ABC, abstractmethod

import cqrs
import pydantic
from cqrs.container.dependency_injector import DependencyInjectorCQRSContainer
from cqrs.events import DomainEvent, Event, EventHandler
from cqrs.models.event import IEvent
from cqrs.message_brokers import devnull
from cqrs.bootstrap import requests as bootstrap
from cqrs.models.request import Request
from cqrs.handlers.request import StreamingRequestHandler
from cqrs.models.response import Response
from dependency_injector import containers, providers


# ============================================================================
# Test Models and Commands
# ============================================================================


class ProcessItemsCommand(Request):
    item_ids: list[str] = pydantic.Field()


class ProcessItemResult(Response):
    item_id: str = pydantic.Field()
    status: str = pydantic.Field()


class ItemProcessedDomainEvent(DomainEvent, frozen=True):
    item_id: str = pydantic.Field()


# ============================================================================
# Dependencies
# ============================================================================


class IItemService(ABC):
    @abstractmethod
    async def process_item(self, item_id: str) -> str:
        pass


class ItemService(IItemService):
    def __init__(self) -> None:
        self.processed_items: list[str] = []

    async def process_item(self, item_id: str) -> str:
        self.processed_items.append(item_id)
        return f"Processed {item_id}"


class IEventLogger(ABC):
    @abstractmethod
    async def log_event(self, event: DomainEvent) -> None:
        pass


class EventLogger(IEventLogger):
    def __init__(self) -> None:
        self.logged_events: list[DomainEvent] = []

    async def log_event(self, event: DomainEvent) -> None:
        self.logged_events.append(event)


# ============================================================================
# Handlers
# ============================================================================


class StreamingItemHandler(
    StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
):
    def __init__(self, item_service: IItemService) -> None:
        self.item_service = item_service
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
            await self.item_service.process_item(item_id)
            result = ProcessItemResult(item_id=item_id, status="processed")
            yield result


class ItemProcessedEventHandler(EventHandler[ItemProcessedDomainEvent]):
    def __init__(self, event_logger: IEventLogger) -> None:
        self.event_logger = event_logger
        self.processed_events: list[ItemProcessedDomainEvent] = []

    async def handle(self, event: ItemProcessedDomainEvent) -> None:
        await self.event_logger.log_event(event)
        self.processed_events.append(event)


# ============================================================================
# Dependency Injector Containers
# ============================================================================


class ApplicationContainer(containers.DeclarativeContainer):
    # Services
    item_service = providers.Singleton(ItemService)
    event_logger = providers.Singleton(EventLogger)

    # Handlers
    streaming_item_handler = providers.Factory(
        StreamingItemHandler,
        item_service=item_service,
    )
    item_processed_event_handler = providers.Factory(
        ItemProcessedEventHandler,
        event_logger=event_logger,
    )


# ============================================================================
# Tests
# ============================================================================


class TestBootstrapStreamingWithDependencyInjector:
    """Test suite for bootstrap_streaming with DependencyInjectorCQRSContainer."""

    def setup_di_container(self) -> DependencyInjectorCQRSContainer:
        """Set up the dependency injector container for the CQRS framework."""
        container = ApplicationContainer()
        cqrs_container = DependencyInjectorCQRSContainer()
        cqrs_container.attach_external_container(container)
        return cqrs_container

    def commands_mapper(self, mapper: cqrs.RequestMap) -> None:
        """Maps commands to handlers."""
        mapper.bind(ProcessItemsCommand, StreamingItemHandler)

    def domain_events_mapper(self, mapper: cqrs.EventMap) -> None:
        """Maps domain events to handlers."""
        mapper.bind(ItemProcessedDomainEvent, ItemProcessedEventHandler)

    async def test_bootstrap_streaming_with_dependency_injector_container(
        self,
    ) -> None:
        """
        Test that bootstrap_streaming works with DependencyInjectorCQRSContainer.

        Validates that:
        - bootstrap_streaming accepts DependencyInjectorCQRSContainer
        - Handlers are resolved from the container
        - Dependencies are injected correctly
        - Streaming works as expected
        """
        cqrs_container = self.setup_di_container()

        mediator = bootstrap.bootstrap_streaming(
            di_container=cqrs_container,
            commands_mapper=self.commands_mapper,
            message_broker=devnull.DevnullMessageBroker(),
        )

        request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
        results = []
        async for result in mediator.stream(request):
            results.append(result)

        # Wait for background tasks to complete
        await asyncio.sleep(0.1)

        assert len(results) == 3
        assert results[0].item_id == "item1"
        assert results[1].item_id == "item2"
        assert results[2].item_id == "item3"

        # Verify handler was called and dependencies were injected
        # Check through item_service (singleton) that handler was executed
        item_service = await cqrs_container.resolve(ItemService)
        assert len(item_service.processed_items) == 3
        assert "item1" in item_service.processed_items
        assert "item2" in item_service.processed_items
        assert "item3" in item_service.processed_items

        # Verify handler can be resolved and dependencies are injected correctly
        handler = await cqrs_container.resolve(StreamingItemHandler)
        assert handler.item_service is not None
        assert isinstance(handler.item_service, ItemService)
        # Verify that handler uses the same service instance (singleton)
        assert handler.item_service is item_service

    async def test_bootstrap_streaming_with_events_and_dependency_injection(
        self,
    ) -> None:
        """
        Test that bootstrap_streaming processes events with dependency injection.

        Validates that:
        - Event handlers are resolved from the container
        - Event handler dependencies are injected correctly
        - Events are processed after command execution
        """

        # Create a handler that emits events
        class EventEmittingHandler(
            StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
        ):
            def __init__(self, item_service: IItemService) -> None:
                self.item_service = item_service
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
                for item_id in request.item_ids:
                    await self.item_service.process_item(item_id)
                    result = ProcessItemResult(item_id=item_id, status="processed")
                    # Emit domain event
                    event = ItemProcessedDomainEvent(item_id=item_id)
                    self._events.append(event)
                    yield result

        # Create a container with the handler registered
        class EventEmittingContainer(containers.DeclarativeContainer):
            item_service = providers.Singleton(ItemService)
            event_logger = providers.Singleton(EventLogger)
            event_emitting_handler = providers.Factory(
                EventEmittingHandler,
                item_service=item_service,
            )
            item_processed_event_handler = providers.Factory(
                ItemProcessedEventHandler,
                event_logger=event_logger,
            )

        container = EventEmittingContainer()
        cqrs_container = DependencyInjectorCQRSContainer()
        cqrs_container.attach_external_container(container)

        # Update mapper
        def commands_mapper_override(mapper: cqrs.RequestMap) -> None:
            mapper.bind(ProcessItemsCommand, EventEmittingHandler)

        mediator = bootstrap.bootstrap_streaming(
            di_container=cqrs_container,
            commands_mapper=commands_mapper_override,
            domain_events_mapper=self.domain_events_mapper,
            message_broker=devnull.DevnullMessageBroker(),
        )

        request = ProcessItemsCommand(item_ids=["item1", "item2"])
        results = []
        async for result in mediator.stream(request):
            results.append(result)

        # Wait for background tasks to complete
        await asyncio.sleep(0.2)

        assert len(results) == 2

        # Verify event handler was called and dependencies were injected
        # Since ItemProcessedEventHandler is a Factory, we get a new instance each time
        # So we check through event_logger (Singleton) that events were processed
        event_logger = await cqrs_container.resolve(EventLogger)
        assert len(event_logger.logged_events) == 2
        assert isinstance(event_logger.logged_events[0], ItemProcessedDomainEvent)
        assert isinstance(event_logger.logged_events[1], ItemProcessedDomainEvent)
        assert event_logger.logged_events[0].item_id == "item1"
        assert event_logger.logged_events[1].item_id == "item2"

        # Verify that event handler can be resolved and dependencies are injected
        event_handler = await cqrs_container.resolve(ItemProcessedEventHandler)
        assert event_handler.event_logger is not None
        assert isinstance(event_handler.event_logger, EventLogger)
        # Verify that handler uses the same logger instance (singleton)
        assert event_handler.event_logger is event_logger

    async def test_bootstrap_streaming_with_parallel_event_handling(
        self,
    ) -> None:
        """
        Test that bootstrap_streaming processes events in parallel with dependency injection.

        Validates that:
        - Multiple event handlers can run in parallel
        - Each handler instance gets its own dependencies
        - Parallel processing works correctly with dependency injection
        """

        # Create a handler that emits events
        class EventEmittingHandler(
            StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
        ):
            def __init__(self, item_service: IItemService) -> None:
                self.item_service = item_service
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
                for item_id in request.item_ids:
                    await self.item_service.process_item(item_id)
                    result = ProcessItemResult(item_id=item_id, status="processed")
                    # Emit domain event
                    event = ItemProcessedDomainEvent(item_id=item_id)
                    self._events.append(event)
                    yield result

        # Create a container with the handler registered
        class EventEmittingContainer(containers.DeclarativeContainer):
            item_service = providers.Singleton(ItemService)
            event_logger = providers.Singleton(EventLogger)
            event_emitting_handler = providers.Factory(
                EventEmittingHandler,
                item_service=item_service,
            )
            item_processed_event_handler = providers.Factory(
                ItemProcessedEventHandler,
                event_logger=event_logger,
            )

        container = EventEmittingContainer()
        cqrs_container = DependencyInjectorCQRSContainer()
        cqrs_container.attach_external_container(container)

        # Update mapper
        def commands_mapper_override(mapper: cqrs.RequestMap) -> None:
            mapper.bind(ProcessItemsCommand, EventEmittingHandler)

        mediator = bootstrap.bootstrap_streaming(
            di_container=cqrs_container,
            commands_mapper=commands_mapper_override,
            domain_events_mapper=self.domain_events_mapper,
            message_broker=devnull.DevnullMessageBroker(),
            max_concurrent_event_handlers=3,
            concurrent_event_handle_enable=True,
        )

        request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
        results = []
        async for result in mediator.stream(request):
            results.append(result)

        # Wait for background tasks to complete
        await asyncio.sleep(0.2)

        assert len(results) == 3

        # Verify event handler was called and dependencies were injected
        # Since ItemProcessedEventHandler is a Factory, we get a new instance each time
        # So we check through event_logger (Singleton) that events were processed
        event_logger = await cqrs_container.resolve(EventLogger)
        assert len(event_logger.logged_events) == 3
        assert isinstance(event_logger.logged_events[0], ItemProcessedDomainEvent)
        assert isinstance(event_logger.logged_events[1], ItemProcessedDomainEvent)
        assert isinstance(event_logger.logged_events[2], ItemProcessedDomainEvent)
        assert event_logger.logged_events[0].item_id == "item1"
        assert event_logger.logged_events[1].item_id == "item2"
        assert event_logger.logged_events[2].item_id == "item3"

        # Verify that event handler can be resolved and dependencies are injected
        event_handler = await cqrs_container.resolve(ItemProcessedEventHandler)
        assert event_handler.event_logger is not None
        assert isinstance(event_handler.event_logger, EventLogger)
        # Verify that handler uses the same logger instance (singleton)
        assert event_handler.event_logger is event_logger

    async def test_bootstrap_streaming_resolves_handlers_by_interface(
        self,
    ) -> None:
        """
        Test that bootstrap_streaming resolves handlers using interface-based resolution.

        Validates that:
        - Handlers can depend on abstract interfaces
        - DependencyInjectorCQRSContainer resolves interfaces to concrete implementations
        - Dependency injection works with interfaces
        """

        # Handler that depends on interface
        class InterfaceBasedHandler(
            StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
        ):
            def __init__(self, item_service: IItemService) -> None:
                self.item_service = item_service
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
                    status = await self.item_service.process_item(item_id)
                    result = ProcessItemResult(item_id=item_id, status=status)
                    yield result

        # Create a container with the handler registered
        class InterfaceBasedContainer(containers.DeclarativeContainer):
            item_service = providers.Singleton(ItemService)
            interface_based_handler = providers.Factory(
                InterfaceBasedHandler,
                item_service=item_service,
            )

        container = InterfaceBasedContainer()
        cqrs_container = DependencyInjectorCQRSContainer()
        cqrs_container.attach_external_container(container)

        def commands_mapper_override(mapper: cqrs.RequestMap) -> None:
            mapper.bind(ProcessItemsCommand, InterfaceBasedHandler)

        mediator = bootstrap.bootstrap_streaming(
            di_container=cqrs_container,
            commands_mapper=commands_mapper_override,
            message_broker=devnull.DevnullMessageBroker(),
        )

        request = ProcessItemsCommand(item_ids=["item1", "item2"])
        results = []
        async for result in mediator.stream(request):
            results.append(result)

        # Wait for background tasks to complete
        await asyncio.sleep(0.1)

        assert len(results) == 2
        assert results[0].item_id == "item1"
        assert results[1].item_id == "item2"

        # Verify handler was resolved and dependencies injected via interface
        # The handler should have been resolved during stream execution
        # We can verify this by checking that the item_service (singleton) was used
        item_service = await cqrs_container.resolve(
            IItemService,
        )  # Resolve via interface
        assert isinstance(item_service, ItemService)  # Concrete implementation
        assert len(item_service.processed_items) == 2
        assert "item1" in item_service.processed_items
        assert "item2" in item_service.processed_items

        # Also verify that handler can be resolved directly
        handler = await cqrs_container.resolve(InterfaceBasedHandler)
        assert handler.item_service is not None
        assert isinstance(handler.item_service, ItemService)  # Concrete implementation
        # Verify that handler uses the same service instance (singleton)
        assert handler.item_service is item_service
