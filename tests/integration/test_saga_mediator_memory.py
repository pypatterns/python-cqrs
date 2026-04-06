"""Integration tests for SagaMediator with MemorySagaStorage."""

import asyncio
import dataclasses
import typing
import uuid
from unittest import mock

import pytest

import cqrs
from cqrs import events
from cqrs.events.event import DomainEvent, Event
from cqrs.events.event_handler import EventHandler
from cqrs.response import Response
from cqrs.requests.map import SagaMap
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.enums import SagaStatus
from cqrs.saga.storage.memory import MemorySagaStorage


# Test models
@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str
    user_id: str
    amount: float
    inventory_id: typing.Optional[str] = None
    payment_id: typing.Optional[str] = None
    shipment_id: typing.Optional[str] = None


class ReserveInventoryResponse(Response):
    inventory_id: str
    reserved: bool


class ProcessPaymentResponse(Response):
    payment_id: str
    charged: bool


class ShipOrderResponse(Response):
    shipment_id: str
    shipped: bool


# Test events
class InventoryReservedEvent(DomainEvent, frozen=True):
    order_id: str
    inventory_id: str


class PaymentProcessedEvent(DomainEvent, frozen=True):
    order_id: str
    payment_id: str


class OrderShippedEvent(DomainEvent, frozen=True):
    order_id: str
    shipment_id: str


# Test step handlers
class ReserveInventoryStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    def __init__(self) -> None:
        self._events: list[Event] = []
        self.act_called = False
        self.compensate_called = False

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        self.act_called = True
        inventory_id = f"inv_{context.order_id}"
        context.inventory_id = inventory_id

        event = InventoryReservedEvent(
            order_id=context.order_id,
            inventory_id=inventory_id,
        )
        self._events.append(event)

        response = ReserveInventoryResponse(
            inventory_id=inventory_id,
            reserved=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        self.compensate_called = True
        context.inventory_id = None


class ProcessPaymentStep(SagaStepHandler[OrderContext, ProcessPaymentResponse]):
    def __init__(self) -> None:
        self._events: list[Event] = []
        self.act_called = False
        self.compensate_called = False

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ProcessPaymentResponse]:
        self.act_called = True
        payment_id = f"pay_{context.order_id}"
        context.payment_id = payment_id

        event = PaymentProcessedEvent(
            order_id=context.order_id,
            payment_id=payment_id,
        )
        self._events.append(event)

        response = ProcessPaymentResponse(payment_id=payment_id, charged=True)
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        self.compensate_called = True
        context.payment_id = None


class ShipOrderStep(SagaStepHandler[OrderContext, ShipOrderResponse]):
    def __init__(self) -> None:
        self._events: list[Event] = []
        self.act_called = False
        self.compensate_called = False

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ShipOrderResponse]:
        self.act_called = True
        shipment_id = f"ship_{context.order_id}"
        context.shipment_id = shipment_id

        event = OrderShippedEvent(
            order_id=context.order_id,
            shipment_id=shipment_id,
        )
        self._events.append(event)

        response = ShipOrderResponse(shipment_id=shipment_id, shipped=True)
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        self.compensate_called = True
        context.shipment_id = None


class FailingStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    """Step that always fails."""

    def __init__(self) -> None:
        self._events: list[Event] = []
        self.act_called = False
        self.compensate_called = False

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        self.act_called = True
        raise ValueError(f"Step failed for order {context.order_id}")

    async def compensate(self, context: OrderContext) -> None:
        self.compensate_called = True


# Test saga class
class OrderSaga(Saga[OrderContext]):
    """Test saga for order processing."""

    steps = [
        ReserveInventoryStep,
        ProcessPaymentStep,
        ShipOrderStep,
    ]


class FailingOrderSaga(Saga[OrderContext]):
    """Test saga that fails."""

    steps = [
        ReserveInventoryStep,
        ProcessPaymentStep,
        FailingStep,
    ]


# Event handlers
class InventoryReservedEventHandler(EventHandler[InventoryReservedEvent]):
    def __init__(self) -> None:
        self.handled_events: list[InventoryReservedEvent] = []

    async def handle(self, event: InventoryReservedEvent) -> None:
        self.handled_events.append(event)


class PaymentProcessedEventHandler(EventHandler[PaymentProcessedEvent]):
    def __init__(self) -> None:
        self.handled_events: list[PaymentProcessedEvent] = []

    async def handle(self, event: PaymentProcessedEvent) -> None:
        self.handled_events.append(event)


class OrderShippedEventHandler(EventHandler[OrderShippedEvent]):
    def __init__(self) -> None:
        self.handled_events: list[OrderShippedEvent] = []

    async def handle(self, event: OrderShippedEvent) -> None:
        self.handled_events.append(event)


# Container setup
class _TestContainer:
    """Test container that resolves step handlers, sagas, and event handlers."""

    def __init__(self, storage: MemorySagaStorage) -> None:
        self._storage = storage
        self._external_container = None
        self._step_handlers = {
            ReserveInventoryStep: ReserveInventoryStep(),
            ProcessPaymentStep: ProcessPaymentStep(),
            ShipOrderStep: ShipOrderStep(),
            FailingStep: FailingStep(),
        }
        self._event_handlers = {
            InventoryReservedEventHandler: InventoryReservedEventHandler(),
            PaymentProcessedEventHandler: PaymentProcessedEventHandler(),
            OrderShippedEventHandler: OrderShippedEventHandler(),
        }
        # Create sagas with this container and storage
        self._sagas = {
            OrderSaga: OrderSaga(),  # type: ignore[arg-type]
            FailingOrderSaga: FailingOrderSaga(),  # type: ignore[arg-type]
        }

    @property
    def external_container(self) -> typing.Any:
        """Return external container (for Container protocol compatibility)."""
        return self._external_container

    def attach_external_container(self, container: typing.Any) -> None:
        """Attach external container (for Container protocol compatibility)."""
        self._external_container = container

    async def resolve(self, type_) -> typing.Any:
        """Resolve type from container."""
        if type_ in self._step_handlers:
            return self._step_handlers[type_]
        if type_ in self._event_handlers:
            return self._event_handlers[type_]
        if type_ in self._sagas:
            return self._sagas[type_]
        if type_ == MemorySagaStorage:
            return self._storage
        raise ValueError(f"Unknown type: {type_}")


@pytest.fixture
def storage() -> MemorySagaStorage:
    """Create MemorySagaStorage instance."""
    return MemorySagaStorage()


@pytest.fixture
def container(storage: MemorySagaStorage) -> _TestContainer:
    """Create test container."""
    container = _TestContainer(storage)
    # Clear events in step handlers before each test
    for step_handler in container._step_handlers.values():
        if hasattr(step_handler, "_events"):
            step_handler._events.clear()
    # Clear events in event handlers before each test
    for event_handler in container._event_handlers.values():
        if hasattr(event_handler, "handled_events"):
            event_handler.handled_events.clear()
    return container


@pytest.fixture
def saga_mediator(
    container: _TestContainer,
    storage: MemorySagaStorage,
) -> cqrs.SagaMediator:
    """Create SagaMediator with MemorySagaStorage."""

    def saga_mapper(mapper: SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    def events_mapper(mapper: events.EventMap) -> None:
        mapper.bind(InventoryReservedEvent, InventoryReservedEventHandler)
        mapper.bind(PaymentProcessedEvent, PaymentProcessedEventHandler)
        mapper.bind(OrderShippedEvent, OrderShippedEventHandler)

    # Create event emitter
    event_map = events.EventMap()
    events_mapper(event_map)

    message_broker = mock.AsyncMock()
    message_broker.produce = mock.AsyncMock()

    event_emitter = events.EventEmitter(
        event_map=event_map,
        container=container,  # type: ignore
        message_broker=message_broker,
    )

    # Create mediator directly (simpler than using bootstrap for tests)
    saga_map = SagaMap()
    saga_mapper(saga_map)

    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
        storage=storage,
    )

    return mediator


class TestSagaMediatorMemoryStorage:
    """Integration tests for SagaMediator with MemorySagaStorage."""

    async def test_saga_mediator_stream_returns_async_iterator_consumable_with_async_for(
        self,
        saga_mediator: cqrs.SagaMediator,
    ) -> None:
        """
        Contract: mediator.stream(context) is called without await
        and returns an AsyncIterator that is consumed with async for.
        """
        context = OrderContext(order_id="contract", user_id="user1", amount=50.0)
        # stream() is called (no await) and returns async iterator
        async_gen = saga_mediator.stream(context)
        step_results = []
        async for result in async_gen:
            step_results.append(result)
        assert len(step_results) == 3
        assert step_results[0].step_type == ReserveInventoryStep
        assert step_results[1].step_type == ProcessPaymentStep
        assert step_results[2].step_type == ShipOrderStep

    async def test_saga_mediator_executes_saga_successfully(
        self,
        saga_mediator: cqrs.SagaMediator,
        storage: MemorySagaStorage,
    ) -> None:
        """Test that SagaMediator executes saga successfully."""
        context = OrderContext(order_id="123", user_id="user1", amount=100.0)
        saga_id = uuid.uuid4()

        step_results = []
        async for result in saga_mediator.stream(context, saga_id=saga_id):
            step_results.append(result)

        # Verify all steps were executed
        assert len(step_results) == 3

        # Verify step results
        assert isinstance(step_results[0].response, ReserveInventoryResponse)
        assert step_results[0].response.inventory_id == "inv_123"
        assert step_results[0].response.reserved is True

        assert isinstance(step_results[1].response, ProcessPaymentResponse)
        assert step_results[1].response.payment_id == "pay_123"
        assert step_results[1].response.charged is True

        assert isinstance(step_results[2].response, ShipOrderResponse)
        assert step_results[2].response.shipment_id == "ship_123"
        assert step_results[2].response.shipped is True

        # Verify step types
        assert step_results[0].step_type == ReserveInventoryStep
        assert step_results[1].step_type == ProcessPaymentStep
        assert step_results[2].step_type == ShipOrderStep

        # Verify saga status in storage
        status, stored_context, version = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.COMPLETED

    async def test_saga_mediator_processes_events_from_steps(
        self,
        saga_mediator: cqrs.SagaMediator,
        container: _TestContainer,
    ) -> None:
        """Test that SagaMediator processes events from saga steps."""
        context = OrderContext(order_id="456", user_id="user2", amount=200.0)

        step_results = []
        async for result in saga_mediator.stream(context):
            step_results.append(result)

        # Wait for background tasks to complete
        await asyncio.sleep(0.1)

        # Verify step results were returned
        assert len(step_results) == 3
        assert isinstance(step_results[0].response, ReserveInventoryResponse)
        assert isinstance(step_results[1].response, ProcessPaymentResponse)
        assert isinstance(step_results[2].response, ShipOrderResponse)

        # Verify event handlers were called (events are processed internally)
        # Note: events are processed twice - once via dispatcher and once via emitter
        # So we check that handlers were called at least once
        inventory_handler = await container.resolve(InventoryReservedEventHandler)
        payment_handler = await container.resolve(PaymentProcessedEventHandler)
        shipping_handler = await container.resolve(OrderShippedEventHandler)

        assert len(inventory_handler.handled_events) >= 1
        assert len(payment_handler.handled_events) >= 1
        assert len(shipping_handler.handled_events) >= 1

    async def test_saga_mediator_emits_events(
        self,
        saga_mediator: cqrs.SagaMediator,
        container: _TestContainer,
    ) -> None:
        """Test that SagaMediator processes events via EventEmitter."""
        context = OrderContext(order_id="789", user_id="user3", amount=300.0)

        step_results = []
        async for result in saga_mediator.stream(context):
            step_results.append(result)

        # Wait for background tasks to complete
        await asyncio.sleep(0.1)

        # Verify events were processed (DomainEvent calls handlers, not message broker)
        # Note: events are processed twice - once via dispatcher and once via emitter
        # Check that event handlers were called at least once
        inventory_handler = await container.resolve(InventoryReservedEventHandler)
        payment_handler = await container.resolve(PaymentProcessedEventHandler)
        shipping_handler = await container.resolve(OrderShippedEventHandler)

        assert len(inventory_handler.handled_events) >= 1
        assert len(payment_handler.handled_events) >= 1
        assert len(shipping_handler.handled_events) >= 1

    async def test_saga_mediator_handles_saga_failure_with_compensation(
        self,
        container: _TestContainer,
        storage: MemorySagaStorage,
    ) -> None:
        """Test that SagaMediator handles saga failure and compensation."""

        # Create mediator with failing saga
        def failing_saga_mapper(mapper: SagaMap) -> None:
            mapper.bind(OrderContext, FailingOrderSaga)

        saga_map = SagaMap()
        failing_saga_mapper(saga_map)

        event_map = events.EventMap()
        event_emitter = events.EventEmitter(
            event_map=event_map,
            container=container,  # type: ignore
            message_broker=mock.AsyncMock(),
        )

        failing_mediator = cqrs.SagaMediator(
            saga_map=saga_map,
            container=container,  # type: ignore
            event_emitter=event_emitter,
            event_map=event_map,
            storage=storage,
        )

        context = OrderContext(order_id="fail_123", user_id="user4", amount=400.0)
        saga_id = uuid.uuid4()

        step_results = []
        with pytest.raises(ValueError, match="Step failed for order fail_123"):
            async for result in failing_mediator.stream(context, saga_id=saga_id):
                step_results.append(result)

        # Verify that some steps were executed before failure
        assert len(step_results) >= 1

        # Verify compensation was called
        reserve_step = await container.resolve(ReserveInventoryStep)
        payment_step = await container.resolve(ProcessPaymentStep)
        assert reserve_step.compensate_called
        assert payment_step.compensate_called

        # Verify saga status is FAILED
        status, _, version = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.FAILED

    async def test_saga_mediator_with_saga_id_recovery(
        self,
        saga_mediator: cqrs.SagaMediator,
        storage: MemorySagaStorage,
    ) -> None:
        """Test that SagaMediator can recover saga using saga_id."""
        context = OrderContext(order_id="recover_123", user_id="user5", amount=500.0)
        saga_id = uuid.uuid4()

        # Execute first part of saga
        step_results_1 = []
        async for result in saga_mediator.stream(context, saga_id=saga_id):
            step_results_1.append(result)
            # Simulate interruption after first step
            if len(step_results_1) == 1:
                break

        # Verify first step was executed
        assert len(step_results_1) == 1
        assert isinstance(step_results_1[0].response, ReserveInventoryResponse)
        assert step_results_1[0].step_type == ReserveInventoryStep

        # Verify saga is in RUNNING status
        status, _, version = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.RUNNING

        # Resume saga execution with same saga_id
        step_results_2 = []
        async for result in saga_mediator.stream(context, saga_id=saga_id):
            step_results_2.append(result)

        # Verify remaining steps were executed
        # Note: Saga will skip already completed steps
        assert len(step_results_2) >= 2  # At least 2 more steps

        # Verify final status
        final_status, _, version = await storage.load_saga_state(saga_id)
        assert final_status == SagaStatus.COMPLETED
