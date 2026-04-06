"""Shared fixtures and test classes for saga tests."""

import dataclasses
import typing

import pytest

from cqrs.events.event import Event
from cqrs.response import Response
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.memory import MemorySagaStorage


# Test context and response models
@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str
    user_id: str
    amount: float


class ReserveInventoryResponse(Response):
    inventory_id: str
    reserved: bool


class ProcessPaymentResponse(Response):
    payment_id: str
    charged: bool


class ShipOrderResponse(Response):
    shipment_id: str
    shipped: bool


# Test step handlers
class ReserveInventoryStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    def __init__(self) -> None:
        self._events: list[Event] = []
        self.act_called = False
        self.compensate_called = False
        self._inventory_id: typing.Optional[str] = None

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        self.act_called = True
        self._inventory_id = f"inv_{context.order_id}"
        response = ReserveInventoryResponse(
            inventory_id=self._inventory_id,
            reserved=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        self.compensate_called = True
        self._inventory_id = None


class ProcessPaymentStep(SagaStepHandler[OrderContext, ProcessPaymentResponse]):
    def __init__(self) -> None:
        self._events: list[Event] = []
        self.act_called = False
        self.compensate_called = False
        self._payment_id: typing.Optional[str] = None

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ProcessPaymentResponse]:
        self.act_called = True
        self._payment_id = f"pay_{context.order_id}"
        response = ProcessPaymentResponse(payment_id=self._payment_id, charged=True)
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        self.compensate_called = True
        self._payment_id = None


class ShipOrderStep(SagaStepHandler[OrderContext, ShipOrderResponse]):
    def __init__(self) -> None:
        self._events: list[Event] = []
        self.act_called = False
        self.compensate_called = False
        self._shipment_id: typing.Optional[str] = None

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ShipOrderResponse]:
        self.act_called = True
        self._shipment_id = f"ship_{context.order_id}"
        response = ShipOrderResponse(shipment_id=self._shipment_id, shipped=True)
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        self.compensate_called = True
        self._shipment_id = None


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


class CompensationFailingStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    """Step that succeeds but compensation fails."""

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
        response = ReserveInventoryResponse(inventory_id="inv_123", reserved=True)
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        self.compensate_called = True
        raise RuntimeError("Compensation failed")


class CompensationRetryStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    """Step that fails compensation a few times, then succeeds."""

    def __init__(self, fail_count: int = 2) -> None:
        self._events: list[Event] = []
        self.act_called = False
        self.compensate_called_count = 0
        self.fail_count = fail_count

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        self.act_called = True
        response = ReserveInventoryResponse(inventory_id="inv_123", reserved=True)
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        self.compensate_called_count += 1
        if self.compensate_called_count <= self.fail_count:
            raise RuntimeError(
                f"Compensation failed (attempt {self.compensate_called_count})",
            )


class AlwaysFailingCompensationStep(
    SagaStepHandler[OrderContext, ReserveInventoryResponse],
):
    """Step that always fails compensation."""

    def __init__(self) -> None:
        self._events: list[Event] = []
        self.act_called = False
        self.compensate_called_count = 0

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        self.act_called = True
        response = ReserveInventoryResponse(inventory_id="inv_123", reserved=True)
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        self.compensate_called_count += 1
        raise RuntimeError(
            f"Compensation always fails (attempt {self.compensate_called_count})",
        )


# Mock container
class SagaContainer:
    def __init__(self) -> None:
        self._handlers: dict[type, SagaStepHandler] = {}
        self._external_container: typing.Any = None

    def register(self, handler_type: type, handler: SagaStepHandler) -> None:
        self._handlers[handler_type] = handler

    @property
    def external_container(self) -> typing.Any:
        return self._external_container

    def attach_external_container(self, container: typing.Any) -> None:
        self._external_container = container

    async def resolve(
        self,
        type_: typing.Type[typing.Any],
    ) -> typing.Any:
        handler_type = type_
        if handler_type not in self._handlers:
            # Create a new instance if not registered
            handler = handler_type()
            self._handlers[handler_type] = handler
            return handler
        return self._handlers[handler_type]


@pytest.fixture
def saga_container() -> SagaContainer:
    return SagaContainer()


@pytest.fixture
def storage() -> MemorySagaStorage:
    """Create memory storage for tests."""
    return MemorySagaStorage()


@pytest.fixture
def successful_saga(saga_container: SagaContainer) -> Saga[OrderContext]:
    class SuccessfulSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]

    return SuccessfulSaga()
