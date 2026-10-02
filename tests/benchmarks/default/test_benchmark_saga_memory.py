"""Benchmarks for Saga with memory storage (default Response).

- Benchmarks named *_run_* use the scoped run path (create_run, checkpoint commits).
- Benchmarks named *_legacy_* use the legacy path (no create_run, commit per storage call).
"""

import asyncio
import dataclasses
import typing

import pytest
from cqrs.models.event import Event
from cqrs.models.response import Response
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.handlers.saga import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.memory import MemorySagaStorage

from ..conftest import MemorySagaStorageLegacy


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


class ReserveInventoryStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    def __init__(self) -> None:
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        response = ReserveInventoryResponse(
            inventory_id=f"inv_{context.order_id}",
            reserved=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        pass


class ProcessPaymentStep(SagaStepHandler[OrderContext, ProcessPaymentResponse]):
    def __init__(self) -> None:
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ProcessPaymentResponse]:
        response = ProcessPaymentResponse(
            payment_id=f"pay_{context.order_id}",
            charged=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        pass


class ShipOrderStep(SagaStepHandler[OrderContext, ShipOrderResponse]):
    def __init__(self) -> None:
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ShipOrderResponse]:
        response = ShipOrderResponse(
            shipment_id=f"ship_{context.order_id}",
            shipped=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        pass


class SagaContainer:
    """Simple container that resolves saga step handlers."""

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

    async def resolve(self, type_: type) -> typing.Any:
        if type_ not in self._handlers:
            self._handlers[type_] = type_()
        return self._handlers[type_]


@pytest.fixture
def saga_container() -> SagaContainer:
    container = SagaContainer()
    container.register(ReserveInventoryStep, ReserveInventoryStep())
    container.register(ProcessPaymentStep, ProcessPaymentStep())
    container.register(ShipOrderStep, ShipOrderStep())
    return container


@pytest.fixture
def memory_storage() -> MemorySagaStorage:
    """
    Create a fresh in-memory saga storage instance for tests.

    Returns:
        MemorySagaStorage: A new MemorySagaStorage used to persist saga state in memory.
    """
    return MemorySagaStorage()


@pytest.fixture
def memory_storage_legacy() -> MemorySagaStorageLegacy:
    """
    Create a MemorySagaStorageLegacy instance for legacy-path benchmarks.

    Returns:
        MemorySagaStorageLegacy: A storage instance where `create_run()` is disabled and will raise NotImplementedError if called.
    """
    return MemorySagaStorageLegacy()


@pytest.fixture
def saga_with_memory_storage() -> Saga[OrderContext]:
    """
    Create an OrderSaga preconfigured with inventory reservation, payment processing, and shipping steps.

    Returns:
        Saga[OrderContext]: An instance configured with ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep.
    """

    class OrderSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]

    return OrderSaga()


@pytest.mark.benchmark
def test_benchmark_saga_memory_run_full_transaction(
    benchmark,
    saga_with_memory_storage: Saga[OrderContext],
    saga_container: SagaContainer,
    memory_storage: MemorySagaStorage,
):
    """Benchmark full saga transaction with memory storage, scoped run (3 steps)."""

    async def run() -> None:
        """
        Execute a full three-step OrderSaga transaction using the memory storage scoped-run path.

        Creates an OrderContext and runs the saga transaction to completion with the provided saga container and memory storage.
        """
        context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)
        async with saga_with_memory_storage.transaction(
            context=context,
            container=saga_container,
            storage=memory_storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_saga_memory_run_single_step(
    benchmark,
    saga_with_memory_storage: Saga[OrderContext],
    saga_container: SagaContainer,
    memory_storage: MemorySagaStorage,
):
    """Benchmark saga with single step, scoped run (memory storage)."""

    class SingleStepSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = SingleStepSaga()

    async def run() -> None:
        context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)
        async with saga.transaction(
            context=context,
            container=saga_container,
            storage=memory_storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_saga_memory_run_ten_transactions(
    benchmark,
    saga_with_memory_storage: Saga[OrderContext],
    saga_container: SagaContainer,
):
    """Benchmark 10 saga transactions in sequence, scoped run (memory storage)."""

    async def run() -> None:
        """
        Run ten sequential saga transactions, each using a new MemorySagaStorage and an OrderContext.

        Each iteration (i from 0 to 9) creates:
        - a fresh MemorySagaStorage,
        - an OrderContext with order_id "ord_i", user_id "user_i", and amount 100.0 + i,
        then opens a transaction from `saga_with_memory_storage` with `saga_container` and the storage and iterates the transaction to completion.
        """
        for i in range(10):
            storage = MemorySagaStorage()
            context = OrderContext(
                order_id=f"ord_{i}",
                user_id=f"user_{i}",
                amount=100.0 + i,
            )
            async with saga_with_memory_storage.transaction(
                context=context,
                container=saga_container,
                storage=storage,
            ) as transaction:
                async for _ in transaction:
                    pass

    benchmark(lambda: asyncio.run(run()))


# ---- Legacy path (no create_run, commit per storage call) ----


@pytest.mark.benchmark
def test_benchmark_saga_memory_legacy_full_transaction(
    benchmark,
    saga_with_memory_storage: Saga[OrderContext],
    saga_container: SagaContainer,
    memory_storage_legacy: MemorySagaStorageLegacy,
):
    """Benchmark full saga transaction with memory storage, legacy path (3 steps)."""

    async def run() -> None:
        """
        Execute a full OrderSaga transaction using the legacy memory storage path.

        Builds an OrderContext (order_id "ord_1", user_id "user_1", amount 100.0) and runs the saga_with_memory_storage transaction with saga_container and memory_storage_legacy, iterating the transaction to completion.
        """
        context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)
        async with saga_with_memory_storage.transaction(
            context=context,
            container=saga_container,
            storage=memory_storage_legacy,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_saga_memory_legacy_single_step(
    benchmark,
    saga_with_memory_storage: Saga[OrderContext],
    saga_container: SagaContainer,
    memory_storage_legacy: MemorySagaStorageLegacy,
):
    """Benchmark saga with single step, legacy path (memory storage)."""

    class SingleStepSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = SingleStepSaga()

    async def run() -> None:
        """
        Runs a full OrderSaga transaction using the legacy memory storage path.

        This coroutine executes the saga with an OrderContext and the MemorySagaStorageLegacy instance so the saga proceeds through all steps while exercising the legacy storage behavior (create_run disabled, commit-per-call path).
        """
        context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)
        async with saga.transaction(
            context=context,
            container=saga_container,
            storage=memory_storage_legacy,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_saga_memory_legacy_ten_transactions(
    benchmark,
    saga_with_memory_storage: Saga[OrderContext],
    saga_container: SagaContainer,
):
    """Benchmark 10 saga transactions in sequence, legacy path (memory storage)."""

    async def run() -> None:
        """
        Run ten sequential saga transactions using the legacy memory storage path.

        For each iteration this function creates a new MemorySagaStorageLegacy, constructs an OrderContext with a unique order_id and user_id and increasing amount, opens a saga transaction using the shared saga_with_memory_storage and saga_container, and iterates the transaction to completion.
        """
        for i in range(10):
            storage = MemorySagaStorageLegacy()
            context = OrderContext(
                order_id=f"ord_{i}",
                user_id=f"user_{i}",
                amount=100.0 + i,
            )
            async with saga_with_memory_storage.transaction(
                context=context,
                container=saga_container,
                storage=storage,
            ) as transaction:
                async for _ in transaction:
                    pass

    benchmark(lambda: asyncio.run(run()))
