"""Benchmarks for Saga with memory storage (dataclass DCResponse).

- Benchmarks named *_run_* use the scoped run path (create_run, checkpoint commits).
- Benchmarks named *_legacy_* use the legacy path (no create_run, commit per storage call).
"""

import asyncio
import dataclasses
import typing

import pytest
from cqrs.models.event import Event
from cqrs.models.response import DCResponse
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


@dataclasses.dataclass
class ReserveInventoryResponse(DCResponse):
    inventory_id: str
    reserved: bool


@dataclasses.dataclass
class ProcessPaymentResponse(DCResponse):
    payment_id: str
    charged: bool


@dataclasses.dataclass
class ShipOrderResponse(DCResponse):
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
    Provide a fresh in-memory saga storage instance for tests and benchmarks.

    Returns:
        MemorySagaStorage: A new MemorySagaStorage instance.
    """
    return MemorySagaStorage()


@pytest.fixture
def memory_storage_legacy() -> MemorySagaStorageLegacy:
    """
    Provide a legacy in-memory saga storage that does not support scoped runs.

    Returns:
        MemorySagaStorageLegacy: An in-memory saga storage whose `create_run` is disabled (raises `NotImplementedError`) for legacy-path benchmarks.
    """
    return MemorySagaStorageLegacy()


@pytest.fixture
def saga_with_memory_storage() -> Saga[OrderContext]:
    """
    Create an OrderSaga configured with reserve-inventory, process-payment, and ship-order steps.

    Returns a Saga subclass instance with the three ordered step handlers: ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep. No fixture dependencies.
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
        Execute a full saga transaction using the module's memory-backed saga, advancing through every step.

        Creates an OrderContext with order_id "ord_1", user_id "user_1", and amount 100.0, opens a transaction using the provided saga container and memory storage, and iterates the transaction to exercise each step in the run path.
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
        Execute 10 sequential saga transactions using a fresh in-memory storage and context for each iteration.

        Each iteration creates a new MemorySagaStorage and OrderContext, opens a saga transaction with the provided container and storage, and iterates through the transaction steps without performing additional work.
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
        Run a full-order saga transaction against the legacy in-memory storage used for benchmarks.

        Creates an OrderContext and executes the saga transaction using the provided saga container and legacy memory storage, iterating the transaction to completion.
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
        Run a saga transaction using the legacy memory storage and iterate its steps.

        Enters a transaction for an OrderContext (order_id "ord_1") with the registered saga_container and memory_storage_legacy, then iterates through the transaction steps without performing work for each step.
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
        Execute ten sequential saga transactions using MemorySagaStorageLegacy.

        Each iteration creates a new MemorySagaStorageLegacy and an OrderContext (with distinct order_id, user_id, and amount) and runs the configured saga transaction to completion by iterating through its steps.
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
