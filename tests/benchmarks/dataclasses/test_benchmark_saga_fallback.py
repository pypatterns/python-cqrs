"""Benchmarks for Saga with Fallback (dataclass DCResponse)."""

import asyncio

import pytest
from cqrs.adapters.circuit_breaker import AioBreakerAdapter
from cqrs.models.event import Event
from cqrs.saga.fallback import Fallback
from cqrs.saga.saga import Saga
from cqrs.handlers.saga import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.memory import MemorySagaStorage

from .test_benchmark_saga_memory import (
    OrderContext,
    ProcessPaymentStep,
    ReserveInventoryResponse,
    ReserveInventoryStep,
    SagaContainer,
    ShipOrderStep,
)


class FallbackReserveStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    """Fallback step used when primary fails (not used in happy-path benchmark)."""

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
            inventory_id=f"fallback_inv_{context.order_id}",
            reserved=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        pass


@pytest.fixture
def saga_container_fallback() -> SagaContainer:
    container = SagaContainer()
    container.register(ReserveInventoryStep, ReserveInventoryStep())
    container.register(FallbackReserveStep, FallbackReserveStep())
    container.register(ProcessPaymentStep, ProcessPaymentStep())
    container.register(ShipOrderStep, ShipOrderStep())
    return container


@pytest.fixture
def memory_storage() -> MemorySagaStorage:
    return MemorySagaStorage()


@pytest.mark.benchmark
def test_benchmark_saga_fallback_without_circuit_breaker(
    benchmark,
    saga_container_fallback: SagaContainer,
    memory_storage: MemorySagaStorage,
):
    """Benchmark saga with Fallback step (no circuit breaker). Primary step runs."""

    class SagaWithFallbackNoCB(Saga[OrderContext]):
        steps = [
            Fallback(
                step=ReserveInventoryStep,
                fallback=FallbackReserveStep,
                circuit_breaker=None,
            ),
            ProcessPaymentStep,
            ShipOrderStep,
        ]

    saga = SagaWithFallbackNoCB()

    async def run() -> None:
        context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)
        async with saga.transaction(
            context=context,
            container=saga_container_fallback,
            storage=memory_storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_saga_fallback_with_aiobreaker_adapter(
    benchmark,
    saga_container_fallback: SagaContainer,
    memory_storage: MemorySagaStorage,
):
    """Benchmark saga with Fallback step and AioBreakerAdapter circuit breaker."""

    circuit_breaker = AioBreakerAdapter(fail_max=5, timeout_duration=60)

    class SagaWithFallbackWithCB(Saga[OrderContext]):
        steps = [
            Fallback(
                step=ReserveInventoryStep,
                fallback=FallbackReserveStep,
                circuit_breaker=circuit_breaker,
            ),
            ProcessPaymentStep,
            ShipOrderStep,
        ]

    saga = SagaWithFallbackWithCB()

    async def run() -> None:
        context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)
        async with saga.transaction(
            context=context,
            container=saga_container_fallback,
            storage=memory_storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: asyncio.run(run()))
