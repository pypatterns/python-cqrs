"""
Saga step, compensation, fallback, open circuit breaker, and recovery.

Run:
   python examples/testing/saga.py
"""

import asyncio
import dataclasses
import typing

import di

import cqrs
import cqrs.saga.fallback
from cqrs.saga.models import SagaContext
from cqrs.saga.storage import SagaStatus, SagaStepStatus
from cqrs.testing import test_harness


class CardDeclined(Exception):
    pass


class SagaCircuitOpen(Exception):
    pass


class OpenSagaBreaker:
    async def call(self, identifier, func, *args, **kwargs):
        raise SagaCircuitOpen()

    def is_circuit_breaker_error(self, exc: Exception) -> bool:
        return isinstance(exc, SagaCircuitOpen)


@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str


def _success_step(name: str):
    class Step(cqrs.SagaStepHandler[OrderContext, None]):
        async def act(self, context: OrderContext):
            return self._generate_step_result(None)

        async def compensate(self, context: OrderContext) -> None:
            return None

    Step.__name__ = name
    Step.__qualname__ = name
    return Step


async def saga_step() -> None:
    ReserveStock = _success_step("ReserveStock")
    ChargeCard = _success_step("ChargeCard")

    class OrderSaga(cqrs.Saga[OrderContext]):
        steps = [ReserveStock, ChargeCard]

    def sagas_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    container = di.Container()
    async with test_harness(container, sagas_mapper=sagas_mapper) as harness:
        async for step in harness.saga_mediator.execute(OrderContext(order_id="1")):
            if step.step_type is ReserveStock:
                assert harness.sagas.running_count == 1
                assert harness.invocations.of(ReserveStock)[0].method == "act"

        assert harness.sagas.running_count == 0
        assert harness.sagas.completed_count == 1
        saga = harness.sagas.one(OrderSaga)
        assert saga.completed_steps == ["ReserveStock", "ChargeCard"]


async def compensation() -> None:
    ReserveStock = _success_step("ReserveStock")

    class ChargeCard(cqrs.SagaStepHandler[OrderContext, None]):
        async def act(self, context: OrderContext):
            raise CardDeclined("card declined")

        async def compensate(self, context: OrderContext) -> None:
            return None

    class OrderSaga(cqrs.Saga[OrderContext]):
        steps = [ReserveStock, ChargeCard]

    def sagas_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    container = di.Container()
    async with test_harness(container, sagas_mapper=sagas_mapper) as harness:
        try:
            async for step in harness.saga_mediator.execute(OrderContext(order_id="1")):
                pass
        except CardDeclined:
            pass

        assert harness.invocations.of(ReserveStock)[-1].method == "compensate"
        saga = harness.sagas.one(OrderSaga)
        assert saga.status is SagaStatus.FAILED
        assert saga.step_status("ReserveStock") is SagaStepStatus.COMPENSATED


async def saga_fallback() -> None:
    class ReserveStock(cqrs.SagaStepHandler[OrderContext, None]):
        async def act(self, context: OrderContext):
            raise RuntimeError("stock down")

        async def compensate(self, context: OrderContext) -> None:
            return None

    ReserveStockFallback = _success_step("ReserveStockFallback")
    ChargeCard = _success_step("ChargeCard")

    class OrderSaga(cqrs.Saga[OrderContext]):
        steps = [
            cqrs.saga.fallback.Fallback(step=ReserveStock, fallback=ReserveStockFallback),
            ChargeCard,
        ]

    def sagas_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    container = di.Container()
    async with test_harness(container, sagas_mapper=sagas_mapper) as harness:
        async for step in harness.saga_mediator.execute(OrderContext(order_id="1")):
            pass

        harness.invocations.assert_entered(ReserveStock)
        harness.invocations.assert_entered(ReserveStockFallback)
        assert harness.invocations.of(ReserveStock)[-1].method == "act"
        assert harness.sagas.one(OrderSaga).status is SagaStatus.COMPLETED


async def saga_circuit_breaker() -> None:
    class ReserveStock(cqrs.SagaStepHandler[OrderContext, None]):
        async def act(self, context: OrderContext):
            raise RuntimeError("stock down")

        async def compensate(self, context: OrderContext) -> None:
            return None

    ReserveStockFallback = _success_step("ReserveStockFallback")

    class OrderSaga(cqrs.Saga[OrderContext]):
        steps: typing.ClassVar[list[type[cqrs.SagaStepHandler] | cqrs.saga.fallback.Fallback]] = [
            cqrs.saga.fallback.Fallback(
                step=ReserveStock,
                fallback=ReserveStockFallback,
                circuit_breaker=OpenSagaBreaker(),
            ),
        ]

    def sagas_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    container = di.Container()
    async with test_harness(container, sagas_mapper=sagas_mapper) as harness:
        async for step in harness.saga_mediator.execute(OrderContext(order_id="1")):
            pass

        harness.invocations.assert_not_entered(ReserveStock)
        harness.invocations.assert_entered(ReserveStockFallback)


async def recovery() -> None:
    ReserveStock = _success_step("ReserveStock")
    ChargeCard = _success_step("ChargeCard")

    class OrderSaga(cqrs.Saga[OrderContext]):
        steps = [ReserveStock, ChargeCard]

    def sagas_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    container = di.Container()
    async with test_harness(container, sagas_mapper=sagas_mapper) as harness:
        async for step in harness.saga_mediator.execute(OrderContext(order_id="1")):
            if step.step_type is ReserveStock:
                break

        saga_id = harness.sagas.one(OrderSaga).saga_id
        await harness.sagas.recover(OrderSaga(), saga_id, OrderContext)
        acts = [item for item in harness.invocations.of(ReserveStock) if item.method == "act"]
        assert len(acts) == 1
        assert harness.sagas.one(OrderSaga).status is SagaStatus.COMPLETED


async def main() -> None:
    await saga_step()
    await compensation()
    await saga_fallback()
    await saga_circuit_breaker()
    await recovery()


if __name__ == "__main__":
    asyncio.run(main())
