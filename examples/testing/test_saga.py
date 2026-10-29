"""
Saga step, compensation, fallback, open circuit breaker, and recovery.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_saga.py
"""

import dataclasses
import typing

import di
import pytest

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


def _mapper(saga: type[cqrs.Saga[OrderContext]]):
    def sagas_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, saga)

    return sagas_mapper


async def test_saga_step_acts_and_the_saga_completes() -> None:
    ReserveStock = _success_step("ReserveStock")
    ChargeCard = _success_step("ChargeCard")

    class OrderSaga(cqrs.Saga[OrderContext]):
        steps = [ReserveStock, ChargeCard]

    container = di.Container()
    async with test_harness(container, sagas_mapper=_mapper(OrderSaga)) as harness:
        running_after_reserve = None
        async for step in harness.saga_mediator.execute(OrderContext(order_id="1")):
            if step.step_type is ReserveStock:
                running_after_reserve = harness.sagas.running_count

        assert running_after_reserve == 1
        assert harness.invocations.of(ReserveStock)[0].method == "act"
        assert harness.sagas.running_count == 0
        assert harness.sagas.completed_count == 1
        assert harness.sagas.one(OrderSaga).completed_steps == ["ReserveStock", "ChargeCard"]


async def test_failed_step_compensates_finished_steps() -> None:
    ReserveStock = _success_step("ReserveStock")

    class ChargeCard(cqrs.SagaStepHandler[OrderContext, None]):
        async def act(self, context: OrderContext):
            raise CardDeclined("card declined")

        async def compensate(self, context: OrderContext) -> None:
            return None

    class OrderSaga(cqrs.Saga[OrderContext]):
        steps = [ReserveStock, ChargeCard]

    container = di.Container()
    async with test_harness(container, sagas_mapper=_mapper(OrderSaga)) as harness:
        with pytest.raises(CardDeclined):
            async for _ in harness.saga_mediator.execute(OrderContext(order_id="1")):
                pass

        assert harness.invocations.of(ReserveStock)[-1].method == "compensate"
        saga = harness.sagas.one(OrderSaga)
        assert saga.status is SagaStatus.FAILED
        assert saga.step_status("ReserveStock") is SagaStepStatus.COMPENSATED


async def test_saga_fallback_replaces_the_failed_step() -> None:
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

    container = di.Container()
    async with test_harness(container, sagas_mapper=_mapper(OrderSaga)) as harness:
        async for _ in harness.saga_mediator.execute(OrderContext(order_id="1")):
            pass

        harness.invocations.assert_entered(ReserveStock)
        harness.invocations.assert_entered(ReserveStockFallback)
        assert harness.invocations.of(ReserveStock)[-1].method == "act"
        assert harness.sagas.one(OrderSaga).status is SagaStatus.COMPLETED


async def test_open_saga_breaker_skips_the_step() -> None:
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

    container = di.Container()
    async with test_harness(container, sagas_mapper=_mapper(OrderSaga)) as harness:
        async for _ in harness.saga_mediator.execute(OrderContext(order_id="1")):
            pass

        harness.invocations.assert_not_entered(ReserveStock)
        harness.invocations.assert_entered(ReserveStockFallback)


async def test_recovery_does_not_repeat_a_finished_step() -> None:
    ReserveStock = _success_step("ReserveStock")
    ChargeCard = _success_step("ChargeCard")

    class OrderSaga(cqrs.Saga[OrderContext]):
        steps = [ReserveStock, ChargeCard]

    container = di.Container()
    async with test_harness(container, sagas_mapper=_mapper(OrderSaga)) as harness:
        async for step in harness.saga_mediator.execute(OrderContext(order_id="1")):
            if step.step_type is ReserveStock:
                break
        saga_id = harness.sagas.one(OrderSaga).saga_id

        await harness.sagas.recover(OrderSaga(), saga_id, OrderContext)

        acts = [item for item in harness.invocations.of(ReserveStock) if item.method == "act"]
        assert len(acts) == 1
        assert harness.sagas.one(OrderSaga).status is SagaStatus.COMPLETED
