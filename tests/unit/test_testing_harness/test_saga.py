import dataclasses

import pytest

import cqrs
from cqrs.saga.fallback import Fallback
from cqrs.saga.models import SagaContext
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.testing import TestContainer, test_harness


@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str


class ReserveStock(cqrs.SagaStepHandler[OrderContext, None]):
    async def act(self, context: OrderContext):
        return self._generate_step_result(None)

    async def compensate(self, context: OrderContext) -> None:
        return None


class ChargeCard(cqrs.SagaStepHandler[OrderContext, None]):
    async def act(self, context: OrderContext):
        return self._generate_step_result(None)

    async def compensate(self, context: OrderContext) -> None:
        return None


class FailingCharge(cqrs.SagaStepHandler[OrderContext, None]):
    async def act(self, context: OrderContext):
        raise RuntimeError("charge failed")

    async def compensate(self, context: OrderContext) -> None:
        return None


class FailingReserve(cqrs.SagaStepHandler[OrderContext, None]):
    async def act(self, context: OrderContext):
        raise RuntimeError("reserve failed")

    async def compensate(self, context: OrderContext) -> None:
        return None


class ReserveStockFallback(cqrs.SagaStepHandler[OrderContext, None]):
    async def act(self, context: OrderContext):
        return self._generate_step_result(None)

    async def compensate(self, context: OrderContext) -> None:
        return None


class CircuitOpen(Exception):
    pass


class OpenSagaBreaker:
    async def call(self, identifier, func, *args, **kwargs):
        raise CircuitOpen()

    def is_circuit_breaker_error(self, exc: Exception) -> bool:
        return isinstance(exc, CircuitOpen)


async def test_saga_step_is_running_then_completed() -> None:
    class OrderSaga(cqrs.Saga[OrderContext]):
        steps = [ReserveStock, ChargeCard]

    def sagas_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    async with test_harness(TestContainer(), sagas_mapper=sagas_mapper) as harness:
        assert harness.saga_mediator is not None
        assert harness.saga_mediator._dispatcher._compensation_retry_delay == 0
        async for step in harness.saga_mediator.execute(OrderContext(order_id="1")):
            if step.step_type is ReserveStock:
                assert harness.sagas.running_count == 1
                assert harness.invocations.of(ReserveStock)[0].method == "act"
        assert harness.sagas.running_count == 0
        assert harness.sagas.completed_count == 1
        saga = harness.sagas.one(OrderSaga)
        assert saga.completed_steps == ["ReserveStock", "ChargeCard"]
        assert saga.status is SagaStatus.COMPLETED


async def test_saga_compensation_fails_the_saga_and_compensates_the_step() -> None:
    class OrderSaga(cqrs.Saga[OrderContext]):
        steps = [ReserveStock, FailingCharge]

    def sagas_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    async with test_harness(TestContainer(), sagas_mapper=sagas_mapper) as harness:
        assert harness.saga_mediator is not None
        with pytest.raises(RuntimeError, match="charge failed"):
            async for _step in harness.saga_mediator.execute(OrderContext(order_id="1")):
                pass
        assert harness.invocations.of(ReserveStock)[-1].method == "compensate"
        saga = harness.sagas.one(OrderSaga)
        assert saga.status is SagaStatus.FAILED
        assert saga.status is not SagaStatus.COMPENSATING
        assert saga.step_status("ReserveStock") is SagaStepStatus.COMPENSATED


async def test_saga_fallback_replaces_a_failed_act() -> None:
    class OrderSaga(cqrs.Saga[OrderContext]):
        steps = [
            Fallback(step=FailingReserve, fallback=ReserveStockFallback),
            ChargeCard,
        ]

    def sagas_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    async with test_harness(TestContainer(), sagas_mapper=sagas_mapper) as harness:
        assert harness.saga_mediator is not None
        async for _step in harness.saga_mediator.execute(OrderContext(order_id="1")):
            pass
        harness.invocations.assert_entered(FailingReserve)
        harness.invocations.assert_entered(ReserveStockFallback)
        assert harness.invocations.of(FailingReserve)[-1].method == "act"
        assert harness.sagas.one(OrderSaga).status is SagaStatus.COMPLETED


async def test_saga_circuit_breaker_skips_the_primary_step() -> None:
    class OrderSaga(cqrs.Saga[OrderContext]):
        steps = [
            Fallback(
                step=ReserveStock,
                fallback=ReserveStockFallback,
                circuit_breaker=OpenSagaBreaker(),
            ),
        ]

    def sagas_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    async with test_harness(TestContainer(), sagas_mapper=sagas_mapper) as harness:
        assert harness.saga_mediator is not None
        async for _step in harness.saga_mediator.execute(OrderContext(order_id="1")):
            pass
        harness.invocations.assert_not_entered(ReserveStock)
        harness.invocations.assert_entered(ReserveStockFallback)


async def test_saga_recovery_does_not_act_a_completed_step_again() -> None:
    class OrderSaga(cqrs.Saga[OrderContext]):
        steps = [ReserveStock, ChargeCard]

    def sagas_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    async with test_harness(TestContainer(), sagas_mapper=sagas_mapper) as harness:
        assert harness.saga_mediator is not None
        async for _step in harness.saga_mediator.execute(OrderContext(order_id="1")):
            pass
        saga_id = harness.sagas.one(OrderSaga).saga_id
        await harness.sagas.recover(OrderSaga(), saga_id, OrderContext)
        acts = [item for item in harness.invocations.of(ReserveStock) if item.method == "act"]
        assert len(acts) == 1
        assert harness.sagas.one(OrderSaga).status is SagaStatus.COMPLETED


async def test_saga_recovery_continues_compensation() -> None:
    class OrderSaga(cqrs.Saga[OrderContext]):
        steps = [ReserveStock, ChargeCard]

    def sagas_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    async with test_harness(TestContainer(), sagas_mapper=sagas_mapper) as harness:
        assert harness.saga_mediator is not None
        async for _step in harness.saga_mediator.execute(OrderContext(order_id="1")):
            pass
        saga_id = harness.sagas.one(OrderSaga).saga_id
        await harness.saga_storage.update_status(saga_id, SagaStatus.COMPENSATING)
        with pytest.raises(RuntimeError, match="recovered in"):
            await harness.sagas.recover(OrderSaga(), saga_id, OrderContext)
        compensations = [item for item in harness.invocations.of(ReserveStock) if item.method == "compensate"]
        acts = [item for item in harness.invocations.of(ReserveStock) if item.method == "act"]
        assert len(acts) == 1
        assert len(compensations) == 1
        assert harness.sagas.one(OrderSaga).status is SagaStatus.FAILED
        assert harness.sagas.one(OrderSaga).step_status("ReserveStock") is SagaStepStatus.COMPENSATED
