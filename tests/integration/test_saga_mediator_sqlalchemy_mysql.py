"""Integration tests for SagaMediator with SqlAlchemySagaStorage (MySQL)."""

import uuid
from unittest import mock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import cqrs
from cqrs import events, SagaMap
from cqrs.saga.storage.enums import SagaStatus
from cqrs.saga.storage.sqlalchemy import SqlAlchemySagaStorage

from tests.integration.test_saga_mediator_memory import (
    _TestContainer,
    FailingOrderSaga,
    InventoryReservedEventHandler,
    OrderContext,
    OrderSaga,
    OrderShippedEventHandler,
    PaymentProcessedEventHandler,
    ProcessPaymentResponse,
    ProcessPaymentStep,
    ReserveInventoryResponse,
    ReserveInventoryStep,
    ShipOrderResponse,
    ShipOrderStep,
)


@pytest.fixture
def storage(
    saga_session_factory_mysql: async_sessionmaker[AsyncSession],
) -> SqlAlchemySagaStorage:
    """Create SqlAlchemySagaStorage instance (MySQL)."""
    return SqlAlchemySagaStorage(saga_session_factory_mysql)


class TestSagaMediatorSqlAlchemyStorageMysql:
    """Integration tests for SagaMediator with SqlAlchemySagaStorage (MySQL)."""

    async def test_saga_mediator_executes_saga_successfully(
        self,
        saga_mediator: cqrs.SagaMediator,
        storage: SqlAlchemySagaStorage,
    ) -> None:
        context = OrderContext(order_id="123", user_id="user1", amount=100.0)
        saga_id = uuid.uuid4()
        step_results = []
        async for result in saga_mediator.execute(context, saga_id=saga_id):
            step_results.append(result)
        assert len(step_results) == 3
        assert isinstance(step_results[0].response, ReserveInventoryResponse)
        assert step_results[0].response.inventory_id == "inv_123"
        assert isinstance(step_results[1].response, ProcessPaymentResponse)
        assert isinstance(step_results[2].response, ShipOrderResponse)
        assert step_results[0].step_type == ReserveInventoryStep
        assert step_results[1].step_type == ProcessPaymentStep
        assert step_results[2].step_type == ShipOrderStep
        status, stored_context, version = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.COMPLETED

    async def test_saga_mediator_processes_events_from_steps(
        self,
        saga_mediator: cqrs.SagaMediator,
        container: _TestContainer,
    ) -> None:
        context = OrderContext(order_id="456", user_id="user2", amount=200.0)
        step_results = []
        async for result in saga_mediator.execute(context):
            step_results.append(result)
        assert len(step_results) == 3
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
        context = OrderContext(order_id="789", user_id="user3", amount=300.0)
        async for result in saga_mediator.execute(context):
            pass
        inventory_handler = await container.resolve(InventoryReservedEventHandler)
        payment_handler = await container.resolve(PaymentProcessedEventHandler)
        shipping_handler = await container.resolve(OrderShippedEventHandler)
        assert len(inventory_handler.handled_events) >= 1
        assert len(payment_handler.handled_events) >= 1
        assert len(shipping_handler.handled_events) >= 1

    async def test_saga_mediator_handles_saga_failure_with_compensation(
        self,
        container: _TestContainer,
        storage: SqlAlchemySagaStorage,
    ) -> None:
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
            async for result in failing_mediator.execute(context, saga_id=saga_id):
                step_results.append(result)
        assert len(step_results) >= 1
        reserve_step = await container.resolve(ReserveInventoryStep)
        payment_step = await container.resolve(ProcessPaymentStep)
        assert reserve_step.compensate_called
        assert payment_step.compensate_called
        status, _, version = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.FAILED

    async def test_saga_mediator_with_saga_id_recovery(
        self,
        saga_mediator: cqrs.SagaMediator,
        storage: SqlAlchemySagaStorage,
    ) -> None:
        context = OrderContext(order_id="recover_123", user_id="user5", amount=500.0)
        saga_id = uuid.uuid4()
        step_results_1 = []
        async for result in saga_mediator.execute(context, saga_id=saga_id):
            step_results_1.append(result)
            if len(step_results_1) == 1:
                break
        assert len(step_results_1) == 1
        assert isinstance(step_results_1[0].response, ReserveInventoryResponse)
        status, _, version = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.RUNNING
        step_results_2 = []
        async for result in saga_mediator.execute(context, saga_id=saga_id):
            step_results_2.append(result)
        assert len(step_results_2) >= 2
        final_status, _, version = await storage.load_saga_state(saga_id)
        assert final_status == SagaStatus.COMPLETED

    async def test_saga_mediator_persistence_across_sessions(
        self,
        container: _TestContainer,
        storage: SqlAlchemySagaStorage,
        saga_session_factory_mysql: async_sessionmaker[AsyncSession],
    ) -> None:
        context = OrderContext(order_id="persist_123", user_id="user6", amount=600.0)
        saga_id = uuid.uuid4()

        def saga_mapper(mapper: SagaMap) -> None:
            mapper.bind(OrderContext, OrderSaga)

        saga_map = SagaMap()
        saga_mapper(saga_map)
        event_map = events.EventMap()
        event_emitter = events.EventEmitter(
            event_map=event_map,
            container=container,  # type: ignore
            message_broker=mock.AsyncMock(),
        )
        mediator = cqrs.SagaMediator(
            saga_map=saga_map,
            container=container,  # type: ignore
            event_emitter=event_emitter,
            event_map=event_map,
            storage=storage,
        )
        step_results_1 = []
        async for result in mediator.execute(context, saga_id=saga_id):
            step_results_1.append(result)
            if len(step_results_1) == 1:
                break
        new_storage = SqlAlchemySagaStorage(saga_session_factory_mysql)
        status, stored_context, version = await new_storage.load_saga_state(saga_id)
        assert status == SagaStatus.RUNNING
        history = await new_storage.get_step_history(saga_id)
        assert len(history) >= 1
        assert history[0].step_name == "ReserveInventoryStep"

    async def test_saga_mediator_concurrent_sagas(
        self,
        saga_mediator: cqrs.SagaMediator,
        storage: SqlAlchemySagaStorage,
    ) -> None:
        import asyncio

        contexts = [OrderContext(order_id=f"order_{i}", user_id=f"user_{i}", amount=100.0 * i) for i in range(3)]
        saga_ids = [uuid.uuid4() for _ in range(3)]

        async def execute_saga(context: OrderContext, saga_id: uuid.UUID) -> list:
            results = []
            async for result in saga_mediator.execute(context, saga_id=saga_id):
                results.append(result)
            return results

        tasks = [execute_saga(context, saga_id) for context, saga_id in zip(contexts, saga_ids)]
        all_results = await asyncio.gather(*tasks)
        assert len(all_results) == 3
        assert all(len(results) == 3 for results in all_results)
        for saga_id in saga_ids:
            status, _, version = await storage.load_saga_state(saga_id)
            assert status == SagaStatus.COMPLETED

    async def test_saga_mediator_concurrent_saga_creation_no_deadlock(
        self,
        saga_mediator: cqrs.SagaMediator,
        storage: SqlAlchemySagaStorage,
    ) -> None:
        import asyncio

        n = 10
        contexts = [OrderContext(order_id=f"order_{i}", user_id=f"user_{i}", amount=100.0 * (i + 1)) for i in range(n)]
        saga_ids = [uuid.uuid4() for _ in range(n)]

        async def execute_saga(context: OrderContext, saga_id: uuid.UUID) -> list:
            results = []
            async for result in saga_mediator.execute(context, saga_id=saga_id):
                results.append(result)
            return results

        tasks = [execute_saga(context, saga_id) for context, saga_id in zip(contexts, saga_ids)]
        all_results = await asyncio.gather(*tasks)
        assert len(all_results) == n
        assert all(len(results) == 3 for results in all_results)
        for saga_id in saga_ids:
            status, _, _ = await storage.load_saga_state(saga_id)
            assert status == SagaStatus.COMPLETED
