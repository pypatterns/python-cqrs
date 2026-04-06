"""Tests for Fallback mechanism in Saga."""

import typing

import pytest

from cqrs.adapters.circuit_breaker import AioBreakerAdapter
from cqrs.events.event import Event
from cqrs.saga.fallback import Fallback
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.memory import MemorySagaStorage
from .conftest import (
    OrderContext,
    ProcessPaymentResponse,
    ProcessPaymentStep,
    ReserveInventoryResponse,
    SagaContainer,
)


# Test step handlers for Fallback
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
        # Modify context to test snapshot/restore
        context.amount = 999.0
        raise RuntimeError("Primary step failed")

    async def compensate(self, context: OrderContext) -> None:
        self.compensate_called = True


class FallbackStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    """Fallback step that succeeds."""

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
        # Verify context was restored (amount should be original, not 999.0)
        assert context.amount != 999.0, "Context should be restored from snapshot"
        self._inventory_id = f"fallback_inv_{context.order_id}"
        response = ReserveInventoryResponse(
            inventory_id=self._inventory_id,
            reserved=True,
        )
        # Modify context to verify changes persist
        context.amount = 888.0
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        self.compensate_called = True
        self._inventory_id = None


class FailingFallbackStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    """Fallback step that also fails."""

    def __init__(self) -> None:
        self._events: list[Event] = []
        self.act_called = False

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        self.act_called = True
        raise RuntimeError("Fallback step also failed")

    async def compensate(self, context: OrderContext) -> None:
        pass


class BusinessException(Exception):
    """Business exception that should not open circuit breaker."""

    pass


class StepWithBusinessException(
    SagaStepHandler[OrderContext, ReserveInventoryResponse],
):
    """Step that raises business exception."""

    def __init__(self) -> None:
        self._events: list[Event] = []
        self.act_called = False

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        self.act_called = True
        raise BusinessException("Business logic error")

    async def compensate(self, context: OrderContext) -> None:
        pass


class StepWithSpecificException(
    SagaStepHandler[OrderContext, ReserveInventoryResponse],
):
    """Step that raises a specific exception type."""

    def __init__(self) -> None:
        self._events: list[Event] = []
        self.act_called = False

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        self.act_called = True
        raise ConnectionError("Connection failed")

    async def compensate(self, context: OrderContext) -> None:
        pass


@pytest.fixture
def container():
    """Create a simple DI container."""
    return SagaContainer()


@pytest.fixture
def storage():
    """Create memory storage."""
    return MemorySagaStorage()


@pytest.mark.asyncio
async def test_fallback_execution_on_primary_failure(container, storage):
    """Test that fallback is executed when primary step fails."""

    # Create saga with Fallback
    class TestSaga(Saga[OrderContext]):
        steps = [
            Fallback(
                step=FailingStep,
                fallback=FallbackStep,
            ),
            ProcessPaymentStep,
        ]

    saga = TestSaga()
    context = OrderContext(order_id="123", user_id="user1", amount=100.0)
    results: list[SagaStepResult[OrderContext, typing.Any]] = []

    async with saga.transaction(
        context=context,
        container=container,
        storage=storage,
    ) as transaction:
        async for result in transaction:
            results.append(result)

    # Verify fallback was executed
    assert len(results) == 2  # Fallback step + ProcessPaymentStep
    assert results[0].response.inventory_id == "fallback_inv_123"

    # Verify context was restored before fallback execution
    # (amount should be original, not modified by failing step)
    # But then modified by fallback step
    assert context.amount == 888.0

    # Verify step history
    history = await storage.get_step_history(transaction.saga_id)
    step_names = {e.step_name for e in history if e.status == SagaStepStatus.COMPLETED}
    assert "FallbackStep" in step_names
    assert "FailingStep" not in step_names  # Should not be logged as completed

    # Verify saga completed successfully
    status, _, _ = await storage.load_saga_state(transaction.saga_id)
    assert status == SagaStatus.COMPLETED


@pytest.mark.asyncio
async def test_fallback_with_failure_exceptions(container, storage):
    """Test that fallback is triggered for specific exception types."""

    class TestSaga(Saga[OrderContext]):
        steps = [
            Fallback(
                step=StepWithSpecificException,
                fallback=FallbackStep,
                failure_exceptions=(ConnectionError,),
            ),
        ]

    saga = TestSaga()
    context = OrderContext(order_id="123", user_id="user1", amount=100.0)
    results: list[SagaStepResult[OrderContext, typing.Any]] = []

    async with saga.transaction(
        context=context,
        container=container,
        storage=storage,
    ) as transaction:
        async for result in transaction:
            results.append(result)

    # Verify fallback was executed
    assert len(results) == 1
    assert results[0].response.inventory_id == "fallback_inv_123"

    # Verify step history
    history = await storage.get_step_history(transaction.saga_id)
    step_names = {e.step_name for e in history if e.status == SagaStepStatus.COMPLETED}
    assert "FallbackStep" in step_names


@pytest.mark.asyncio
async def test_fallback_failure_triggers_saga_failure(container, storage):
    """Test that if fallback also fails, saga fails."""
    import uuid

    class TestSaga(Saga[OrderContext]):
        steps = [
            Fallback(
                step=FailingStep,
                fallback=FailingFallbackStep,
            ),
        ]

    saga = TestSaga()
    context = OrderContext(order_id="123", user_id="user1", amount=100.0)
    saga_id = uuid.uuid4()

    with pytest.raises(RuntimeError, match="Fallback step also failed"):
        async with saga.transaction(
            context=context,
            container=container,
            storage=storage,
            saga_id=saga_id,
        ) as transaction:
            async for _ in transaction:
                pass

    # Verify saga failed
    status, _, _ = await storage.load_saga_state(saga_id)
    assert status == SagaStatus.FAILED


@pytest.mark.asyncio
async def test_fallback_idempotency(container, storage):
    """Test that completed fallback steps are skipped on recovery."""

    class TestSaga(Saga[OrderContext]):
        steps = [
            Fallback(
                step=FailingStep,
                fallback=FallbackStep,
            ),
            ProcessPaymentStep,
        ]

    saga = TestSaga()
    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    # First execution - fallback should execute
    results1: list[SagaStepResult[OrderContext, typing.Any]] = []
    async with saga.transaction(
        context=context,
        container=container,
        storage=storage,
    ) as transaction1:
        async for result in transaction1:
            results1.append(result)

    saga_id = transaction1.saga_id

    # Second execution with same saga_id - should skip completed steps
    context2 = OrderContext(order_id="123", user_id="user1", amount=100.0)
    results2: list[SagaStepResult[OrderContext, typing.Any]] = []
    async with saga.transaction(
        context=context2,
        container=container,
        storage=storage,
        saga_id=saga_id,
    ) as transaction2:
        async for result in transaction2:
            results2.append(result)

    # Should have no new results (all steps already completed)
    assert len(results2) == 0


@pytest.mark.asyncio
async def test_fallback_with_circuit_breaker(container, storage):
    """Test Fallback with circuit breaker integration."""
    pytest.importorskip("aiobreaker")

    # Create circuit breaker adapter with unique namespace for this test
    cb_adapter = AioBreakerAdapter(
        fail_max=3,  # Increase to 3 to allow 2 failures before opening
        timeout_duration=1,
        exclude=[BusinessException],
    )

    class TestSaga(Saga[OrderContext]):
        steps = [
            Fallback(
                step=FailingStep,
                fallback=FallbackStep,
                circuit_breaker=cb_adapter,
            ),
        ]

    saga = TestSaga()
    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    # First failure - should trigger fallback
    results1: list[SagaStepResult[OrderContext, typing.Any]] = []
    async with saga.transaction(
        context=context,
        container=container,
        storage=storage,
    ) as transaction1:
        async for result in transaction1:
            results1.append(result)

    assert len(results1) == 1
    assert results1[0].response.inventory_id == "fallback_inv_123"

    # Second failure - should trigger fallback again
    context2 = OrderContext(order_id="456", user_id="user2", amount=200.0)
    results2: list[SagaStepResult[OrderContext, typing.Any]] = []
    async with saga.transaction(
        context=context2,
        container=container,
        storage=storage,
    ) as transaction2:
        async for result in transaction2:
            results2.append(result)

    assert len(results2) == 1
    assert results2[0].response.inventory_id == "fallback_inv_456"


@pytest.mark.asyncio
async def test_circuit_breaker_namespace_isolation(container, storage):
    """Test that different steps have isolated circuit breaker states."""
    pytest.importorskip("aiobreaker")

    # Create adapter with higher fail_max to allow multiple failures
    cb_adapter = AioBreakerAdapter(
        fail_max=3,
        timeout_duration=1,
    )

    class AnotherFailingStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
        """Another failing step for namespace isolation test."""

        def __init__(self) -> None:
            self._events: list[Event] = []
            self.act_called = False

        @property
        def events(self) -> list[Event]:
            return self._events.copy()

        async def act(
            self,
            context: OrderContext,
        ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
            self.act_called = True
            raise RuntimeError("Another step failed")

        async def compensate(self, context: OrderContext) -> None:
            pass

    class TestSaga(Saga[OrderContext]):
        steps = [
            Fallback(
                step=FailingStep,
                fallback=FallbackStep,
                circuit_breaker=cb_adapter,
            ),
            Fallback(
                step=AnotherFailingStep,
                fallback=FallbackStep,
                circuit_breaker=cb_adapter,
            ),
        ]

    saga = TestSaga()
    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    results: list[SagaStepResult[OrderContext, typing.Any]] = []
    async with saga.transaction(
        context=context,
        container=container,
        storage=storage,
    ) as transaction:
        async for result in transaction:
            results.append(result)

    # Both steps should have triggered fallbacks
    # Namespace isolation ensures one step's failures don't affect the other
    assert len(results) == 2
    assert results[0].response.inventory_id == "fallback_inv_123"
    assert results[1].response.inventory_id == "fallback_inv_123"


@pytest.mark.asyncio
async def test_business_exception_does_not_open_circuit(container, storage):
    """Test that business exceptions don't open the circuit breaker."""
    pytest.importorskip("aiobreaker")

    cb_adapter = AioBreakerAdapter(
        fail_max=1,
        timeout_duration=1,
        exclude=[BusinessException],
    )

    class TestSaga(Saga[OrderContext]):
        steps = [
            Fallback(
                step=StepWithBusinessException,
                fallback=FallbackStep,
                circuit_breaker=cb_adapter,
            ),
        ]

    saga = TestSaga()

    # Execute multiple times - business exception should not open circuit
    for i in range(3):
        context_i = OrderContext(order_id=f"123_{i}", user_id="user1", amount=100.0)
        results: list[SagaStepResult[OrderContext, typing.Any]] = []
        async with saga.transaction(
            context=context_i,
            container=container,
            storage=storage,
        ) as transaction:
            async for result in transaction:
                results.append(result)

        # Fallback should execute each time (circuit should not be open)
        assert len(results) == 1
        assert results[0].response.inventory_id == f"fallback_inv_123_{i}"


@pytest.mark.asyncio
async def test_primary_step_success_no_fallback(container, storage):
    """Test that fallback is not executed when primary step succeeds."""

    class SuccessfulStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
        """Step that succeeds."""

        def __init__(self) -> None:
            self._events: list[Event] = []
            self.act_called = False
            self._inventory_id: typing.Optional[str] = None

        @property
        def events(self) -> list[Event]:
            return self._events.copy()

        async def act(
            self,
            context: OrderContext,
        ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
            self.act_called = True
            self._inventory_id = f"primary_inv_{context.order_id}"
            response = ReserveInventoryResponse(
                inventory_id=self._inventory_id,
                reserved=True,
            )
            return self._generate_step_result(response)

        async def compensate(self, context: OrderContext) -> None:
            pass

    class TestSaga(Saga[OrderContext]):
        steps = [
            Fallback(
                step=SuccessfulStep,
                fallback=FallbackStep,
            ),
        ]

    saga = TestSaga()
    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    results: list[SagaStepResult[OrderContext, typing.Any]] = []
    async with saga.transaction(
        context=context,
        container=container,
        storage=storage,
    ) as transaction:
        async for result in transaction:
            results.append(result)

    # Primary step should succeed, fallback should not execute
    assert len(results) == 1
    assert results[0].response.inventory_id == "primary_inv_123"

    # Verify fallback step was not called
    fallback_step = await container.resolve(FallbackStep)
    assert not fallback_step.act_called

    # Verify step history
    history = await storage.get_step_history(transaction.saga_id)
    step_names = {e.step_name for e in history if e.status == SagaStepStatus.COMPLETED}
    assert "SuccessfulStep" in step_names
    assert "FallbackStep" not in step_names


@pytest.mark.asyncio
async def test_fallback_context_snapshot_restore(container, storage):
    """Test that context is properly snapshot and restored on fallback."""

    class ContextModifyingFailingStep(
        SagaStepHandler[OrderContext, ReserveInventoryResponse],
    ):
        """Step that modifies context then fails."""

        def __init__(self) -> None:
            self._events: list[Event] = []
            self.act_called = False

        @property
        def events(self) -> list[Event]:
            return self._events.copy()

        async def act(
            self,
            context: OrderContext,
        ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
            self.act_called = True
            # Modify context
            context.amount = 999.0
            context.order_id = "modified"
            raise RuntimeError("Failed after modifying context")

        async def compensate(self, context: OrderContext) -> None:
            pass

    class ContextVerifyingFallbackStep(
        SagaStepHandler[OrderContext, ReserveInventoryResponse],
    ):
        """Fallback step that verifies context was restored."""

        def __init__(self) -> None:
            self._events: list[Event] = []
            self.act_called = False
            self._inventory_id: typing.Optional[str] = None

        @property
        def events(self) -> list[Event]:
            return self._events.copy()

        async def act(
            self,
            context: OrderContext,
        ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
            self.act_called = True
            # Verify context was restored (not modified by failing step)
            assert context.amount == 100.0, "Context amount should be restored"
            assert context.order_id == "123", "Context order_id should be restored"
            self._inventory_id = f"fallback_inv_{context.order_id}"
            response = ReserveInventoryResponse(
                inventory_id=self._inventory_id,
                reserved=True,
            )
            return self._generate_step_result(response)

        async def compensate(self, context: OrderContext) -> None:
            pass

    class TestSaga(Saga[OrderContext]):
        steps = [
            Fallback(
                step=ContextModifyingFailingStep,
                fallback=ContextVerifyingFallbackStep,
            ),
        ]

    saga = TestSaga()
    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    results: list[SagaStepResult[OrderContext, typing.Any]] = []
    async with saga.transaction(
        context=context,
        container=container,
        storage=storage,
    ) as transaction:
        async for result in transaction:
            results.append(result)

    # Verify fallback executed and context was restored
    assert len(results) == 1
    assert results[0].response.inventory_id == "fallback_inv_123"


@pytest.mark.asyncio
async def test_fallback_compensation(container, storage):
    """Test that fallback steps are properly compensated when saga fails."""

    class CompensatableFallbackStep(
        SagaStepHandler[OrderContext, ReserveInventoryResponse],
    ):
        """Fallback step that can be compensated."""

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
            self._inventory_id = f"fallback_inv_{context.order_id}"
            response = ReserveInventoryResponse(
                inventory_id=self._inventory_id,
                reserved=True,
            )
            return self._generate_step_result(response)

        async def compensate(self, context: OrderContext) -> None:
            self.compensate_called = True
            self._inventory_id = None

    class FailingNextStep(SagaStepHandler[OrderContext, ProcessPaymentResponse]):
        """Step that fails after fallback succeeds."""

        def __init__(self) -> None:
            self._events: list[Event] = []
            self.act_called = False

        @property
        def events(self) -> list[Event]:
            return self._events.copy()

        async def act(
            self,
            context: OrderContext,
        ) -> SagaStepResult[OrderContext, ProcessPaymentResponse]:
            self.act_called = True
            raise RuntimeError("Next step failed")

        async def compensate(self, context: OrderContext) -> None:
            pass

    class TestSaga(Saga[OrderContext]):
        steps = [
            Fallback(
                step=FailingStep,
                fallback=CompensatableFallbackStep,
            ),
            FailingNextStep,
        ]

    saga = TestSaga()
    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    with pytest.raises(RuntimeError, match="Next step failed"):
        async with saga.transaction(
            context=context,
            container=container,
            storage=storage,
        ) as transaction:
            async for _ in transaction:
                pass

    # Verify fallback step was compensated
    fallback_step = await container.resolve(CompensatableFallbackStep)
    assert fallback_step.act_called
    assert fallback_step.compensate_called
    assert fallback_step._inventory_id is None
