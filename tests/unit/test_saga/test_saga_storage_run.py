"""Tests for saga storage create_run() and checkpoint (run) path."""

import typing
import uuid

import pytest

from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.memory import MemorySagaStorage
from cqrs.saga.storage.protocol import ISagaStorage

from .conftest import (
    FailingStep,
    OrderContext,
    ReserveInventoryStep,
    SagaContainer,
    ShipOrderStep,
)


class StorageWithoutCreateRun(ISagaStorage):
    """Storage that does not implement create_run (legacy path)."""

    def __init__(self) -> None:
        """
        Create a storage wrapper that delegates all saga operations to an internal in-memory storage while intentionally not providing `create_run`.
        """
        self._inner = MemorySagaStorage()

    async def create_saga(self, saga_id: uuid.UUID, name: str, context: dict) -> None:
        """
        Create a new saga record with the given identifier, name, and initial context.

        Parameters:
            saga_id (uuid.UUID): Unique identifier for the saga.
            name (str): Human-readable name of the saga.
            context (dict): Initial context payload for the saga.
        """
        await self._inner.create_saga(saga_id, name, context)

    async def update_context(
        self,
        saga_id: uuid.UUID,
        context: dict,
        current_version: typing.Optional[int] = None,
    ) -> None:
        """
        Update the stored context for a saga, optionally validating the expected current version.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga whose context will be updated.
            context (dict): New context data to persist for the saga.
            current_version (typing.Optional[int ]): If provided, the update will only proceed when the stored version equals this value; pass None to skip version validation.
        """
        await self._inner.update_context(saga_id, context, current_version)

    async def update_status(self, saga_id: uuid.UUID, status: SagaStatus) -> None:
        """
        Update the stored status of a saga.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga to update.
            status (SagaStatus): New status to set for the saga.
        """
        await self._inner.update_status(saga_id, status)

    async def log_step(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: typing.Optional[str] = None,
    ) -> None:
        """
        Record the execution or compensation outcome of a saga step.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga.
            step_name (str): Name of the step being logged.
            action (Literal["act", "compensate"]): Whether this log entry is for the step's normal action ("act") or its compensation ("compensate").
            status (SagaStepStatus): Resulting status of the step.
            details (typing.Optional[str ]): Optional human-readable details or metadata about the step event.
        """
        await self._inner.log_step(saga_id, step_name, action, status, details)

    async def load_saga_state(
        self,
        saga_id: uuid.UUID,
        *,
        read_for_update: bool = False,
    ) -> tuple[SagaStatus, dict, int]:
        """
        Load the current state for a saga from the underlying storage.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga to load.
            read_for_update (bool): If True, load the state with intent to update (may acquire locks or use a read-for-update strategy).

        Returns:
            tuple[SagaStatus, dict, int]: A tuple containing the saga's status, its context dictionary, and the current version number.
        """
        return await self._inner.load_saga_state(
            saga_id,
            read_for_update=read_for_update,
        )

    async def get_step_history(self, saga_id: uuid.UUID) -> list:
        """
        Return the step execution history for the given saga.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga whose history to retrieve.

        Returns:
            list: Step history records in chronological order. Each record describes the step name, action ("act" or "compensate"), step status, timestamp, and any optional details.
        """
        return await self._inner.get_step_history(saga_id)

    async def get_sagas_for_recovery(
        self,
        limit: int,
        max_recovery_attempts: int = 5,
        stale_after_seconds: typing.Optional[int] = None,
        saga_name: typing.Optional[str] = None,
    ) -> list[uuid.UUID]:
        """
        Selects saga IDs that are eligible for recovery.

        Parameters:
            limit (int): Maximum number of saga IDs to return.
            max_recovery_attempts (int): Only include sagas with fewer than this many recovery attempts.
            stale_after_seconds (typing.Optional[int ]): If provided, only include sagas last updated more than this many seconds ago; if None, do not filter by staleness.
            saga_name (typing.Optional[str ]): If provided, restrict results to sagas with this name.

        Returns:
            list[uuid.UUID]: Saga UUIDs that match the recovery criteria, up to `limit`.
        """
        return await self._inner.get_sagas_for_recovery(
            limit,
            max_recovery_attempts=max_recovery_attempts,
            stale_after_seconds=stale_after_seconds,
            saga_name=saga_name,
        )

    async def increment_recovery_attempts(
        self,
        saga_id: uuid.UUID,
        new_status: typing.Optional[SagaStatus] = None,
    ) -> None:
        """
        Increment the recovery-attempts counter for a saga and optionally update its status.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga whose recovery attempts should be incremented.
            new_status (typing.Optional[SagaStatus ]): If provided, update the saga's status to this value after incrementing attempts; otherwise leave status unchanged.
        """
        await self._inner.increment_recovery_attempts(saga_id, new_status)

    async def set_recovery_attempts(self, saga_id: uuid.UUID, attempts: int) -> None:
        """
        Set the number of recovery attempts recorded for a saga.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga whose recovery attempts will be set.
            attempts (int): Number of recovery attempts to record; should be zero or a positive integer.
        """
        await self._inner.set_recovery_attempts(saga_id, attempts)


async def test_memory_storage_create_run_yields_run_with_required_methods() -> None:
    """create_run() yields an object with create_saga, update_*, log_step, commit, rollback."""
    storage = MemorySagaStorage()
    async with storage.create_run() as run:
        assert run is not None
        assert hasattr(run, "create_saga")
        assert hasattr(run, "update_context")
        assert hasattr(run, "update_status")
        assert hasattr(run, "log_step")
        assert hasattr(run, "load_saga_state")
        assert hasattr(run, "get_step_history")
        assert hasattr(run, "commit")
        assert hasattr(run, "rollback")


async def test_memory_storage_run_commit_rollback_are_no_op() -> None:
    """Run from MemorySagaStorage: commit and rollback do not raise."""
    storage = MemorySagaStorage()
    async with storage.create_run() as run:
        await run.commit()
        await run.rollback()


async def test_memory_storage_run_persists_after_commit() -> None:
    """Using run: create_saga + commit makes state visible to storage."""
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()
    async with storage.create_run() as run:
        await run.create_saga(saga_id, "TestSaga", {"key": "value"})
        await run.update_status(saga_id, SagaStatus.RUNNING)
        await run.commit()
    status, context, version = await storage.load_saga_state(saga_id)
    assert status == SagaStatus.RUNNING
    assert context == {"key": "value"}
    assert version >= 1


async def test_saga_with_storage_with_create_run_completes_successfully() -> None:
    """Saga with MemorySagaStorage (has create_run) uses run path and completes."""
    from cqrs.saga.saga import Saga

    class TwoStepSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ShipOrderStep]

    container = SagaContainer()
    container.register(ReserveInventoryStep, ReserveInventoryStep())
    container.register(ShipOrderStep, ShipOrderStep())
    storage = MemorySagaStorage()
    saga = TwoStepSaga()
    context = OrderContext(order_id="o1", user_id="u1", amount=50.0)
    results = []
    async with saga.transaction(
        context=context,
        container=container,  # type: ignore[arg-type]
        storage=storage,
    ) as transaction:
        async for step_result in transaction:
            results.append(step_result)
    assert len(results) == 2
    status, _, _ = await storage.load_saga_state(transaction.saga_id)
    assert status == SagaStatus.COMPLETED


async def test_saga_with_storage_without_create_run_completes_successfully() -> None:
    """Saga with storage that does not implement create_run uses legacy path."""
    from cqrs.saga.saga import Saga

    class TwoStepSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ShipOrderStep]

    container = SagaContainer()
    container.register(ReserveInventoryStep, ReserveInventoryStep())
    container.register(ShipOrderStep, ShipOrderStep())
    storage = StorageWithoutCreateRun()
    saga = TwoStepSaga()
    context = OrderContext(order_id="o2", user_id="u2", amount=60.0)
    results = []
    async with saga.transaction(
        context=context,
        container=container,  # type: ignore[arg-type]
        storage=storage,
    ) as transaction:
        async for step_result in transaction:
            results.append(step_result)
    assert len(results) == 2
    status, _, _ = await storage.load_saga_state(transaction.saga_id)
    assert status == SagaStatus.COMPLETED


async def test_saga_with_run_path_compensates_on_failure() -> None:
    """When a step fails, compensation runs and saga ends in FAILED (run path)."""
    from cqrs.saga.saga import Saga

    class SagaWithFailure(Saga[OrderContext]):
        steps = [ReserveInventoryStep, FailingStep]

    container = SagaContainer()
    container.register(ReserveInventoryStep, ReserveInventoryStep())
    container.register(FailingStep, FailingStep())
    storage = MemorySagaStorage()
    saga = SagaWithFailure()
    context = OrderContext(order_id="o3", user_id="u3", amount=70.0)
    saga_id: typing.Optional[uuid.UUID] = None
    with pytest.raises(ValueError, match="Step failed"):
        async with saga.transaction(
            context=context,
            container=container,  # type: ignore[arg-type]
            storage=storage,
        ) as transaction:
            saga_id = transaction.saga_id
            async for _ in transaction:
                pass
    assert saga_id is not None
    status, _, _ = await storage.load_saga_state(saga_id)
    assert status == SagaStatus.FAILED


async def test_storage_create_run_raises_not_implemented_by_default() -> None:
    """Default create_run() on a minimal storage raises NotImplementedError."""
    storage = StorageWithoutCreateRun()
    with pytest.raises(NotImplementedError, match="does not support create_run"):
        storage.create_run()
