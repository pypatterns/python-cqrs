import contextlib
import datetime
import logging
import typing
import uuid

from cqrs.dispatcher.exceptions import SagaConcurrencyError
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.models import SagaLogEntry
from cqrs.saga.storage.protocol import ISagaStorage, SagaStorageRun

logger = logging.getLogger("cqrs.saga.storage.memory")


class _MemorySagaStorageRun(SagaStorageRun):
    """Run that delegates to the underlying MemorySagaStorage; commit/rollback are no-ops."""

    def __init__(self, storage: "MemorySagaStorage") -> None:
        """
        Initialize the run and bind it to the provided MemorySagaStorage.

        Parameters:
            storage (MemorySagaStorage): Underlying in-memory storage instance used to delegate saga operations.
        """
        self._storage = storage

    async def create_saga(
        self,
        saga_id: uuid.UUID,
        name: str,
        context: dict[str, typing.Any],
    ) -> None:
        """
        Create a new saga entry in the underlying memory storage.

        Parameters:
            saga_id (uuid.UUID): Unique identifier for the saga.
            name (str): Human-readable saga name.
            context (dict[str, typing.Any]): Initial saga context payload.

        Raises:
            ValueError: If a saga with the same `saga_id` already exists.
        """
        await self._storage.create_saga(saga_id, name, context)

    async def update_context(
        self,
        saga_id: uuid.UUID,
        context: dict[str, typing.Any],
        current_version: typing.Optional[int] = None,
    ) -> None:
        """
        Update the stored context for the given saga.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga whose context will be updated.
            context (dict[str, typing.Any]): New context to store for the saga.
            current_version (typing.Optional[int ]): If provided, require the stored saga version to match this value (optimistic locking).

        Raises:
            ValueError: If the saga_id does not exist.
            SagaConcurrencyError: If current_version is provided and does not match the stored version.
        """
        await self._storage.update_context(saga_id, context, current_version)

    async def update_status(
        self,
        saga_id: uuid.UUID,
        status: SagaStatus,
    ) -> None:
        """
        Update the stored status of the saga identified by `saga_id`.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga to update.
            status (SagaStatus): New status to set for the saga.

        Raises:
            ValueError: If no saga exists with the given `saga_id`.
        """
        await self._storage.update_status(saga_id, status)

    async def log_step(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: typing.Optional[str] = None,
    ) -> None:
        """
        Log a step entry for the given saga into the underlying storage.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga.
            step_name (str): Name of the saga step.
            action (Literal["act", "compensate"]): Whether the step is a forward action ("act") or a compensation ("compensate").
            status (SagaStepStatus): Outcome status of the step.
            details (typing.Optional[str ]): Optional free-form details or metadata about the step.
        """
        await self._storage.log_step(
            saga_id,
            step_name,
            action,
            status,
            details,
        )

    async def load_saga_state(
        self,
        saga_id: uuid.UUID,
        *,
        read_for_update: bool = False,
    ) -> tuple[SagaStatus, dict[str, typing.Any], int]:
        """
        Load the current state for a saga.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga to load.
            read_for_update (bool): If True, acquire the state for update (may be used for optimistic locking or exclusive access).

        Returns:
            tuple[SagaStatus, dict[str, typing.Any], int]: A tuple containing the saga's status, its context dictionary, and the current version number.
        """
        return await self._storage.load_saga_state(
            saga_id,
            read_for_update=read_for_update,
        )

    async def get_step_history(
        self,
        saga_id: uuid.UUID,
    ) -> list[SagaLogEntry]:
        """
        Retrieve the step log/history for a saga.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga whose step history is requested.

        Returns:
            list[SagaLogEntry]: Saga step log entries sorted by timestamp in ascending order (oldest first). Returns an empty list if no logs exist.
        """
        return await self._storage.get_step_history(saga_id)

    async def commit(self) -> None:
        """
        No-op commit for an in-memory saga run; provided to satisfy the SagaStorageRun interface.

        This method intentionally performs no action because the memory storage does not require an explicit commit.
        """
        pass

    async def rollback(self) -> None:
        """
        Perform no action for rollback; provided to satisfy the SagaStorageRun interface.
        """
        pass


class MemorySagaStorage(ISagaStorage):
    """In-memory implementation of ISagaStorage for testing and development."""

    def __init__(self) -> None:
        # Structure: {saga_id: {name, status, context, created_at, updated_at, version}}
        """
        Initialize in-memory storage for sagas and their step logs.

        Creates two internal mappings:
        - _sagas: maps saga_id (UUID) to a dictionary containing keys `name`, `status`, `context`, `created_at`, `updated_at`, and `version`.
        - _logs: maps saga_id (UUID) to a list of SagaLogEntry objects representing the saga's step history.
        """
        self._sagas: dict[uuid.UUID, dict[str, typing.Any]] = {}
        # Structure: {saga_id: [SagaLogEntry, ...]}
        self._logs: dict[uuid.UUID, list[SagaLogEntry]] = {}

    def create_run(
        self,
    ) -> contextlib.AbstractAsyncContextManager[SagaStorageRun]:
        """
        Provide an asynchronous context manager that yields a SagaStorageRun bound to this storage.

        Returns:
            An asynchronous context manager that yields a `SagaStorageRun` instance backed by this `MemorySagaStorage`.
        """

        @contextlib.asynccontextmanager
        async def _run() -> typing.AsyncGenerator[SagaStorageRun, None]:
            yield _MemorySagaStorageRun(self)

        return _run()

    async def create_saga(
        self,
        saga_id: uuid.UUID,
        name: str,
        context: dict[str, typing.Any],
    ) -> None:
        """
        Create a new saga record in the in-memory store.

        Parameters:
            saga_id (uuid.UUID): Identifier for the saga; must not already exist.
            name (str): Human-readable name for the saga.
            context (dict[str, typing.Any]): Initial context payload for the saga.

        Raises:
            ValueError: If a saga with `saga_id` already exists.
        """
        if saga_id in self._sagas:
            raise ValueError(f"Saga {saga_id} already exists")

        now = datetime.datetime.now(datetime.timezone.utc)
        self._sagas[saga_id] = {
            "name": name,
            "status": SagaStatus.PENDING,
            "context": context,
            "created_at": now,
            "updated_at": now,
            "version": 1,
            "recovery_attempts": 0,
        }
        self._logs[saga_id] = []

    async def update_context(
        self,
        saga_id: uuid.UUID,
        context: dict[str, typing.Any],
        current_version: typing.Optional[int] = None,
    ) -> None:
        if saga_id not in self._sagas:
            raise ValueError(f"Saga {saga_id} not found")

        saga_data = self._sagas[saga_id]

        # Optimistic locking check
        if current_version is not None:
            if saga_data["version"] != current_version:
                raise SagaConcurrencyError(f"Saga {saga_id} was modified concurrently")

        saga_data["context"] = context
        saga_data["updated_at"] = datetime.datetime.now(datetime.timezone.utc)
        saga_data["version"] += 1

    async def update_status(
        self,
        saga_id: uuid.UUID,
        status: SagaStatus,
    ) -> None:
        if saga_id not in self._sagas:
            raise ValueError(f"Saga {saga_id} not found")

        saga_data = self._sagas[saga_id]
        saga_data["status"] = status
        saga_data["updated_at"] = datetime.datetime.now(datetime.timezone.utc)
        saga_data["version"] += 1

    async def log_step(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: typing.Optional[str] = None,
    ) -> None:
        if saga_id not in self._sagas:
            raise ValueError(f"Saga {saga_id} not found")

        entry = SagaLogEntry(
            saga_id=saga_id,
            step_name=step_name,
            action=action,
            status=status,
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            details=details,
        )
        self._logs[saga_id].append(entry)
        logger.debug(f"Saga {saga_id} step {step_name} {action}: {status}")

    async def load_saga_state(
        self,
        saga_id: uuid.UUID,
        *,
        read_for_update: bool = False,
    ) -> tuple[SagaStatus, dict[str, typing.Any], int]:
        if saga_id not in self._sagas:
            raise ValueError(f"Saga {saga_id} not found")

        data = self._sagas[saga_id]
        return data["status"], data["context"], data["version"]

    async def get_step_history(
        self,
        saga_id: uuid.UUID,
    ) -> list[SagaLogEntry]:
        if saga_id not in self._logs:
            return []
        # Sort by timestamp
        return sorted(self._logs[saga_id], key=lambda x: x.timestamp)

    async def get_sagas_for_recovery(
        self,
        limit: int,
        max_recovery_attempts: int = 5,
        stale_after_seconds: typing.Optional[int] = None,
        saga_name: typing.Optional[str] = None,
    ) -> list[uuid.UUID]:
        """
        Selects saga IDs eligible for recovery based on status, recovery attempts, staleness, and an optional name filter.

        Parameters:
            limit (int): Maximum number of saga IDs to return.
            max_recovery_attempts (int): Upper bound (exclusive) on recovery attempts; only sagas with fewer attempts are considered.
            stale_after_seconds (typing.Optional[int ]): If provided, only sagas last updated earlier than this many seconds before now are considered; if None, staleness is ignored.
            saga_name (typing.Optional[str ]): If provided, only sagas with this name are considered; if None, name is not filtered.

        Returns:
            list[uuid.UUID]: Up to `limit` saga IDs sorted by oldest `updated_at` first that match the recovery criteria.
        """
        recoverable = (SagaStatus.RUNNING, SagaStatus.COMPENSATING)
        now = datetime.datetime.now(datetime.timezone.utc)
        threshold = (now - datetime.timedelta(seconds=stale_after_seconds)) if stale_after_seconds is not None else None
        candidates = [
            sid
            for sid, data in self._sagas.items()
            if data["status"] in recoverable
            and data.get("recovery_attempts", 0) < max_recovery_attempts
            and (threshold is None or data["updated_at"] < threshold)
            and (saga_name is None or data["name"] == saga_name)
        ]
        candidates.sort(key=lambda sid: self._sagas[sid]["updated_at"])
        return candidates[:limit]

    async def increment_recovery_attempts(
        self,
        saga_id: uuid.UUID,
        new_status: typing.Optional[SagaStatus] = None,
    ) -> None:
        if saga_id not in self._sagas:
            raise ValueError(f"Saga {saga_id} not found")
        data = self._sagas[saga_id]
        data["recovery_attempts"] = data.get("recovery_attempts", 0) + 1
        data["updated_at"] = datetime.datetime.now(datetime.timezone.utc)
        data["version"] += 1
        if new_status is not None:
            data["status"] = new_status

    async def set_recovery_attempts(
        self,
        saga_id: uuid.UUID,
        attempts: int,
    ) -> None:
        if saga_id not in self._sagas:
            raise ValueError(f"Saga {saga_id} not found")
        data = self._sagas[saga_id]
        data["recovery_attempts"] = attempts
        data["updated_at"] = datetime.datetime.now(datetime.timezone.utc)
        data["version"] += 1
