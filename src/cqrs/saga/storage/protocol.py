import abc
import contextlib
import typing
import uuid

from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.models import SagaLogEntry


class SagaStorageRun(typing.Protocol):
    """Protocol for a scoped saga storage run (one session, checkpoint commits).

    Returned by ISagaStorage.create_run(). Methods do not commit; the caller
    must call commit() at checkpoints. Session is never exposed.
    """

    async def create_saga(
        self,
        saga_id: uuid.UUID,
        name: str,
        context: dict[str, typing.Any],
    ) -> None:
        """
        Create a new saga execution record with initial PENDING status and version 1.

        Parameters:
            saga_id (uuid.UUID): Unique identifier for the saga (primary key).
            name (str): Human-friendly name used for diagnostics and filtering.
            context (dict[str, Any]): JSON-serializable initial saga context to persist.
        """

    async def update_context(
        self,
        saga_id: uuid.UUID,
        context: dict[str, typing.Any],
        current_version: typing.Optional[int] = None,
    ) -> None:
        """
        Persist a snapshot of the saga's execution context, optionally using optimistic locking.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga to update.
            context (dict[str, Any]): JSON-serializable context object to store as the new snapshot.
            current_version (typing.Optional[int ]): If provided, perform an optimistic-locking update that succeeds only
                if the stored version matches this value; on success the stored version is incremented.

        Raises:
            SagaConcurrencyError: If `current_version` is provided and does not match the stored version.
        """

    async def update_status(
        self,
        saga_id: uuid.UUID,
        status: SagaStatus,
    ) -> None:
        """
        Set the global status for the saga identified by `saga_id`.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga to update.
            status (SagaStatus): New global status to persist (for example RUNNING, COMPLETED, COMPENSATING).

        Notes:
            This operation does not commit the storage session; the caller must call `commit()` on the active run or session to persist the change.
        """

    async def log_step(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: typing.Optional[str] = None,
    ) -> None:
        """
        Append a step transition to the saga's execution log.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga whose log will be appended.
            step_name (str): Logical name of the step (used for diagnostics and replay).
            action (Literal["act", "compensate"]): Whether this entry records the primary action ("act") or its compensating action ("compensate").
            status (SagaStepStatus): The step transition status to record (e.g., started, completed, failed, compensated).
            details (typing.Optional[str ]): Optional human-readable details or diagnostics about the transition.
        """

    async def load_saga_state(
        self,
        saga_id: uuid.UUID,
        *,
        read_for_update: bool = False,
    ) -> tuple[SagaStatus, dict[str, typing.Any], int]:
        """
        Load the current saga execution state.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga to load.
            read_for_update (bool): If True, acquire a database lock for update to prevent concurrent modifications.

        Returns:
            tuple[SagaStatus, dict[str, Any], int]: A tuple containing the saga's global status, the latest persisted context (JSON-serializable), and the current optimistic-locking version number.
        """
        ...

    async def get_step_history(
        self,
        saga_id: uuid.UUID,
    ) -> list[SagaLogEntry]:
        """
        Retrieve the chronological step log for a saga.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga whose step history to retrieve.

        Returns:
            list[SagaLogEntry]: Ordered list of step log entries for the saga, from oldest to newest.
        """
        ...

    async def commit(self) -> None:
        """
        Finalize the storage run by persisting and committing all pending changes made during this session.

        This method makes the run's checkpointed changes durable; the caller is responsible for invoking commit at logical checkpoints to persist session state.
        """

    async def rollback(self) -> None:
        """
        Abort the current storage run and revert any uncommitted changes in the session.

        This releases the run's transactional state without persisting pending updates so that the storage remains as it was before the run began.
        """


class ISagaStorage(abc.ABC):
    """Interface for saga persistence storage.

    Storage is responsible for persisting saga execution state so that:
    - Saga progress (status, context, step history) survives process restarts.
    - Recovery jobs can find interrupted sagas (RUNNING/COMPENSATING) and retry them.
    - Optimistic locking (version) prevents lost updates when multiple workers
      touch the same saga.
    """

    @abc.abstractmethod
    async def create_saga(
        self,
        saga_id: uuid.UUID,
        name: str,
        context: dict[str, typing.Any],
    ) -> None:
        """Create a new saga record in storage (initial state).

        Called when a saga is started for the first time. Creates the execution
        record with PENDING status, initial context, and version 1.

        Args:
            saga_id: Unique identifier of the saga (used as primary key).
            name: Saga name (e.g. handler/type name) for diagnostics and filtering.
            context: Initial context as a JSON-serializable dict (step inputs/outputs).
        """

    @abc.abstractmethod
    async def update_context(
        self,
        saga_id: uuid.UUID,
        context: dict[str, typing.Any],
        current_version: typing.Optional[int] = None,
    ) -> None:
        """Save saga context snapshot (e.g. after a step completes).

        Persists the current context so recovery can resume with up-to-date data.
        When current_version is provided, implements optimistic locking: update
        succeeds only if the stored version equals current_version (and version
        is incremented), otherwise a concurrent update is detected.

        Args:
            saga_id: The ID of the saga to update.
            context: The new context data (full snapshot, JSON-serializable).
            current_version: The expected current version of the saga execution.
                If provided, optimistic locking is used; if the stored version
                differs, the update is rejected.

        Raises:
            SagaConcurrencyError: If optimistic locking fails (version mismatch).
        """

    @abc.abstractmethod
    async def update_status(
        self,
        saga_id: uuid.UUID,
        status: SagaStatus,
    ) -> None:
        """Update the saga's global status.

        Status drives lifecycle: PENDING → RUNNING → COMPLETED, or RUNNING →
        COMPENSATING → FAILED. Used by execution and recovery to know whether
        to run steps, compensate, or consider the saga finished.

        Args:
            saga_id: The ID of the saga to update.
            status: New status (e.g. SagaStatus.RUNNING, SagaStatus.COMPLETED,
                SagaStatus.COMPENSATING, SagaStatus.FAILED).
        """

    @abc.abstractmethod
    async def log_step(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: typing.Optional[str] = None,
    ) -> None:
        """Append a step transition to the saga log.

        Used to record each step's outcome (started/completed/failed/compensated)
        so that recovery can determine which steps have already been executed
        and which need to be run or compensated.

        Args:
            saga_id: The ID of the saga this step belongs to.
            step_name: Name of the step (must match the step handler name).
            action: "act" for forward execution, "compensate" for compensation.
            status: Step outcome: STARTED, COMPLETED, FAILED, or COMPENSATED.
            details: Optional message (e.g. error text when status is FAILED).
        """

    @abc.abstractmethod
    async def load_saga_state(
        self,
        saga_id: uuid.UUID,
        *,
        read_for_update: bool = False,
    ) -> tuple[SagaStatus, dict[str, typing.Any], int]:
        """Load current saga status, context, and version.

        Used by execution and recovery to restore in-memory state. When
        read_for_update is True, the implementation may lock the row (e.g.
        SELECT FOR UPDATE) to avoid concurrent updates.

        Args:
            saga_id: The ID of the saga to load.
            read_for_update: If True, lock the row for update (e.g. for
                subsequent update_context with optimistic locking).

        Returns:
            Tuple of (status, context_dict, version). context_dict is the
            last persisted context; version is used for optimistic locking.
        """

    @abc.abstractmethod
    async def get_step_history(
        self,
        saga_id: uuid.UUID,
    ) -> list[SagaLogEntry]:
        """Return the ordered list of step log entries for the saga.

        Used by recovery to determine which steps completed successfully
        (and must be compensated in reverse order if compensating) and
        which steps still need to be executed.

        Args:
            saga_id: The ID of the saga whose step history to load.

        Returns:
            List of SagaLogEntry in chronological order (oldest first).
        """

    @abc.abstractmethod
    async def get_sagas_for_recovery(
        self,
        limit: int,
        max_recovery_attempts: int = 5,
        stale_after_seconds: typing.Optional[int] = None,
        saga_name: typing.Optional[str] = None,
    ) -> list[uuid.UUID]:
        """Return saga IDs that are candidates for recovery.

        Used by a recovery job/scheduler to find sagas that were left in
        RUNNING or COMPENSATING (e.g. process crash) and should be retried.
        Excludes COMPLETED and optionally limits by recovery attempts,
        staleness, and saga name to avoid re-processing fresh or repeatedly
        failing sagas.

        Args:
            limit: Maximum number of saga IDs to return per call.
            max_recovery_attempts: Only include sagas with recovery_attempts
                strictly less than this value. Sagas that have failed
                recovery this many times can be excluded (e.g. marked FAILED).
                Default 5.
            stale_after_seconds: If set, only include sagas whose updated_at
                is older than (now_utc - stale_after_seconds). Use this to
                avoid picking sagas that are currently being executed (recently
                updated). None means no staleness filter (backward compatible).
            saga_name: If set, only include sagas with this name (e.g. handler
                or type name). None means return all saga types (default).

        Returns:
            List of saga IDs (RUNNING or COMPENSATING only; FAILED/COMPLETED
            are not included), ordered by updated_at ascending, with
            recovery_attempts < max_recovery_attempts, and optionally
            updated_at older than the staleness threshold and name equal to
            saga_name when saga_name is provided.
        """

    @abc.abstractmethod
    async def increment_recovery_attempts(
        self,
        saga_id: uuid.UUID,
        new_status: typing.Optional[SagaStatus] = None,
    ) -> None:
        """Increment recovery attempt counter after a failed recovery run.

        Called when recovery of a saga fails (e.g. exception). Increments
        recovery_attempts and optionally sets status (e.g. to FAILED) so that
        get_sagas_for_recovery can exclude this saga or limit retries.

        Args:
            saga_id: The saga that failed recovery.
            new_status: If provided, set saga status to this value (e.g.
                SagaStatus.FAILED) in the same atomic update.
        """

    @abc.abstractmethod
    async def set_recovery_attempts(
        self,
        saga_id: uuid.UUID,
        attempts: int,
    ) -> None:
        """Set recovery attempt counter to an explicit value.

        Used to reset the counter after successfully recovering one of the
        steps (e.g. set to 0), or to set it to the maximum value so that
        get_sagas_for_recovery excludes this saga from further recovery
        (e.g. mark as permanently failed without changing status).

        Args:
            saga_id: The saga to update.
            attempts: The value to set recovery_attempts to (e.g. 0 to reset,
                or max_recovery_attempts to exclude from recovery).
        """

    def create_run(
        self,
    ) -> contextlib.AbstractAsyncContextManager[SagaStorageRun]:
        """
        Create a scoped async run context for a single saga execution session with checkpointed commits.

        The context manager yields a SagaStorageRun that provides the same mutation/read methods as the storage but does not commit automatically; the caller must call commit() or rollback() at desired checkpoints.

        Returns:
            contextlib.AbstractAsyncContextManager[SagaStorageRun]: Async context manager yielding a SagaStorageRun session.

        Raises:
            NotImplementedError: If the storage backend does not support scoped runs.
        """
        raise NotImplementedError("This storage does not support create_run()")
