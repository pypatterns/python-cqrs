import contextlib
import datetime
import logging
import os
import typing
import uuid

import dotenv
from cqrs.dispatcher.exceptions import SagaConcurrencyError
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.models import SagaLogEntry
from cqrs.saga.storage.protocol import ISagaStorage, SagaStorageRun

try:
    import sqlalchemy
    from sqlalchemy import func
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from sqlalchemy.orm import registry
except ImportError:
    raise ImportError(
        "You are trying to use SQLAlchemy saga storage implementation, "
        "but 'sqlalchemy' is not installed. "
        "Please install it using: pip install python-cqrs[sqlalchemy]",
    ) from None

Base = registry().generate_base()
logger = logging.getLogger(__name__)

dotenv.load_dotenv()

DEFAULT_SAGA_EXECUTION_TABLE_NAME = "saga_executions"
DEFAULT_SAGA_LOG_TABLE_NAME = "saga_logs"

SAGA_EXECUTION_TABLE_NAME = os.getenv(
    "CQRS_SAGA_EXECUTION_TABLE_NAME",
    DEFAULT_SAGA_EXECUTION_TABLE_NAME,
)
SAGA_LOG_TABLE_NAME = os.getenv(
    "CQRS_SAGA_LOG_TABLE_NAME",
    DEFAULT_SAGA_LOG_TABLE_NAME,
)


class SagaExecutionModel(Base):
    __tablename__ = SAGA_EXECUTION_TABLE_NAME

    id = sqlalchemy.Column(
        sqlalchemy.Uuid,
        primary_key=True,
        nullable=False,
        comment="Saga ID",
    )
    name = sqlalchemy.Column(
        sqlalchemy.String(255),
        nullable=False,
        comment="Saga Name",
    )
    status = sqlalchemy.Column(
        sqlalchemy.Enum(SagaStatus),
        nullable=False,
        default=SagaStatus.PENDING,
        comment="Current status of the saga",
    )
    context = sqlalchemy.Column(
        sqlalchemy.JSON,
        nullable=False,
        comment="Serialized context",
    )
    version = sqlalchemy.Column(
        sqlalchemy.Integer,
        nullable=False,
        default=1,
        comment="Optimistic locking version",
    )
    created_at = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="Creation timestamp",
    )
    updated_at = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="Last update timestamp",
    )
    recovery_attempts = sqlalchemy.Column(
        sqlalchemy.Integer,
        nullable=False,
        default=0,
        server_default=sqlalchemy.text("0"),
        comment="Number of recovery attempts",
    )


class SagaLogModel(Base):
    __tablename__ = SAGA_LOG_TABLE_NAME

    id = sqlalchemy.Column(
        sqlalchemy.BigInteger(),
        sqlalchemy.Identity(),
        primary_key=True,
        nullable=False,
        autoincrement=True,
        comment="Log ID",
    )
    saga_id = sqlalchemy.Column(
        sqlalchemy.Uuid,
        sqlalchemy.ForeignKey(f"{SAGA_EXECUTION_TABLE_NAME}.id"),
        nullable=False,
        comment="Saga ID",
    )
    step_name = sqlalchemy.Column(
        sqlalchemy.String(255),
        nullable=False,
        comment="Name of the step",
    )
    action = sqlalchemy.Column(
        sqlalchemy.String(50),
        nullable=False,
        comment="Action performed (act/compensate)",
    )
    status = sqlalchemy.Column(
        sqlalchemy.Enum(SagaStepStatus),
        nullable=False,
        comment="Status of the step",
    )
    details = sqlalchemy.Column(
        sqlalchemy.Text,
        nullable=True,
        comment="Additional details or error message",
    )
    created_at = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="Timestamp of the log entry",
    )


class _SqlAlchemySagaStorageRun(SagaStorageRun):
    """Scoped run: one session, no commit inside methods; caller calls commit()."""

    def __init__(self, session: AsyncSession) -> None:
        """
        Initialize the run wrapper with an async SQLAlchemy session.

        Parameters:
            session (AsyncSession): The AsyncSession instance scoped to this run, used for all database operations.
        """
        self._session = session

    async def create_saga(
        self,
        saga_id: uuid.UUID,
        name: str,
        context: dict[str, typing.Any],
    ) -> None:
        """
        Create and stage a new saga execution record in the current session with initial metadata.

        Creates a SagaExecutionModel for the given saga identifier with status set to PENDING, version set to 1, and recovery_attempts set to 0, and adds it to the active session without committing the transaction.

        Parameters:
            saga_id (uuid.UUID): Unique identifier for the saga execution.
            name (str): Human-readable name of the saga.
            context (dict[str, Any]): Initial saga context to be stored (will be serialized to the model's JSON column).
        """
        execution = SagaExecutionModel(
            id=saga_id,
            name=name,
            status=SagaStatus.PENDING,
            context=context,
            version=1,
            recovery_attempts=0,
        )
        self._session.add(execution)

    async def update_context(
        self,
        saga_id: uuid.UUID,
        context: dict[str, typing.Any],
        current_version: typing.Optional[int] = None,
    ) -> None:
        """
        Update the stored context for a saga and increment its version, optionally enforcing an optimistic version check.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga to update.
            context (dict[str, typing.Any]): New serialized saga context to persist.
            current_version (typing.Optional[int ]): If provided, require the saga's current version to match this value before updating.

        Raises:
            SagaConcurrencyError: If an optimistic version check fails (indicating a concurrent modification) or if the saga does not exist when a version was supplied.
        """
        stmt = sqlalchemy.update(SagaExecutionModel).where(
            SagaExecutionModel.id == saga_id,
        )
        if current_version is not None:
            stmt = stmt.where(SagaExecutionModel.version == current_version)
        stmt = stmt.values(
            context=context,
            version=SagaExecutionModel.version + 1,
        )
        result = await self._session.execute(stmt)
        if result.rowcount == 0:  # type: ignore[attr-defined]
            if current_version is not None:
                check_stmt = sqlalchemy.select(SagaExecutionModel.id).where(
                    SagaExecutionModel.id == saga_id,
                )
                check_result = await self._session.execute(check_stmt)
                if check_result.scalar_one_or_none():
                    raise SagaConcurrencyError(
                        f"Saga {saga_id} was modified concurrently",
                    )
                raise SagaConcurrencyError(
                    f"Saga {saga_id} was modified concurrently or does not exist",
                )

    async def update_status(
        self,
        saga_id: uuid.UUID,
        status: SagaStatus,
    ) -> None:
        """
        Update the stored status of a saga execution and increment its optimistic-lock version.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga execution to update.
            status (SagaStatus): New status to set for the saga.

        Note:
            The update is executed in the active database session; a commit is required to persist the change.

        Raises:
            SagaConcurrencyError: If no row was updated (saga does not exist or was modified concurrently).
        """
        result = await self._session.execute(
            sqlalchemy.update(SagaExecutionModel)
            .where(SagaExecutionModel.id == saga_id)
            .values(
                status=status,
                version=SagaExecutionModel.version + 1,
            ),
        )
        if result.rowcount == 0:  # pyright: ignore[reportAttributeAccessIssue]
            raise SagaConcurrencyError(
                f"Saga {saga_id} does not exist or was modified concurrently",
            )

    async def log_step(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: typing.Optional[str] = None,
    ) -> None:
        """
        Record a saga step event by creating and staging a log entry in the active session.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga execution.
            step_name (str): Name of the step being recorded.
            action (Literal["act", "compensate"]): The performed action: "act" for normal action or "compensate" for compensation.
            status (SagaStepStatus): The step's outcome status.
            details (typing.Optional[str ]): Optional free-form details or error message associated with the step.
        """
        log_entry = SagaLogModel(
            saga_id=saga_id,
            step_name=step_name,
            action=action,
            status=status,
            details=details,
        )
        self._session.add(log_entry)

    async def load_saga_state(
        self,
        saga_id: uuid.UUID,
        *,
        read_for_update: bool = False,
    ) -> tuple[SagaStatus, dict[str, typing.Any], int]:
        """
        Load the current execution state for a saga.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga to load.
            read_for_update (bool): If true, acquire a row-level lock for update.

        Returns:
            tuple[SagaStatus, dict[str, Any], int]: The saga's status, its context dictionary, and the current version.

        Raises:
            ValueError: If no saga with the given id exists.
        """
        stmt = sqlalchemy.select(SagaExecutionModel).where(
            SagaExecutionModel.id == saga_id,
        )
        if read_for_update:
            stmt = stmt.with_for_update()
        result = await self._session.execute(stmt)
        execution = result.scalars().first()
        if not execution:
            raise ValueError(f"Saga {saga_id} not found")
        status_value: SagaStatus = typing.cast(SagaStatus, execution.status)
        context_value: dict[str, typing.Any] = typing.cast(
            dict[str, typing.Any],
            execution.context,
        )
        version_value: int = typing.cast(int, execution.version)
        return status_value, context_value, version_value

    async def get_step_history(
        self,
        saga_id: uuid.UUID,
    ) -> list[SagaLogEntry]:
        """
        Retrieve chronological step log entries for the given saga.

        Parameters:
            saga_id (uuid.UUID): UUID of the saga whose step history to fetch.

        Returns:
            list[SagaLogEntry]: List of log entries ordered by creation time. Each entry's `timestamp`
            is normalized to UTC if not already timezone-aware.
        """
        result = await self._session.execute(
            sqlalchemy.select(SagaLogModel).where(SagaLogModel.saga_id == saga_id).order_by(SagaLogModel.created_at),
        )
        rows = result.scalars().all()
        return [
            SagaLogEntry(
                saga_id=typing.cast(uuid.UUID, row.saga_id),
                step_name=typing.cast(str, row.step_name),
                action=typing.cast(typing.Literal["act", "compensate"], row.action),
                status=typing.cast(SagaStepStatus, row.status),
                timestamp=typing.cast(
                    datetime.datetime,
                    row.created_at.replace(tzinfo=datetime.timezone.utc)
                    if row.created_at.tzinfo is None
                    else row.created_at,
                ),
                details=typing.cast(typing.Optional[str], row.details),
            )
            for row in rows
        ]

    async def commit(self) -> None:
        """
        Commit the current transaction in the associated AsyncSession.
        """
        await self._session.commit()

    async def rollback(self) -> None:
        """
        Revert all staged changes in the current session's transaction.

        This aborts the in-progress transaction associated with the run's AsyncSession,
        discarding any pending writes or flushes.
        """
        await self._session.rollback()


class SqlAlchemySagaStorage(ISagaStorage):
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        """
        Initialize the SQLAlchemy-based saga storage with a factory for creating async sessions.

        Parameters:
            session_factory (async_sessionmaker[AsyncSession]): Factory that produces new AsyncSession instances used for each storage run and operation.
        """
        self.session_factory = session_factory

    def create_run(
        self,
    ) -> contextlib.AbstractAsyncContextManager[SagaStorageRun]:
        """
        Create a scoped run that yields a SagaStorageRun bound to a fresh session.

        The returned context manager provides a run object whose lifecycle is tied to a single session. If an exception is raised inside the context, the run's transaction is rolled back; the session is always closed on exit.

        Returns:
            A context manager that yields a `SagaStorageRun`. On exception within the context, the run's `rollback()` is invoked and the session is closed when the context exits.
        """

        @contextlib.asynccontextmanager
        async def _run() -> typing.AsyncGenerator[SagaStorageRun, None]:
            async with self.session_factory() as session:
                run = _SqlAlchemySagaStorageRun(session)
                try:
                    yield run
                except BaseException:
                    await run.rollback()
                    raise

        return _run()

    async def create_saga(
        self,
        saga_id: uuid.UUID,
        name: str,
        context: dict[str, typing.Any],
    ) -> None:
        """
        Create and persist a new saga execution record with initial metadata.

        Creates a SagaExecutionModel for the given saga_id and name, sets status to PENDING,
        version to 1, and recovery_attempts to 0, and commits it to the database.

        Parameters:
            saga_id (uuid.UUID): Unique identifier for the saga execution.
            name (str): Human-readable saga name.
            context (dict[str, typing.Any]): Initial saga context to store.

        Raises:
            SQLAlchemyError: If the database operation fails; the transaction is rolled back before the exception is propagated.
        """
        async with self.session_factory() as session:
            try:
                execution = SagaExecutionModel(
                    id=saga_id,
                    name=name,
                    status=SagaStatus.PENDING,
                    context=context,
                    version=1,
                    recovery_attempts=0,
                )
                session.add(execution)
                await session.commit()
            except SQLAlchemyError:
                await session.rollback()
                raise

    async def update_context(
        self,
        saga_id: uuid.UUID,
        context: dict[str, typing.Any],
        current_version: typing.Optional[int] = None,
    ) -> None:
        async with self.session_factory() as session:
            try:
                stmt = sqlalchemy.update(SagaExecutionModel).where(
                    SagaExecutionModel.id == saga_id,
                )
                if current_version is not None:
                    stmt = stmt.where(SagaExecutionModel.version == current_version)
                stmt = stmt.values(
                    context=context,
                    version=SagaExecutionModel.version + 1,
                )

                result = await session.execute(stmt)

                # Type ignore: SQLAlchemy Result from update() has rowcount attribute
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    # Check if saga exists to distinguish between "not found" and "concurrency error"
                    # But for now, we assume if rowcount is 0 and we checked version, it's concurrency
                    if current_version is not None:
                        # Double check if saga exists
                        check_stmt = sqlalchemy.select(SagaExecutionModel.id).where(
                            SagaExecutionModel.id == saga_id,
                        )
                        check_result = await session.execute(check_stmt)
                        if check_result.scalar_one_or_none():
                            raise SagaConcurrencyError(
                                f"Saga {saga_id} was modified concurrently",
                            )

                    # If we didn't check version or saga doesn't exist, maybe raise not found or ignore?
                    # Original implementation didn't check rowcount.
                    # If version check was requested and failed, we MUST raise error.
                    if current_version is not None:
                        raise SagaConcurrencyError(
                            f"Saga {saga_id} was modified concurrently or does not exist",
                        )

                await session.commit()
            except SQLAlchemyError:
                await session.rollback()
                raise

    async def update_status(
        self,
        saga_id: uuid.UUID,
        status: SagaStatus,
    ) -> None:
        async with self.session_factory() as session:
            try:
                await session.execute(
                    sqlalchemy.update(SagaExecutionModel)
                    .where(SagaExecutionModel.id == saga_id)
                    .values(
                        status=status,
                        version=SagaExecutionModel.version + 1,
                    ),
                )
                await session.commit()
            except SQLAlchemyError:
                await session.rollback()
                raise

    async def log_step(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: typing.Optional[str] = None,
    ) -> None:
        async with self.session_factory() as session:
            try:
                log_entry = SagaLogModel(
                    saga_id=saga_id,
                    step_name=step_name,
                    action=action,
                    status=status,
                    details=details,
                )
                session.add(log_entry)
                await session.commit()
            except SQLAlchemyError:
                await session.rollback()
                raise

    async def load_saga_state(
        self,
        saga_id: uuid.UUID,
        *,
        read_for_update: bool = False,
    ) -> tuple[SagaStatus, dict[str, typing.Any], int]:
        async with self.session_factory() as session:
            stmt = sqlalchemy.select(SagaExecutionModel).where(
                SagaExecutionModel.id == saga_id,
            )
            if read_for_update:
                stmt = stmt.with_for_update()

            result = await session.execute(stmt)
            execution = result.scalars().first()
            if not execution:
                raise ValueError(f"Saga {saga_id} not found")

            # Type assertions for SQLAlchemy ORM attributes
            status_value: SagaStatus = typing.cast(SagaStatus, execution.status)
            context_value: dict[str, typing.Any] = typing.cast(
                dict[str, typing.Any],
                execution.context,
            )
            version_value: int = typing.cast(int, execution.version)
            return status_value, context_value, version_value

    async def get_step_history(
        self,
        saga_id: uuid.UUID,
    ) -> list[SagaLogEntry]:
        async with self.session_factory() as session:
            result = await session.execute(
                sqlalchemy.select(SagaLogModel)
                .where(SagaLogModel.saga_id == saga_id)
                .order_by(SagaLogModel.created_at),
            )
            rows = result.scalars().all()

            return [
                SagaLogEntry(
                    saga_id=typing.cast(uuid.UUID, row.saga_id),
                    step_name=typing.cast(str, row.step_name),
                    action=typing.cast(typing.Literal["act", "compensate"], row.action),
                    status=typing.cast(SagaStepStatus, row.status),
                    timestamp=typing.cast(
                        datetime.datetime,
                        row.created_at.replace(tzinfo=datetime.timezone.utc)
                        if row.created_at.tzinfo is None
                        else row.created_at,
                    ),
                    details=typing.cast(typing.Optional[str], row.details),
                )
                for row in rows
            ]

    async def get_sagas_for_recovery(
        self,
        limit: int,
        max_recovery_attempts: int = 5,
        stale_after_seconds: typing.Optional[int] = None,
        saga_name: typing.Optional[str] = None,
    ) -> list[uuid.UUID]:
        recoverable = (
            SagaStatus.RUNNING,
            SagaStatus.COMPENSATING,
        )
        async with self.session_factory() as session:
            stmt = (
                sqlalchemy.select(SagaExecutionModel.id)
                .where(SagaExecutionModel.status.in_(recoverable))
                .where(SagaExecutionModel.recovery_attempts < max_recovery_attempts)
            )
            if saga_name is not None:
                stmt = stmt.where(SagaExecutionModel.name == saga_name)
            if stale_after_seconds is not None:
                threshold = datetime.datetime.now(
                    datetime.timezone.utc,
                ) - datetime.timedelta(
                    seconds=stale_after_seconds,
                )
                stmt = stmt.where(SagaExecutionModel.updated_at < threshold)
            stmt = stmt.order_by(SagaExecutionModel.updated_at.asc()).limit(limit)
            result = await session.execute(stmt)
            rows = result.scalars().all()
            return [typing.cast(uuid.UUID, row) for row in rows]

    async def increment_recovery_attempts(
        self,
        saga_id: uuid.UUID,
        new_status: typing.Optional[SagaStatus] = None,
    ) -> None:
        """
        Increment the recovery attempts counter for the given saga execution and optionally update its status.

        Parameters:
            saga_id (uuid.UUID): Identifier of the saga execution to update.
            new_status (typing.Optional[SagaStatus ]): If provided, set the saga's status to this value.

        Raises:
            ValueError: If no saga execution exists with the given `saga_id`.
            SQLAlchemyError: On database errors; the transaction is rolled back and the error is propagated.
        """
        async with self.session_factory() as session:
            try:
                values: dict[str, typing.Any] = {
                    "recovery_attempts": SagaExecutionModel.recovery_attempts + 1,
                    "version": SagaExecutionModel.version + 1,
                }
                if new_status is not None:
                    values["status"] = new_status
                result = await session.execute(
                    sqlalchemy.update(SagaExecutionModel).where(SagaExecutionModel.id == saga_id).values(**values),
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    raise ValueError(f"Saga {saga_id} not found")
                await session.commit()
            except SQLAlchemyError:
                await session.rollback()
                raise

    async def set_recovery_attempts(
        self,
        saga_id: uuid.UUID,
        attempts: int,
    ) -> None:
        async with self.session_factory() as session:
            try:
                result = await session.execute(
                    sqlalchemy.update(SagaExecutionModel)
                    .where(SagaExecutionModel.id == saga_id)
                    .values(
                        recovery_attempts=attempts,
                        version=SagaExecutionModel.version + 1,
                    ),
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    raise ValueError(f"Saga {saga_id} not found")
                await session.commit()
            except SQLAlchemyError:
                await session.rollback()
                raise
