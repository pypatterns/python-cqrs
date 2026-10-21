"""
SQLModel saga storage: same schema and operations as the SQLAlchemy storage.

[AI Instruction]
Import ``SqlModelSagaStorage`` from this module. It needs the ``sqlmodel``
extra and is not an attribute of ``cqrs``. The SQLModel outbox is
``cqrs.SqlModelOutboxedEventRepository``.
"""

import contextlib
import datetime
import typing
import uuid

try:
    from cqrs.saga.storage.sqlalchemy import (
        DEFAULT_SAGA_EXECUTION_TABLE_NAME,
        DEFAULT_SAGA_LOG_TABLE_NAME,
        SAGA_EXECUTION_TABLE_NAME,
        SAGA_LOG_TABLE_NAME,
        SqlAlchemySagaStorage,
    )

    import sqlalchemy
    from sqlalchemy import func
    from sqlalchemy.ext.asyncio import async_sessionmaker
    from sqlmodel import Field
    from sqlmodel.ext.asyncio.session import AsyncSession

    from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
    from cqrs.sqlalchemy_types import JSONType
    from cqrs.sqlmodel_base import CqrsSQLModel, SqlAlchemyExecuteSession
except ImportError:
    raise ImportError(
        "You are trying to use the SQLModel saga storage implementation, "
        "but 'sqlmodel' is not installed. "
        "Please install it using: pip install python-cqrs[sqlmodel]",
    ) from None

Base = CqrsSQLModel


class SagaExecutionModel(Base, table=True):
    __tablename__ = SAGA_EXECUTION_TABLE_NAME  # type: ignore[assignment]

    id: uuid.UUID = Field(
        sa_column=sqlalchemy.Column(
            sqlalchemy.Uuid,
            primary_key=True,
            nullable=False,
            comment="Saga ID",
        ),
    )
    name: str = Field(
        sa_column=sqlalchemy.Column(
            sqlalchemy.String(255),
            nullable=False,
            comment="Saga Name",
        ),
    )
    status: SagaStatus = Field(
        default=SagaStatus.PENDING,
        sa_column=sqlalchemy.Column(
            sqlalchemy.Enum(SagaStatus),
            nullable=False,
            default=SagaStatus.PENDING,
            comment="Current status of the saga",
        ),
    )
    context: dict[str, typing.Any] = Field(
        sa_column=sqlalchemy.Column(
            JSONType,
            nullable=False,
            comment="Serialized context",
        ),
    )
    version: int = Field(
        default=1,
        sa_column=sqlalchemy.Column(
            sqlalchemy.Integer,
            nullable=False,
            default=1,
            comment="Optimistic locking version",
        ),
    )
    created_at: datetime.datetime | None = Field(
        default=None,
        sa_column=sqlalchemy.Column(
            sqlalchemy.DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
            comment="Creation timestamp",
        ),
    )
    updated_at: datetime.datetime | None = Field(
        default=None,
        sa_column=sqlalchemy.Column(
            sqlalchemy.DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
            onupdate=func.now(),
            comment="Last update timestamp",
        ),
    )
    recovery_attempts: int = Field(
        default=0,
        sa_column=sqlalchemy.Column(
            sqlalchemy.Integer,
            nullable=False,
            default=0,
            server_default=sqlalchemy.text("0"),
            comment="Number of recovery attempts",
        ),
    )


class SagaLogModel(Base, table=True):
    __tablename__ = SAGA_LOG_TABLE_NAME  # type: ignore[assignment]

    id: int | None = Field(
        default=None,
        sa_column=sqlalchemy.Column(
            sqlalchemy.BigInteger(),
            sqlalchemy.Identity(),
            primary_key=True,
            nullable=False,
            autoincrement=True,
            comment="Log ID",
        ),
    )
    saga_id: uuid.UUID = Field(
        sa_column=sqlalchemy.Column(
            sqlalchemy.Uuid,
            sqlalchemy.ForeignKey(f"{SAGA_EXECUTION_TABLE_NAME}.id"),
            nullable=False,
            comment="Saga ID",
        ),
    )
    step_name: str = Field(
        sa_column=sqlalchemy.Column(
            sqlalchemy.String(255),
            nullable=False,
            comment="Name of the step",
        ),
    )
    action: str = Field(
        sa_column=sqlalchemy.Column(
            sqlalchemy.String(50),
            nullable=False,
            comment="Action performed (act/compensate)",
        ),
    )
    status: SagaStepStatus = Field(
        sa_column=sqlalchemy.Column(
            sqlalchemy.Enum(SagaStepStatus),
            nullable=False,
            comment="Status of the step",
        ),
    )
    details: str | None = Field(
        default=None,
        sa_column=sqlalchemy.Column(
            sqlalchemy.Text,
            nullable=True,
            comment="Additional details or error message",
        ),
    )
    created_at: datetime.datetime | None = Field(
        default=None,
        sa_column=sqlalchemy.Column(
            sqlalchemy.DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
            comment="Timestamp of the log entry",
        ),
    )


class SqlModelSagaStorage(SqlAlchemySagaStorage):
    """Saga storage that persists state in SQLModel tables.

    ``session_factory`` must be an ``async_sessionmaker`` built with
    ``class_=sqlmodel.ext.asyncio.session.AsyncSession``. Every operation of
    ``SqlAlchemySagaStorage`` is available, including checkpoint runs via
    ``create_run``, optimistic locking, and recovery queries.

    [AI Instruction]
    Same role as ``SqlAlchemySagaStorage`` when the application uses
    SQLModel sessions. Install the sqlmodel extra. Do not point saga storage
    at the outbox table.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        @contextlib.asynccontextmanager
        async def _open() -> typing.AsyncGenerator[SqlAlchemyExecuteSession, None]:
            async with session_factory() as session:
                yield SqlAlchemyExecuteSession(session)

        super().__init__(
            _open,  # type: ignore[arg-type]
            execution_model=SagaExecutionModel,
            log_model=SagaLogModel,
        )


__all__ = (
    "Base",
    "DEFAULT_SAGA_EXECUTION_TABLE_NAME",
    "DEFAULT_SAGA_LOG_TABLE_NAME",
    "SAGA_EXECUTION_TABLE_NAME",
    "SAGA_LOG_TABLE_NAME",
    "SagaExecutionModel",
    "SagaLogModel",
    "SqlModelSagaStorage",
)
