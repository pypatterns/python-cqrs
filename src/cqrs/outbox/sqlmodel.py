"""SQLModel outbox: same table, queries, and repository behavior as SQLAlchemy."""

import datetime
import typing
import uuid

from cqrs import compressors
from cqrs.mapping import outbox as map
from cqrs.outbox import repository
from cqrs.outbox.sqlalchemy import (
    MAX_FLUSH_COUNTER_VALUE,
    BinaryUUID,
    OutboxModel as SqlAlchemyOutboxModel,
    SqlAlchemyOutboxedEventRepository,
    rebind_outbox_model,
)
from cqrs.serializers.protocol import EventCodec

try:
    import sqlalchemy
    from sqlalchemy import func
    from sqlmodel import Field
    from sqlmodel.ext.asyncio.session import AsyncSession

    from cqrs import sqlalchemy_types
    from cqrs.sqlmodel_base import CqrsSQLModel, SqlAlchemyExecuteSession
except ImportError:
    raise ImportError(
        "You are trying to use the SQLModel outbox implementation, "
        "but 'sqlmodel' is not installed. "
        "Please install it using: pip install python-cqrs[sqlmodel]",
    ) from None

Base = CqrsSQLModel

DEFAULT_OUTBOX_TABLE_NAME = "outbox"


class OutboxModel(Base, table=True):
    __tablename__ = DEFAULT_OUTBOX_TABLE_NAME  # type: ignore[assignment]

    __table_args__ = (
        sqlalchemy.UniqueConstraint(
            "event_id_bin",
            "event_name",
            name="event_id_unique_index",
        ),
    )

    id: int | None = Field(
        default=None,
        sa_column=sqlalchemy.Column(
            sqlalchemy.BigInteger,
            sqlalchemy.Identity(),
            primary_key=True,
            nullable=False,
            autoincrement=True,
            comment="Identity",
        ),
    )
    event_id: uuid.UUID = Field(
        sa_column=sqlalchemy.Column(
            sqlalchemy_types.UUIDBinary,
            nullable=False,
            comment="Event idempotency id",
        ),
    )
    event_id_bin: bytes = Field(
        sa_column=sqlalchemy.Column(
            sqlalchemy_types.Binary16,
            nullable=False,
            comment="Event idempotency id in 16 bit presentation",
        ),
    )
    event_status: repository.EventStatus = Field(
        default=repository.EventStatus.NEW,
        sa_column=sqlalchemy.Column(
            sqlalchemy.Enum(repository.EventStatus),
            nullable=False,
            default=repository.EventStatus.NEW,
            comment="Event producing status",
        ),
    )
    flush_counter: int = Field(
        default=0,
        sa_column=sqlalchemy.Column(
            sqlalchemy.SmallInteger,
            nullable=False,
            default=0,
            comment="Event producing flush counter",
        ),
    )
    event_name: str = Field(
        sa_column=sqlalchemy.Column(
            sqlalchemy.String(255),
            nullable=False,
            comment="Event name",
        ),
    )
    topic: str = Field(
        default="",
        sa_column=sqlalchemy.Column(
            sqlalchemy.String(255),
            nullable=False,
            default="",
            comment="Event topic",
        ),
    )
    created_at: datetime.datetime | None = Field(
        default=None,
        sa_column=sqlalchemy.Column(
            sqlalchemy.DateTime,
            nullable=False,
            server_default=func.now(),
            comment="Event creation timestamp",
        ),
    )
    payload: bytes = Field(
        sa_column=sqlalchemy.Column(
            sqlalchemy_types.PayloadBinary,
            nullable=False,
            comment="Event payload",
        ),
    )

    def row_to_dict(self) -> typing.Dict[typing.Text, typing.Any]:
        table = typing.cast(typing.Any, self).__table__
        return {column.name: getattr(self, column.name) for column in table.columns}

    @classmethod
    def get_batch_query(
        cls,
        size: int,
        topic: typing.Text | None = None,
    ) -> sqlalchemy.Select:
        query = SqlAlchemyOutboxModel.get_batch_query
        return query.__func__(cls, size, topic)  # type: ignore[attr-defined]

    @classmethod
    def update_status_query(
        cls,
        outboxed_event_id: int,
        status: repository.EventStatus,
    ) -> sqlalchemy.Update:
        query = SqlAlchemyOutboxModel.update_status_query
        return query.__func__(cls, outboxed_event_id, status)  # type: ignore[attr-defined]

    @classmethod
    def status_sorting_case(cls) -> sqlalchemy.Case:
        query = SqlAlchemyOutboxModel.status_sorting_case
        return query.__func__(cls)  # type: ignore[attr-defined]


class SqlModelOutboxedEventRepository(SqlAlchemyOutboxedEventRepository):
    """Outbox repository that stores rows as SQLModel ``OutboxModel`` instances.

    Pass a ``sqlmodel.ext.asyncio.session.AsyncSession``. Behavior matches
    ``SqlAlchemyOutboxedEventRepository``: the same batch query, flush budget,
    status ordering, codecs, and compressor hooks.
    """

    def __init__(
        self,
        session: AsyncSession,
        compressor: compressors.Compressor | None = None,
        *,
        serializer: EventCodec | None = None,
        event_map: map.OutboxedEventMap | None = None,
    ):
        super().__init__(
            typing.cast(typing.Any, SqlAlchemyExecuteSession(session)),
            compressor,
            serializer=serializer,
            event_map=event_map,
            model=OutboxModel,
        )


__all__ = (
    "AsyncSession",
    "Base",
    "BinaryUUID",
    "DEFAULT_OUTBOX_TABLE_NAME",
    "MAX_FLUSH_COUNTER_VALUE",
    "OutboxModel",
    "SqlModelOutboxedEventRepository",
    "rebind_outbox_model",
)
