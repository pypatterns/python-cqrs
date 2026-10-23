"""
SQLModel inbox: same table, queries, and repository behavior as SQLAlchemy.

[AI Instruction]
Import ``SqlModelInboxRepository`` from ``cqrs``. It is lazy and needs the
``sqlmodel`` extra. Pass a ``sqlmodel.ext.asyncio.session.AsyncSession``.
"""

import datetime
import typing

from cqrs.inbox.map import InboxMessageMap
from cqrs.inbox.repository import InboxStatus

try:
    from cqrs.inbox.sqlalchemy import (
        DEFAULT_INBOX_TABLE_NAME,
        InboxModel as SqlAlchemyInboxModel,
        SqlAlchemyInboxRepository,
        rebind_inbox_model,
    )

    import sqlalchemy
    from sqlalchemy import func
    from sqlmodel import Field
    from sqlmodel.ext.asyncio.session import AsyncSession

    from cqrs import sqlalchemy_types
    from cqrs.sqlmodel_base import CqrsSQLModel, SqlAlchemyExecuteSession
except ImportError:
    raise ImportError(
        "You are trying to use the SQLModel inbox implementation, "
        "but 'sqlmodel' is not installed. "
        "Please install it using: pip install python-cqrs[sqlmodel]",
    ) from None

Base = CqrsSQLModel


class InboxModel(Base, table=True):
    __tablename__ = DEFAULT_INBOX_TABLE_NAME  # type: ignore[assignment]

    __table_args__ = (
        sqlalchemy.UniqueConstraint(
            "message_id",
            "message_name",
            name="inbox_message_unique_index",
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
    message_id: str = Field(
        sa_column=sqlalchemy.Column(
            sqlalchemy.String(255),
            nullable=False,
            comment="Delivery idempotency id",
        ),
    )
    message_name: str = Field(
        sa_column=sqlalchemy.Column(
            sqlalchemy.String(255),
            nullable=False,
            comment="Registered message name",
        ),
    )
    status: InboxStatus = Field(
        default=InboxStatus.NEW,
        sa_column=sqlalchemy.Column(
            sqlalchemy.Enum(InboxStatus),
            nullable=False,
            default=InboxStatus.NEW,
            comment="Inbox processing status",
        ),
    )
    flush_counter: int = Field(
        default=0,
        sa_column=sqlalchemy.Column(
            sqlalchemy.SmallInteger,
            nullable=False,
            default=0,
            comment="Failed processing attempts",
        ),
    )
    created_at: datetime.datetime | None = Field(
        default=None,
        sa_column=sqlalchemy.Column(
            sqlalchemy.DateTime,
            nullable=False,
            server_default=func.now(),
            comment="Row creation timestamp",
        ),
    )
    payload: bytes = Field(
        sa_column=sqlalchemy.Column(
            sqlalchemy_types.PayloadBinary,
            nullable=False,
            comment="Serialized request or event",
        ),
    )

    @classmethod
    def get_batch_query(
        cls,
        size: int,
        *,
        lock: bool = False,
    ) -> sqlalchemy.Select:
        query = SqlAlchemyInboxModel.get_batch_query
        return query.__func__(cls, size, lock=lock)  # type: ignore[attr-defined]

    @classmethod
    def update_status_query(
        cls,
        row_id: int,
        status: InboxStatus,
    ) -> sqlalchemy.Update:
        query = SqlAlchemyInboxModel.update_status_query
        return query.__func__(cls, row_id, status)  # type: ignore[attr-defined]


class SqlModelInboxRepository(SqlAlchemyInboxRepository):
    """
    Inbox on a SQLModel ``AsyncSession``.

    Behavior matches ``SqlAlchemyInboxRepository``: one savepoint per batch
    element, ``get_many`` with ``FOR UPDATE SKIP LOCKED`` on PostgreSQL and
    MySQL, and no commit inside ``add``.

    [AI Instruction]
    Install ``python-cqrs[sqlmodel]``. Pass the SQLModel session the
    presentation or the worker already opened.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        message_map: InboxMessageMap | None = None,
    ) -> None:
        super().__init__(
            typing.cast(typing.Any, SqlAlchemyExecuteSession(session)),
            message_map=message_map,
            model=InboxModel,
        )


__all__ = (
    "AsyncSession",
    "Base",
    "DEFAULT_INBOX_TABLE_NAME",
    "InboxModel",
    "SqlModelInboxRepository",
    "rebind_inbox_model",
)
