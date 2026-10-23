"""
SQLAlchemy transactional inbox.

[AI Instruction]
Import ``SqlAlchemyInboxRepository`` and ``rebind_inbox_model`` from
``cqrs``. Both are lazy and need the ``sqlalchemy`` extra. ``add`` takes a
batch, uses one savepoint per element, and does not commit.
``get_many`` locks rows with ``FOR UPDATE SKIP LOCKED`` on PostgreSQL and
MySQL. SQLite has no skip lock; examples create the table with
``create_sqlite_inbox_table`` because ``Identity()`` is not an autoincrement
on SQLite.
"""

import contextlib
import datetime
import typing

from cqrs.inbox.map import InboxMessageMap, resolve_message_map
from cqrs.inbox.repository import (
    MAX_FLUSH_COUNTER_VALUE,
    InboxEntry,
    InboxMessage,
    InboxRepository,
    InboxStatus,
)

try:
    import sqlalchemy
    from sqlalchemy import func
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import DeclarativeMeta, Mapped, mapped_column, registry

    from cqrs import sqlalchemy_types
except ImportError:
    raise ImportError(
        "You are trying to use the SQLAlchemy inbox implementation, "
        "but 'sqlalchemy' is not installed. "
        "Please install it using: pip install python-cqrs[sqlalchemy]",
    ) from None


Base = registry().generate_base()

DEFAULT_INBOX_TABLE_NAME = "inbox"

_SKIP_LOCKED_DIALECTS = frozenset({"postgresql", "mysql", "mariadb"})

SQLITE_INBOX_DDL = """
CREATE TABLE IF NOT EXISTS inbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id VARCHAR(255) NOT NULL,
    message_name VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL,
    flush_counter SMALLINT NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    payload BLOB NOT NULL,
    CONSTRAINT inbox_message_unique_index UNIQUE (message_id, message_name)
)
"""


def create_sqlite_inbox_table(sync_connection: typing.Any) -> None:
    """
    Create the inbox table on SQLite.

    ``InboxModel.id`` uses ``Identity()``, which SQLite renders as
    ``BIGINT NOT NULL`` without autoincrement. Examples and SQLite tests call
    this instead of ``Base.metadata.create_all``. MySQL and PostgreSQL use
    ``Base.metadata`` as-is.
    """

    sync_connection.exec_driver_sql(SQLITE_INBOX_DDL)


class InboxModel(Base):
    __tablename__ = DEFAULT_INBOX_TABLE_NAME

    __table_args__ = (
        sqlalchemy.UniqueConstraint(
            "message_id",
            "message_name",
            name="inbox_message_unique_index",
        ),
    )

    id: Mapped[int] = mapped_column(
        sqlalchemy.BigInteger,
        sqlalchemy.Identity(),
        primary_key=True,
        nullable=False,
        autoincrement=True,
        comment="Identity",
    )
    message_id: Mapped[str] = mapped_column(
        sqlalchemy.String(255),
        nullable=False,
        comment="Delivery idempotency id",
    )
    message_name: Mapped[str] = mapped_column(
        sqlalchemy.String(255),
        nullable=False,
        comment="Registered message name",
    )
    status: Mapped[InboxStatus] = mapped_column(
        sqlalchemy.Enum(InboxStatus),
        nullable=False,
        default=InboxStatus.NEW,
        comment="Inbox processing status",
    )
    flush_counter: Mapped[int] = mapped_column(
        sqlalchemy.SmallInteger,
        nullable=False,
        default=0,
        comment="Failed processing attempts",
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        sqlalchemy.DateTime,
        nullable=False,
        default=datetime.datetime.now,
        server_default=func.now(),
        comment="Row creation timestamp",
    )
    payload: Mapped[bytes] = mapped_column(
        sqlalchemy_types.PayloadBinary,
        nullable=False,
        comment="Serialized request or event",
    )

    @classmethod
    def get_batch_query(cls, size: int, *, lock: bool = False) -> sqlalchemy.Select:
        query = (
            sqlalchemy.select(cls)
            .where(
                sqlalchemy.and_(
                    cls.status.in_(
                        [
                            InboxStatus.NEW,
                            InboxStatus.NOT_PROCESSED,
                        ],
                    ),
                    cls.flush_counter < MAX_FLUSH_COUNTER_VALUE,
                ),
            )
            .order_by(cls.id.asc())
            .limit(size)
            .execution_options(populate_existing=True)
        )
        if lock:
            query = query.with_for_update(skip_locked=True)
        return query

    @classmethod
    def update_status_query(
        cls,
        row_id: int,
        status: InboxStatus,
    ) -> sqlalchemy.Update:
        values: typing.Dict[str, typing.Any] = {
            "status": status,
            "flush_counter": cls.flush_counter,
        }
        if status == InboxStatus.NOT_PROCESSED:
            values["flush_counter"] += 1
        return sqlalchemy.update(cls).where(cls.id == row_id).values(**values)


class SqlAlchemyInboxRepository(InboxRepository):
    """
    Inbox on a SQLAlchemy ``AsyncSession``.

    [AI Instruction]
    Construct it with the presentation or worker session, not a session
    factory. ``add`` does not commit. Install ``python-cqrs[sqlalchemy]``.
    Register every message name on ``InboxMessageMap`` before the first
    write, including JSON.
    """

    def __init__(
        self,
        session: typing.Any,
        *,
        message_map: InboxMessageMap | None = None,
        model: typing.Any = None,
    ) -> None:
        self.session = session
        self._message_map = resolve_message_map(message_map)
        self._model = InboxModel if model is None else model

    async def add(
        self,
        messages: typing.Sequence[typing.Tuple[str, InboxEntry]],
    ) -> typing.List[bool]:
        if not messages:
            return []
        await self._prepare_savepoint()
        results: typing.List[bool] = []
        seen: typing.Set[typing.Tuple[str, str]] = set()
        for message_id, message in messages:
            name, payload = self._message_map.payload_for(message)
            key = (message_id, name)
            if key in seen:
                results.append(False)
                continue
            try:
                async with self.session.begin_nested():
                    self.session.add(
                        self._model(
                            message_id=message_id,
                            message_name=name,
                            payload=payload,
                            status=InboxStatus.NEW,
                            flush_counter=0,
                            created_at=datetime.datetime.now(),
                        ),
                    )
                    await self.session.flush()
            except IntegrityError:
                results.append(False)
                continue
            seen.add(key)
            results.append(True)
        return results

    async def _prepare_savepoint(self) -> None:
        """Keep a SQLite savepoint inside the caller's transaction.

        sqlite3 commits when ``RELEASE`` runs on a savepoint that started the
        transaction. Emit ``BEGIN`` first so ``add`` and ``nested`` can roll
        back with the outer session. PostgreSQL and MySQL already do this.
        """

        bind = self.session.get_bind()
        if getattr(getattr(bind, "dialect", None), "name", "") != "sqlite":
            return
        connection = await self.session.connection()
        raw = await connection.get_raw_connection()
        driver = raw.driver_connection
        sqlite_connection = getattr(driver, "_conn", driver)
        if getattr(sqlite_connection, "in_transaction", None) is False:
            await connection.exec_driver_sql("BEGIN")

    def _skip_locked(self) -> bool:
        bind = self.session.get_bind()
        return bind.dialect.name in _SKIP_LOCKED_DIALECTS

    def _to_message(self, row: typing.Any) -> InboxMessage:
        status = row.status
        if not isinstance(status, InboxStatus):
            if isinstance(status, str) and status in InboxStatus.__members__:
                status = InboxStatus[status]
            else:
                status = InboxStatus(status)
        payload = row.payload
        if not isinstance(payload, bytes):
            payload = bytes(payload)
        return InboxMessage(
            id=row.id,
            message_id=row.message_id,
            message_name=row.message_name,
            status=status,
            flush_counter=row.flush_counter,
            created_at=row.created_at,
            payload=payload,
        )

    async def get_many(self, batch_size: int = 100) -> typing.List[InboxMessage]:
        query = self._model.get_batch_query(batch_size, lock=self._skip_locked())
        rows = (await self.session.execute(query)).scalars().all()
        return [self._to_message(row) for row in rows]

    async def update_status(self, row_id: int, new_status: InboxStatus) -> None:
        await self.session.execute(
            statement=self._model.update_status_query(row_id, new_status),
        )

    def nested(self) -> contextlib.AbstractAsyncContextManager[None]:
        return self._nested()

    @contextlib.asynccontextmanager
    async def _nested(self) -> typing.AsyncIterator[None]:
        await self._prepare_savepoint()
        async with self.session.begin_nested():
            yield

    async def commit(self) -> None:
        await self.session.commit()

    async def rollback(self) -> None:
        await self.session.rollback()


def rebind_inbox_model(
    model: typing.Any,
    new_base: DeclarativeMeta,
    table_name: typing.Text | None = None,
) -> None:
    """
    Attach the inbox model to the application's declarative base.

    [AI Instruction]
    Call it once at startup, before ``create_all``, before migrations, and
    before the first inbox query. Pass a table name when the default
    ``inbox`` collides with an existing table.
    """

    model.__bases__ = (new_base,)
    model.__table__.name = table_name or model.__table__.name
    new_base.metadata._add_table(
        model.__table__.name,
        model.__table__.schema,
        model.__table__,
    )


__all__ = (
    "DEFAULT_INBOX_TABLE_NAME",
    "Base",
    "InboxModel",
    "SqlAlchemyInboxRepository",
    "create_sqlite_inbox_table",
    "rebind_inbox_model",
)
