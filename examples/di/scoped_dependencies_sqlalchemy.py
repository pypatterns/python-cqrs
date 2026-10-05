"""
One SEND scope: command + domain event + outbox share one AsyncSession.

Do not construct SqlAlchemyOutboxedEventRepository(session_factory()) inside
the bind -- that would open a new session per resolve and break the shared
transaction. Bind the session as a generator, then build the outbox from
that same session.

Requires the examples extra (aiosqlite)::

    pip install -e ".[examples]"
    python examples/di/scoped_dependencies_sqlalchemy.py
"""

from __future__ import annotations

import asyncio
import typing

import di
import pydantic
from di import dependent
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import StaticPool

import cqrs
from cqrs.bootstrap import requests as bootstrap

# ---------------------------------------------------------------------------
# Schema (SQLite in-memory). OutboxModel.id uses Identity(), which SQLite
# does not autoincrement -- recreate that table by hand, same as fastapi_outbox.
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


class TaskRow(Base):
    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(primary_key=True)
    cancelled: Mapped[bool] = mapped_column(default=False)


class AuditRow(Base):
    __tablename__ = "audit"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    task_id: Mapped[int]
    note: Mapped[str]


def _create_sqlite_outbox_table(sync_connection: typing.Any) -> None:
    sync_connection.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id BLOB NOT NULL,
            event_id_bin BLOB NOT NULL,
            event_status VARCHAR(12) NOT NULL,
            flush_counter SMALLINT NOT NULL DEFAULT 0,
            event_name VARCHAR(255) NOT NULL,
            topic VARCHAR(255) NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
            payload BLOB NOT NULL,
            CONSTRAINT event_id_unique_index UNIQUE (event_id_bin, event_name)
        )
        """,
    )


engine = create_async_engine(
    "sqlite+aiosqlite://",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
SessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    engine,
    expire_on_commit=False,
)


async def init_schema() -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        await connection.run_sync(_create_sqlite_outbox_table)


# ---------------------------------------------------------------------------
# Scoped session: commit after handle + domain events, rollback on error.
# ---------------------------------------------------------------------------


async def session_provider() -> typing.AsyncIterator[AsyncSession]:
    session = SessionLocal()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


def outbox_from_session(
    session: AsyncSession,
) -> cqrs.SqlAlchemyOutboxedEventRepository:
    return cqrs.SqlAlchemyOutboxedEventRepository(session)


# ---------------------------------------------------------------------------
# Command + domain event + outbox notification
# ---------------------------------------------------------------------------


class TaskCancelledPayload(pydantic.BaseModel, frozen=True):
    task_id: int


cqrs.OutboxedEventMap.register(
    "scoped_task_cancelled",
    cqrs.NotificationEvent[TaskCancelledPayload],
)


class CancelTask(cqrs.Request):
    task_id: int


class TaskCancelled(cqrs.DomainEvent, frozen=True):
    task_id: int


class CancelTaskHandler(cqrs.RequestHandler[CancelTask, None]):
    def __init__(
        self,
        session: AsyncSession,
        outbox: cqrs.OutboxedEventRepository,
    ) -> None:
        self._session = session
        self._outbox = outbox
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return self._events

    async def handle(self, request: CancelTask) -> None:
        self._session.add(TaskRow(id=request.task_id, cancelled=True))
        self._outbox.add(
            cqrs.NotificationEvent[TaskCancelledPayload](
                event_name="scoped_task_cancelled",
                topic="tasks",
                payload=TaskCancelledPayload(task_id=request.task_id),
            ),
        )
        self._events.append(TaskCancelled(task_id=request.task_id))
        CancelTaskHandler.session_id = id(self._session)  # type: ignore[attr-defined]


class TaskCancelledHandler(cqrs.EventHandler[TaskCancelled]):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def handle(self, event: TaskCancelled) -> None:
        self._session.add(AuditRow(task_id=event.task_id, note="cancelled"))
        TaskCancelledHandler.session_id = id(self._session)  # type: ignore[attr-defined]


def setup_di() -> di.Container:
    container = di.Container()
    container.bind(
        di.bind_by_type(
            dependent.Dependent(session_provider, scope="request"),
            AsyncSession,
        ),
    )
    container.bind(
        di.bind_by_type(
            dependent.Dependent(outbox_from_session, scope="request"),
            cqrs.OutboxedEventRepository,
        ),
    )
    return container


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CancelTask, CancelTaskHandler)


def events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(TaskCancelled, TaskCancelledHandler)


async def main() -> None:
    await init_schema()
    mediator = bootstrap.bootstrap(
        di_container=setup_di(),
        commands_mapper=commands_mapper,
        domain_events_mapper=events_mapper,
        scope_strategy=cqrs.ScopeStrategy.SEND,
    )
    await mediator.send(CancelTask(task_id=7))

    assert CancelTaskHandler.session_id == TaskCancelledHandler.session_id  # type: ignore[attr-defined]

    async with SessionLocal() as session:
        tasks = (await session.execute(select(TaskRow))).scalars().all()
        audits = (await session.execute(select(AuditRow))).scalars().all()
        outbox = cqrs.SqlAlchemyOutboxedEventRepository(session)
        pending = await outbox.get_many(topic="tasks")

    assert len(tasks) == 1 and tasks[0].cancelled is True
    assert len(audits) == 1 and audits[0].task_id == 7
    assert len(pending) == 1
    print("OK: command, domain event, and outbox shared one AsyncSession")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
