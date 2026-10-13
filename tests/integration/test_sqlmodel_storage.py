"""SQLModel outbox and saga storage on MySQL and PostgreSQL.

Tables are created with checkfirst. Tests delete only the rows they insert so
they can share a database with the SQLAlchemy integration suite.
"""

import dataclasses
import os
import typing
import uuid

import pytest
import sqlalchemy as sqla
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

import cqrs
from cqrs.dispatcher.exceptions import SagaConcurrencyError
from cqrs.mapping.outbox import OutboxedEventMap
from cqrs.outbox import repository as outbox_repository
from cqrs.outbox.sqlmodel import SqlModelOutboxedEventRepository
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.sqlmodel import Base, SagaExecutionModel, SagaLogModel, SqlModelSagaStorage
from cqrs.sqlmodel_base import SqlAlchemyExecuteSession

pytestmark = pytest.mark.sqlmodel

EVENT_NAME = "sqlmodel_live_outbox_event"
TOPIC = "sqlmodel-live"


@dataclasses.dataclass
class Payload:
    message: str


class StoredEvent(cqrs.NotificationEvent):
    event_name: str
    payload: Payload


def _dsns() -> list[tuple[str, str]]:
    candidates = [
        ("mysql", os.environ.get("DATABASE_DSN_MYSQL", "") or os.environ.get("DATABASE_DSN", "")),
        ("postgres", os.environ.get("DATABASE_DSN_POSTGRESQL", "")),
    ]
    return [(name, dsn) for name, dsn in candidates if dsn]


@pytest.fixture(params=_dsns() or [("missing", "")], ids=lambda item: item[0])
async def live_factory(request: pytest.FixtureRequest):
    name, dsn = request.param
    if not dsn:
        pytest.skip("DATABASE_DSN_MYSQL / DATABASE_DSN_POSTGRESQL not set")
    engine = create_async_engine(dsn, pool_pre_ping=True)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    yield name, factory
    await engine.dispose()


def _event_map() -> OutboxedEventMap:
    event_map = OutboxedEventMap()
    event_map.register(EVENT_NAME, StoredEvent)
    return event_map


def _event(message: str) -> StoredEvent:
    return StoredEvent(
        event_name=EVENT_NAME,
        topic=TOPIC,
        payload=Payload(message=message),
    )


class TestSqlModelLiveDatabases:
    async def test_outbox_round_trip_enum_and_unique_constraint(self, live_factory) -> None:
        dialect, factory = live_factory
        message = f"hello-{dialect}-{uuid.uuid4()}"
        async with factory() as session:
            repository = SqlModelOutboxedEventRepository(session, event_map=_event_map())
            repository.add(_event(message))
            await repository.commit()

            events = await repository.get_many(100, topic=TOPIC)
            ours = [item for item in events if item.event.payload.message == message]
            assert len(ours) == 1
            assert isinstance(ours[0].event.event_id, uuid.UUID)
            assert ours[0].status == outbox_repository.EventStatus.NEW

            if dialect == "postgres":
                status_sql = (
                    "SELECT event_status::text FROM outbox WHERE event_name = :name AND topic = :topic "
                    "ORDER BY id DESC LIMIT 1"
                )
            else:
                status_sql = (
                    "SELECT event_status FROM outbox WHERE event_name = :name AND topic = :topic "
                    "ORDER BY id DESC LIMIT 1"
                )
            raw_status = (
                await SqlAlchemyExecuteSession(session).execute(
                    sqla.text(status_sql),
                    {"name": EVENT_NAME, "topic": TOPIC},
                )
            ).scalar_one()
            assert raw_status == "NEW"

            duplicate = _event(f"duplicate-{uuid.uuid4()}")
            repository.add(duplicate)
            repository.add(duplicate)
            with pytest.raises(IntegrityError):
                await repository.commit()
            await repository.rollback()

            await SqlAlchemyExecuteSession(session).execute(
                sqla.text("DELETE FROM outbox WHERE topic = :topic"),
                {"topic": TOPIC},
            )
            await session.commit()

    async def test_postgres_column_types(self, live_factory) -> None:
        dialect, factory = live_factory
        if dialect != "postgres":
            pytest.skip("column types are asserted on PostgreSQL")
        async with factory() as session:
            rows = await SqlAlchemyExecuteSession(session).execute(
                sqla.text(
                    "SELECT column_name, udt_name FROM information_schema.columns WHERE table_name = 'outbox'",
                ),
            )
            types = {row.column_name: row.udt_name for row in rows}
            assert types["event_id"] == "uuid"
            assert types["event_id_bin"] == "bytea"
            assert types["payload"] == "bytea"

    async def test_saga_lifecycle_lock_and_for_update(self, live_factory) -> None:
        _, factory = live_factory
        storage = SqlModelSagaStorage(factory)
        saga_id = uuid.uuid4()
        context = {"order_id": "live", "amount": "10"}
        await storage.create_saga(saga_id, "sqlmodel_live_saga", context)
        await storage.update_status(saga_id, SagaStatus.RUNNING)
        await storage.log_step(saga_id, "charge", "act", SagaStepStatus.COMPLETED)
        await storage.update_context(saga_id, {**context, "paid": "yes"}, current_version=2)

        status, stored, version = await storage.load_saga_state(saga_id, read_for_update=True)
        assert status == SagaStatus.RUNNING
        assert stored["paid"] == "yes"
        assert version == 3
        history = await storage.get_step_history(saga_id)
        assert history[0].step_name == "charge"

        with pytest.raises(SagaConcurrencyError):
            await storage.update_context(saga_id, {"stale": True}, current_version=1)

        async with factory() as session:
            await SqlAlchemyExecuteSession(session).execute(
                sqla.delete(SagaLogModel).where(typing.cast(typing.Any, SagaLogModel.saga_id) == saga_id),
            )
            await SqlAlchemyExecuteSession(session).execute(
                sqla.delete(SagaExecutionModel).where(typing.cast(typing.Any, SagaExecutionModel.id) == saga_id),
            )
            await session.commit()
