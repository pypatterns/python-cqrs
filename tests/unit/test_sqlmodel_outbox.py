"""SQLModel outbox repository on MySQL and PostgreSQL."""

import dataclasses
import os
import typing
import uuid

import pytest
import sqlalchemy as sqla
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import registry
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

import cqrs
from cqrs.compressors.zlib import ZlibCompressor
from cqrs.mapping.outbox import OutboxedEventMap
from cqrs.outbox import repository as outbox_repository
from cqrs.outbox.sqlmodel import (
    MAX_FLUSH_COUNTER_VALUE,
    Base,
    OutboxModel,
    SqlModelOutboxedEventRepository,
    rebind_outbox_model,
)
from cqrs.sqlmodel_base import SqlAlchemyExecuteSession

pytestmark = pytest.mark.sqlmodel

EVENT_NAME = "sqlmodel_outbox_event"


@dataclasses.dataclass
class Payload:
    message: str


class StoredEvent(cqrs.NotificationEvent):
    event_name: str
    payload: Payload


def _event_map() -> OutboxedEventMap:
    event_map = OutboxedEventMap()
    event_map.register(EVENT_NAME, StoredEvent)
    return event_map


def _event(message: str, topic: str = "orders") -> StoredEvent:
    return StoredEvent(
        event_name=EVENT_NAME,
        topic=topic,
        payload=Payload(message=message),
    )


def _live_dsns() -> list[tuple[str, str]]:
    candidates = [
        ("mysql", os.environ.get("DATABASE_DSN_MYSQL", "") or os.environ.get("DATABASE_DSN", "")),
        ("postgres", os.environ.get("DATABASE_DSN_POSTGRESQL", "")),
    ]
    return [(name, dsn) for name, dsn in candidates if dsn]


@pytest.fixture(params=_live_dsns() or [("missing", "")], ids=lambda item: item[0])
async def session(request: pytest.FixtureRequest):
    _name, dsn = request.param
    if not dsn:
        pytest.skip("DATABASE_DSN_MYSQL / DATABASE_DSN_POSTGRESQL not set")
    engine = create_async_engine(dsn, pool_pre_ping=True)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as opened:
        yield opened
    await engine.dispose()


def _repository(session: AsyncSession, **kwargs: object) -> SqlModelOutboxedEventRepository:
    return SqlModelOutboxedEventRepository(session, event_map=_event_map(), **kwargs)  # type: ignore[arg-type]


class TestSqlModelOutbox:
    async def test_add_commit_and_read_back(self, session: AsyncSession) -> None:
        repository = _repository(session)
        repository.add(_event("saved"))
        await repository.commit()

        events = await repository.get_many(10)
        assert len(events) == 1
        assert events[0].event.payload.message == "saved"
        assert events[0].event.event_name == EVENT_NAME
        assert events[0].status == outbox_repository.EventStatus.NEW
        assert isinstance(events[0].event.event_id, uuid.UUID)
        assert events[0].topic == "orders"
        assert events[0].payload_bytes is not None

    async def test_topic_filter(self, session: AsyncSession) -> None:
        repository = _repository(session)
        repository.add(_event("a", topic="alpha"))
        repository.add(_event("b", topic="beta"))
        await repository.commit()

        alpha = await repository.get_many(10, topic="alpha")
        assert [event.event.payload.message for event in alpha] == ["a"]

    async def test_rollback_discards_pending_insert(self, session: AsyncSession) -> None:
        repository = _repository(session)
        repository.add(_event("discarded"))
        await repository.rollback()

        assert await repository.get_many(10) == []

    async def test_produced_event_leaves_the_batch(self, session: AsyncSession) -> None:
        repository = _repository(session)
        repository.add(_event("done"))
        await repository.commit()
        [event] = await repository.get_many(10)
        await repository.update_status(event.id, outbox_repository.EventStatus.PRODUCED)
        await repository.commit()

        assert await repository.get_many(10) == []

    async def test_not_produced_orders_before_new_and_then_drops_out(self, session: AsyncSession) -> None:
        repository = _repository(session)
        repository.add(_event("fresh"))
        repository.add(_event("retry"))
        await repository.commit()
        stored = await repository.get_many(10)
        retry = next(event for event in stored if event.event.payload.message == "retry")
        fresh = next(event for event in stored if event.event.payload.message == "fresh")
        await repository.update_status(retry.id, outbox_repository.EventStatus.NOT_PRODUCED)
        await repository.commit()

        ordered = await repository.get_many(10)
        assert [event.id for event in ordered] == [fresh.id, retry.id]

        for _ in range(MAX_FLUSH_COUNTER_VALUE - 1):
            await repository.update_status(retry.id, outbox_repository.EventStatus.NOT_PRODUCED)
        await repository.commit()

        remaining = await repository.get_many(10)
        assert [event.id for event in remaining] == [fresh.id]

    async def test_undecodable_payload_spends_flush_budget(self, session: AsyncSession) -> None:
        repository = _repository(session)
        event_id = uuid.uuid4()
        session.add(
            OutboxModel(
                event_id=event_id,
                event_id_bin=event_id.bytes,
                event_name=EVENT_NAME,
                payload=b"not-valid-json{{{",
                topic="",
            ),
        )
        await session.commit()

        for _ in range(MAX_FLUSH_COUNTER_VALUE):
            assert await repository.get_many(1) == []
        await session.commit()

        flush_counter = (
            await SqlAlchemyExecuteSession(session).execute(sqla.text("SELECT flush_counter FROM outbox"))
        ).scalar_one()
        assert flush_counter == MAX_FLUSH_COUNTER_VALUE
        assert await repository.get_many(1) == []

    async def test_unknown_event_name_is_rejected(self, session: AsyncSession) -> None:
        repository = _repository(session)
        event = _event("nope")
        object.__setattr__(event, "event_name", "not_registered")
        with pytest.raises(TypeError, match="Unknown event name"):
            repository.add(event)

    async def test_compressor_round_trip(self, session: AsyncSession) -> None:
        repository = _repository(session, compressor=ZlibCompressor())
        repository.add(_event("compressed"))
        await repository.commit()

        raw_payload = (
            await SqlAlchemyExecuteSession(session).execute(sqla.text("SELECT payload FROM outbox"))
        ).scalar_one()
        assert raw_payload[:2] == b"\x78\x9c"

        [event] = await repository.get_many(1)
        assert event.event.payload.message == "compressed"

    async def test_duplicate_event_id_violates_unique_constraint(self, session: AsyncSession) -> None:
        repository = _repository(session)
        event = _event("duplicate")
        repository.add(event)
        repository.add(event)
        with pytest.raises(IntegrityError):
            await repository.commit()
        await repository.rollback()

    async def test_rebind_outbox_model(self, session: AsyncSession) -> None:
        original_base = OutboxModel.__bases__[0]
        original_table_name = typing.cast(typing.Any, OutboxModel).__table__.name
        custom_registry = registry()

        class AppSQLModel(SQLModel, registry=custom_registry):
            pass

        try:
            rebind_outbox_model(OutboxModel, AppSQLModel, "rebind_outbox")
            connection = await session.connection()
            await connection.execute(sqla.text("DROP TABLE IF EXISTS outbox"))
            await connection.execute(sqla.text("DROP TABLE IF EXISTS rebind_outbox"))
            await connection.run_sync(custom_registry.metadata.create_all)
            repository = _repository(session)
            repository.add(_event("rebound"))
            await repository.commit()
            stored = (
                await SqlAlchemyExecuteSession(session).execute(sqla.text("SELECT event_name FROM rebind_outbox"))
            ).scalar_one()
            assert stored == EVENT_NAME
        finally:
            await SqlAlchemyExecuteSession(session).execute(sqla.text("DROP TABLE IF EXISTS rebind_outbox"))
            await session.commit()
            rebind_outbox_model(OutboxModel, original_base, original_table_name)
