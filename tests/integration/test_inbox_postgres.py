"""Inbox repository against PostgreSQL, including FOR UPDATE SKIP LOCKED."""

import os

import pytest
import sqlalchemy as sqla
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, registry

import cqrs
from cqrs.inbox import sqlalchemy as inbox_sa

DATABASE_DSN_POSTGRESQL = os.environ.get("DATABASE_DSN_POSTGRESQL", "")


class Deposit(cqrs.Request):
    account_id: str
    amount: str


class Business(DeclarativeBase):
    pass


class Ledger(Business):
    __tablename__ = "inbox_ledger"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(sqla.String(64), nullable=False)


def _map() -> cqrs.InboxMessageMap:
    message_map = cqrs.InboxMessageMap()
    message_map.register("Deposit", Deposit)
    return message_map


def _deposit(account_id: str) -> Deposit:
    return Deposit(account_id=account_id, amount="10.00")


@pytest.fixture
async def engine():
    if not DATABASE_DSN_POSTGRESQL:
        pytest.skip("DATABASE_DSN_POSTGRESQL not set")
    db = create_async_engine(DATABASE_DSN_POSTGRESQL, pool_pre_ping=True, pool_size=5)
    async with db.begin() as connection:
        await connection.run_sync(inbox_sa.Base.metadata.drop_all)
        await connection.run_sync(inbox_sa.Base.metadata.create_all)
        await connection.run_sync(Business.metadata.drop_all)
        await connection.run_sync(Business.metadata.create_all)
    yield db
    async with db.begin() as connection:
        await connection.run_sync(inbox_sa.Base.metadata.drop_all)
        await connection.run_sync(Business.metadata.drop_all)
    await db.dispose()


@pytest.fixture
async def session(engine):
    factory = async_sessionmaker(engine, expire_on_commit=False)
    db_session = factory()
    try:
        yield db_session
    finally:
        await db_session.close()


async def test_batch_duplicate_processed_and_flush_budget(session):
    repository = cqrs.SqlAlchemyInboxRepository(session, message_map=_map())
    stored = await repository.add(
        [
            ("m1", _deposit("acc-1")),
            ("m1", _deposit("acc-1")),
            ("m2", _deposit("acc-2")),
        ],
    )
    assert stored == [True, False, True]
    await session.commit()

    rows = await repository.get_many(batch_size=10)
    assert [row.message_id for row in rows] == ["m1", "m2"]
    await repository.update_status(rows[0].id, cqrs.InboxStatus.PROCESSED)
    await session.commit()
    rest = await repository.get_many(batch_size=10)
    assert [row.message_id for row in rest] == ["m2"]

    for _ in range(inbox_sa.MAX_FLUSH_COUNTER_VALUE):
        await repository.update_status(rest[0].id, cqrs.InboxStatus.NOT_PROCESSED)
    await session.commit()
    assert await repository.get_many(batch_size=10) == []


class _Writer:
    def __init__(self, session, *, fail: bool) -> None:
        self._session = session
        self._fail = fail

    async def send(self, request: Deposit) -> None:
        self._session.add(Ledger(account_id=request.account_id))
        await self._session.flush()
        if self._fail:
            raise RuntimeError("handler failed")


async def test_processor_commits_business_row_and_status_together(engine, session):
    message_map = _map()
    repository = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
    await repository.add([("m1", _deposit("acc-1"))])
    processor = cqrs.InboxProcessor(repository, message_map, request_mediator=_Writer(session, fail=False))
    [row] = await repository.get_many(batch_size=1)
    await processor.process(row)
    await session.commit()

    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as check:
        ledger = (await check.execute(sqla.select(Ledger.account_id))).scalars().all()
        assert list(ledger) == ["acc-1"]
        visible = await cqrs.SqlAlchemyInboxRepository(check, message_map=message_map).get_many()
        assert visible == []


async def test_processor_failure_keeps_not_processed_and_drops_the_business_row(engine, session):
    message_map = _map()
    repository = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
    await repository.add([("m1", _deposit("acc-1"))])
    processor = cqrs.InboxProcessor(repository, message_map, request_mediator=_Writer(session, fail=True))
    [row] = await repository.get_many(batch_size=1)
    await processor.process(row)
    await session.commit()

    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as check:
        ledger = (await check.execute(sqla.select(Ledger.account_id))).scalars().all()
        assert list(ledger) == []
        stored = (await check.execute(sqla.select(inbox_sa.InboxModel))).scalar_one()
        assert stored.status is cqrs.InboxStatus.NOT_PROCESSED
        assert stored.flush_counter == 1


async def test_rebind_puts_the_table_on_application_metadata(engine):
    original_base = inbox_sa.InboxModel.__bases__[0]
    original_name = inbox_sa.InboxModel.__table__.name
    custom_base = registry().generate_base()
    try:
        cqrs.rebind_inbox_model(inbox_sa.InboxModel, custom_base, "rebind_inbox")
        async with engine.begin() as connection:
            # PostgreSQL unique-constraint names are unique in the schema.
            # Drop the library table before creating the rebound one, same as
            # the outbox note about ``event_id_unique_index``.
            await connection.exec_driver_sql("DROP TABLE IF EXISTS inbox")
            await connection.run_sync(custom_base.metadata.create_all)
            await connection.execute(sqla.text("SELECT * FROM rebind_inbox WHERE false"))
    finally:
        async with engine.begin() as connection:
            await connection.exec_driver_sql("DROP TABLE IF EXISTS rebind_inbox")
        cqrs.rebind_inbox_model(inbox_sa.InboxModel, original_base, original_name)


async def _seed(engine, message_map: cqrs.InboxMessageMap, count: int) -> None:
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        repository = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
        messages = [(f"m{index}", _deposit(f"acc-{index}")) for index in range(count)]
        assert await repository.add(messages) == [True] * count
        await session.commit()


async def test_skip_locked_hides_a_held_row_until_commit(engine):
    message_map = _map()
    await _seed(engine, message_map, 3)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    session_a = factory()
    session_b = factory()
    try:
        await session_b.execute(sqla.text("SET lock_timeout = '3s'"))
        held = await cqrs.SqlAlchemyInboxRepository(session_a, message_map=message_map).get_many(batch_size=1)
        free = await cqrs.SqlAlchemyInboxRepository(session_b, message_map=message_map).get_many(batch_size=10)
        assert len(held) == 1
        assert held[0].id not in {row.id for row in free}
        assert {row.message_id for row in free} == {"m1", "m2"}
        await session_a.commit()
        again = await cqrs.SqlAlchemyInboxRepository(session_b, message_map=message_map).get_many(batch_size=10)
        assert held[0].id in {row.id for row in again}
        assert held[0].status is cqrs.InboxStatus.NEW
    finally:
        await session_a.close()
        await session_b.close()


async def test_skip_locked_row_stays_hidden_when_commit_marks_it_processed(engine):
    message_map = _map()
    await _seed(engine, message_map, 2)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    session_a = factory()
    session_b = factory()
    try:
        await session_b.execute(sqla.text("SET lock_timeout = '3s'"))
        repository_a = cqrs.SqlAlchemyInboxRepository(session_a, message_map=message_map)
        held = await repository_a.get_many(batch_size=1)
        free = await cqrs.SqlAlchemyInboxRepository(session_b, message_map=message_map).get_many(batch_size=10)
        assert held[0].id not in {row.id for row in free}
        await repository_a.update_status(held[0].id, cqrs.InboxStatus.PROCESSED)
        await session_a.commit()
        again = await cqrs.SqlAlchemyInboxRepository(session_b, message_map=message_map).get_many(batch_size=10)
        assert held[0].id not in {row.id for row in again}
    finally:
        await session_a.close()
        await session_b.close()
