"""Inbox repository against MySQL from docker-compose-test.yml."""

import os

import pytest
import sqlalchemy as sqla
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, registry

import cqrs
from cqrs.inbox import sqlalchemy as inbox_sa

DATABASE_DSN = os.environ.get("DATABASE_DSN", "")


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
    if not DATABASE_DSN:
        pytest.skip("DATABASE_DSN not set")
    db = create_async_engine(DATABASE_DSN, pool_pre_ping=True)
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


async def test_batch_is_committed_and_read(session):
    repository = cqrs.SqlAlchemyInboxRepository(session, message_map=_map())
    stored = await repository.add(
        [
            ("m1", _deposit("acc-1")),
            ("m2", _deposit("acc-2")),
            ("m3", _deposit("acc-3")),
        ],
    )
    assert stored == [True, True, True]
    await session.commit()

    rows = await repository.get_many(batch_size=2)
    assert [row.message_id for row in rows] == ["m1", "m2"]
    assert await repository.get_many(batch_size=10)
    await session.commit()


async def test_duplicate_in_the_next_transaction_returns_false(session):
    repository = cqrs.SqlAlchemyInboxRepository(session, message_map=_map())
    assert await repository.add([("m1", _deposit("acc-1"))]) == [True]
    await session.commit()

    assert await repository.add([("m1", _deposit("acc-1")), ("m2", _deposit("acc-2"))]) == [
        False,
        True,
    ]
    await session.commit()


async def test_processed_row_is_not_selected(session):
    repository = cqrs.SqlAlchemyInboxRepository(session, message_map=_map())
    await repository.add([("m1", _deposit("acc-1")), ("m2", _deposit("acc-2"))])
    await session.commit()
    rows = await repository.get_many(batch_size=10)
    await repository.update_status(rows[0].id, cqrs.InboxStatus.PROCESSED)
    await session.commit()

    rest = await repository.get_many(batch_size=10)
    await session.commit()
    assert [row.id for row in rest] == [rows[1].id]


async def test_fifth_not_processed_leaves_the_batch(session):
    repository = cqrs.SqlAlchemyInboxRepository(session, message_map=_map())
    await repository.add([("m1", _deposit("acc-1"))])
    await session.commit()
    [row] = await repository.get_many(batch_size=1)
    for _ in range(inbox_sa.MAX_FLUSH_COUNTER_VALUE):
        await repository.update_status(row.id, cqrs.InboxStatus.NOT_PROCESSED)
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
        status = (
            await check.execute(sqla.select(inbox_sa.InboxModel.status).where(inbox_sa.InboxModel.message_id == "m1"))
        ).scalar_one()
        assert status is cqrs.InboxStatus.PROCESSED
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
            await connection.run_sync(custom_base.metadata.create_all)
            await connection.execute(sqla.text("SELECT * FROM rebind_inbox WHERE 1 = 0"))
    finally:
        async with engine.begin() as connection:
            await connection.exec_driver_sql("DROP TABLE IF EXISTS rebind_inbox")
        cqrs.rebind_inbox_model(inbox_sa.InboxModel, original_base, original_name)
