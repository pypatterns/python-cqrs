import os
import tempfile

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import cqrs
from cqrs.inbox.sqlalchemy import create_sqlite_inbox_table

pytestmark = pytest.mark.sqlalchemy


class Deposit(cqrs.Request):
    account_id: str
    amount: str


def _map() -> cqrs.InboxMessageMap:
    message_map = cqrs.InboxMessageMap()
    message_map.register("Deposit", Deposit)
    return message_map


def _deposit(account_id: str = "acc-1", amount: str = "10.00") -> Deposit:
    return Deposit(account_id=account_id, amount=amount)


@pytest.fixture
async def session_factory():
    descriptor, path = tempfile.mkstemp(suffix="-inbox.sqlite")
    os.close(descriptor)
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{path}",
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as connection:
        await connection.run_sync(create_sqlite_inbox_table)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()
    os.remove(path)


async def test_create_sqlite_inbox_table_keeps_existing_rows(session_factory):
    message_map = _map()
    async with session_factory() as session:
        repository = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
        assert await repository.add([("m1", _deposit())]) == [True]
        await session.commit()

    async with session_factory() as session:
        connection = await session.connection()
        await connection.run_sync(create_sqlite_inbox_table)
        repository = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
        rows = await repository.get_many()
        assert [row.message_id for row in rows] == ["m1"]


async def test_duplicate_does_not_abort_the_outer_transaction(session_factory):
    message_map = _map()
    async with session_factory() as session:
        repository = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
        stored = await repository.add(
            [
                ("m1", _deposit("acc-1")),
                ("m1", _deposit("acc-1")),
                ("m2", _deposit("acc-2", "5.00")),
            ],
        )
        assert stored == [True, False, True]
        await session.commit()

    async with session_factory() as session:
        repository = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
        rows = await repository.get_many()
        assert {row.message_id for row in rows} == {"m1", "m2"}


async def test_add_does_not_commit(session_factory):
    message_map = _map()
    writer = session_factory()
    reader = session_factory()
    try:
        repository = cqrs.SqlAlchemyInboxRepository(writer, message_map=message_map)
        assert await repository.add([("m1", _deposit())]) == [True]
        other = cqrs.SqlAlchemyInboxRepository(reader, message_map=message_map)
        assert await other.get_many() == []
    finally:
        await writer.close()
        await reader.close()


async def test_rollback_drops_rows_commit_keeps_them(session_factory):
    message_map = _map()
    async with session_factory() as session:
        repository = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
        assert await repository.add([("m1", _deposit("acc-1")), ("m2", _deposit("acc-2"))]) == [
            True,
            True,
        ]
        await session.rollback()
        assert await repository.get_many() == []

    async with session_factory() as session:
        repository = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
        assert await repository.add([("m1", _deposit())]) == [True]
        await session.commit()

    async with session_factory() as session:
        repository = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
        rows = await repository.get_many()
        assert [row.message_id for row in rows] == ["m1"]
