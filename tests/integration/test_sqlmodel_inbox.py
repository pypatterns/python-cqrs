"""SQLModel inbox on MySQL and PostgreSQL.

Skipped when a DSN is missing, same as ``tests/integration/test_sqlmodel_storage.py``.
"""

import os
import typing
import uuid

import pytest
import sqlalchemy as sqla
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

import cqrs
from cqrs.inbox.sqlmodel import Base, InboxModel, SqlModelInboxRepository
from cqrs.sqlmodel_base import SqlAlchemyExecuteSession

pytestmark = pytest.mark.sqlmodel


class Deposit(cqrs.Request):
    account_id: str
    amount: str


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


async def test_add_get_many_and_duplicate(live_factory) -> None:
    name, factory = live_factory
    token = f"{name}-{uuid.uuid4().hex}"
    message_map = cqrs.InboxMessageMap()
    message_map.register("Deposit", Deposit)
    deposit = Deposit(account_id="acc-1", amount="10.00")
    async with factory() as session:
        repository = SqlModelInboxRepository(session, message_map=message_map)
        try:
            stored = await repository.add(
                [
                    (f"{token}-1", deposit),
                    (f"{token}-1", deposit),
                    (f"{token}-2", deposit),
                ],
            )
            assert stored == [True, False, True]
            await session.commit()

            rows = await repository.get_many(batch_size=1000)
            ours = [row for row in rows if row.message_id.startswith(token)]
            assert {row.message_id for row in ours} == {f"{token}-1", f"{token}-2"}

            assert await repository.add([(f"{token}-1", deposit)]) == [False]
            await session.commit()
        finally:
            await SqlAlchemyExecuteSession(session).execute(
                sqla.delete(InboxModel).where(
                    typing.cast(typing.Any, InboxModel.message_id).like(f"{token}%"),
                ),
            )
            await session.commit()
