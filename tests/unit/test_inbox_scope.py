import os
import tempfile

import di
import pytest
from di import dependent
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

import cqrs
from cqrs.inbox.sqlalchemy import InboxModel, create_sqlite_inbox_table

pytestmark = pytest.mark.sqlalchemy


class Deposit(cqrs.Request):
    account_id: str
    amount: str


class Business(DeclarativeBase):
    pass


class AcceptedRow(Business):
    __tablename__ = "accepted"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    message_id: Mapped[str]


def _map() -> cqrs.InboxMessageMap:
    message_map = cqrs.InboxMessageMap()
    message_map.register("Deposit", Deposit)
    return message_map


@pytest.fixture
async def session_factory():
    descriptor, path = tempfile.mkstemp(suffix="-inbox-scope.sqlite")
    os.close(descriptor)
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{path}",
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as connection:
        await connection.run_sync(Business.metadata.create_all)
        await connection.run_sync(create_sqlite_inbox_table)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()
    os.remove(path)


def _container(session_factory, message_map: cqrs.InboxMessageMap) -> cqrs.ScopeAwareContainer:
    async def session_provider():
        session = session_factory()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    def inbox_from_session(session: AsyncSession) -> cqrs.SqlAlchemyInboxRepository:
        return cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)

    external = di.Container()
    external.bind(
        di.bind_by_type(
            dependent.Dependent(session_provider, scope="request"),
            AsyncSession,
        ),
    )
    external.bind(
        di.bind_by_type(
            dependent.Dependent(inbox_from_session, scope="request"),
            cqrs.InboxRepository,
        ),
    )
    root = cqrs.DIContainer()
    root.attach_external_container(external)
    return cqrs.ScopeAwareContainer(root)


async def _read(session_factory):
    async with session_factory() as session:
        inbox_ids = (await session.execute(select(InboxModel.message_id))).scalars().all()
        accepted_ids = (await session.execute(select(AcceptedRow.message_id))).scalars().all()
        return list(inbox_ids), list(accepted_ids)


async def test_open_scope_commits_inbox_and_presentation_row_together(session_factory):
    message_map = _map()
    container = _container(session_factory, message_map)

    async with container.open_scope() as scoped:
        async with cqrs.bind_scope(scoped):
            session = await scoped.resolve(AsyncSession)
            inbox = await scoped.resolve(cqrs.InboxRepository)
            assert isinstance(inbox, cqrs.SqlAlchemyInboxRepository)
            assert inbox.session is session
            await inbox.add([("m1", Deposit(account_id="acc-1", amount="10.00"))])
            session.add(AcceptedRow(message_id="m1"))

    inbox_ids, accepted_ids = await _read(session_factory)
    assert inbox_ids == ["m1"]
    assert accepted_ids == ["m1"]


async def test_open_scope_rolls_back_inbox_and_presentation_row_together(session_factory):
    message_map = _map()
    container = _container(session_factory, message_map)

    with pytest.raises(RuntimeError, match="presentation failed"):
        async with container.open_scope() as scoped:
            async with cqrs.bind_scope(scoped):
                session = await scoped.resolve(AsyncSession)
                inbox = await scoped.resolve(cqrs.InboxRepository)
                assert isinstance(inbox, cqrs.SqlAlchemyInboxRepository)
                assert inbox.session is session
                await inbox.add([("m2", Deposit(account_id="acc-2", amount="5.00"))])
                session.add(AcceptedRow(message_id="m2"))
                raise RuntimeError("presentation failed")

    assert await _read(session_factory) == ([], [])
