"""
Presentation scope: the extra case, where the route writes more than the inbox.

Use this only when the same presentation transaction must insert the inbox
row and another row. ``open_scope`` and ``bind_scope`` keep both writes on
one ``AsyncSession``. ``session_provider`` commits when the scope exits
without an error and rolls back when it raises. A route that only stores
the message depends on ``InboxRepository`` and calls ``add``. See
``fastapi_accept.py``. The processor opens a later session and does not see
this transaction until it has committed.

Run::

    pip install -e ".[examples]"
    python examples/inbox/presentation_scope.py
"""

from __future__ import annotations

import asyncio
import typing

import di
from di import dependent
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import StaticPool

import cqrs
from cqrs.inbox.sqlalchemy import InboxModel, create_sqlite_inbox_table


class Business(DeclarativeBase):
    pass


class AcceptedRow(Business):
    __tablename__ = "accepted"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    message_id: Mapped[str]


class ProcessDeposit(cqrs.Request[None]):
    account_id: str
    amount: str


MESSAGE_MAP = cqrs.InboxMessageMap()
MESSAGE_MAP.register("ProcessDeposit", ProcessDeposit)

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
        await connection.run_sync(Business.metadata.create_all)
        await connection.run_sync(create_sqlite_inbox_table)


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


def inbox_from_session(session: AsyncSession) -> cqrs.SqlAlchemyInboxRepository:
    return cqrs.SqlAlchemyInboxRepository(session, message_map=MESSAGE_MAP)


def build_container() -> cqrs.ScopeAwareContainer:
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


async def accept(container: cqrs.ScopeAwareContainer, message_id: str, message: cqrs.Request) -> None:
    async with container.open_scope() as scoped:
        async with cqrs.bind_scope(scoped):
            session = await scoped.resolve(AsyncSession)
            inbox = await scoped.resolve(cqrs.InboxRepository)
            assert isinstance(inbox, cqrs.SqlAlchemyInboxRepository)
            assert inbox.session is session
            await inbox.add([(message_id, message)])
            session.add(AcceptedRow(message_id=message_id))


async def _ids() -> tuple[list[str], list[str]]:
    async with SessionLocal() as session:
        inbox_ids = (await session.execute(select(InboxModel.message_id))).scalars().all()
        accepted_ids = (await session.execute(select(AcceptedRow.message_id))).scalars().all()
        return list(inbox_ids), list(accepted_ids)


async def main() -> None:
    await init_schema()
    container = build_container()
    await accept(container, "msg-1", ProcessDeposit(account_id="acc-1", amount="10.00"))
    assert await _ids() == (["msg-1"], ["msg-1"])

    async def accept_then_fail() -> None:
        async with container.open_scope() as scoped:
            async with cqrs.bind_scope(scoped):
                session = await scoped.resolve(AsyncSession)
                inbox = await scoped.resolve(cqrs.InboxRepository)
                await inbox.add([("msg-2", ProcessDeposit(account_id="acc-2", amount="5.00"))])
                session.add(AcceptedRow(message_id="msg-2"))
                raise RuntimeError("presentation failed")

    try:
        await accept_then_fail()
    except RuntimeError as exc:
        assert str(exc) == "presentation failed"
    else:
        raise AssertionError("presentation failure was swallowed")

    assert await _ids() == (["msg-1"], ["msg-1"])
    print("OK: inbox and accepted row commit together and roll back together")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
