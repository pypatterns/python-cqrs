"""
Accept a batch of deposits, then drain the inbox on SQLite.

This script is the whole path without a broker. ``accept_deposits`` opens a
session, writes three items (two of them share a message id), commits, and
closes. ``drain_inbox`` then opens its own session and reads ``get_many`` with
``batch_size=2`` until the batch is empty. That session is the processing
transaction: the repository and the handler both use it, and the loop commits
after the batch. The ledger has two rows.

A FastStream subscriber does not do this. It only calls ``add`` and acks
after the commit. The processor runs in another process. See
``examples/inbox/README.md``.

Run::

    pip install -e ".[examples]"
    python examples/inbox/process_deposits.py
"""

from __future__ import annotations

import asyncio

import di
from di import dependent
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import StaticPool

import cqrs
from cqrs.bootstrap import requests as bootstrap
from cqrs.inbox.sqlalchemy import create_sqlite_inbox_table


class Business(DeclarativeBase):
    pass


class LedgerRow(Business):
    __tablename__ = "ledger"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    account_id: Mapped[str]
    amount: Mapped[str]


class ProcessDeposit(cqrs.Request[None]):
    account_id: str
    amount: str


MESSAGE_MAP = cqrs.InboxMessageMap()
MESSAGE_MAP.register("ProcessDeposit", ProcessDeposit)


class ProcessDepositHandler(cqrs.RequestHandler[ProcessDeposit, None]):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def handle(self, request: ProcessDeposit) -> None:
        self._session.add(LedgerRow(account_id=request.account_id, amount=request.amount))


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


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ProcessDeposit, ProcessDepositHandler)


def mediator_for(session: AsyncSession) -> cqrs.RequestMediator:
    """Give the registered handler the session this batch already opened."""

    container = di.Container()

    def session_provider() -> AsyncSession:
        return session

    container.bind(
        di.bind_by_type(
            dependent.Dependent(session_provider, scope="request"),
            AsyncSession,
        ),
    )
    return bootstrap.bootstrap(
        di_container=container,
        commands_mapper=commands_mapper,
        scope_strategy=cqrs.ScopeStrategy.NONE,
    )


async def accept_deposits() -> list[bool]:
    async with SessionLocal() as session:
        inbox = cqrs.SqlAlchemyInboxRepository(session, message_map=MESSAGE_MAP)
        stored = await inbox.add(
            [
                ("msg-1", ProcessDeposit(account_id="acc-1", amount="10.00")),
                ("msg-1", ProcessDeposit(account_id="acc-1", amount="10.00")),
                ("msg-2", ProcessDeposit(account_id="acc-2", amount="5.00")),
            ],
        )
        await session.commit()
        return stored


async def drain_inbox(batch_size: int = 2) -> int:
    processed = 0
    while True:
        async with SessionLocal() as session:
            inbox = cqrs.SqlAlchemyInboxRepository(session, message_map=MESSAGE_MAP)
            rows = await inbox.get_many(batch_size=batch_size)
            if not rows:
                return processed
            processor = cqrs.InboxProcessor(
                inbox,
                MESSAGE_MAP,
                request_mediator=mediator_for(session),
            )
            for row in rows:
                await processor.process(row)
            await session.commit()
            processed += len(rows)


async def main() -> None:
    await init_schema()
    stored = await accept_deposits()
    assert stored == [True, False, True], stored
    processed = await drain_inbox(batch_size=2)
    assert processed == 2, processed

    async with SessionLocal() as session:
        rows = (await session.execute(select(LedgerRow))).scalars().all()
    accounts = sorted(row.account_id for row in rows)
    assert accounts == ["acc-1", "acc-2"], accounts
    assert await drain_inbox(batch_size=2) == 0
    print("OK: inbox stored two deposits and the processor wrote two ledger rows")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
