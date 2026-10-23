import os
import tempfile
import typing

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

import cqrs
from cqrs.inbox.memory import MemoryInboxRepository
from cqrs.inbox.sqlalchemy import InboxModel, create_sqlite_inbox_table


class Deposit(cqrs.Request):
    account_id: str
    amount: str


class OrderCreated(cqrs.DomainEvent):
    order_id: str


class Ledger(DeclarativeBase):
    pass


class LedgerRow(Ledger):
    __tablename__ = "ledger"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    account_id: Mapped[str]


class _Mediator:
    def __init__(self) -> None:
        self.sent: list[object] = []

    async def send(self, message: object) -> None:
        self.sent.append(message)


def _names() -> cqrs.InboxMessageMap:
    message_map = cqrs.InboxMessageMap()
    message_map.register("Deposit", Deposit)
    message_map.register("OrderCreated", OrderCreated)
    return message_map


async def test_request_and_event_go_to_their_mediators():
    message_map = _names()
    repository = MemoryInboxRepository(message_map)
    await repository.add(
        [
            ("req", Deposit(account_id="acc-1", amount="10.00")),
            ("evt", OrderCreated(order_id="ord-1")),
        ],
    )
    requests = _Mediator()
    events = _Mediator()
    processor = cqrs.InboxProcessor(
        repository,
        message_map,
        request_mediator=requests,
        event_mediator=events,
    )

    for row in await repository.get_many():
        await processor.process(row)

    assert [type(message) for message in requests.sent] == [Deposit]
    assert [type(message) for message in events.sent] == [OrderCreated]
    assert requests.sent[0].account_id == "acc-1"  # type: ignore[attr-defined]
    assert events.sent[0].order_id == "ord-1"  # type: ignore[attr-defined]
    assert await repository.get_many() == []
    assert {row.status for row in repository._rows} == {cqrs.InboxStatus.PROCESSED}


async def test_dispatch_sends_subclasses_to_the_matching_mediator():
    requests = _Mediator()
    events = _Mediator()
    processor = cqrs.InboxProcessor(
        MemoryInboxRepository(),
        _names(),
        request_mediator=requests,
        event_mediator=events,
    )
    deposit = Deposit(account_id="acc-1", amount="10.00")
    created = OrderCreated(order_id="ord-1")

    await processor._dispatch(deposit)
    await processor._dispatch(created)

    assert requests.sent == [deposit]
    assert events.sent == [created]


async def test_dispatch_rejects_a_missing_mediator_and_an_unsupported_type():
    processor = cqrs.InboxProcessor(MemoryInboxRepository(), _names())
    with pytest.raises(RuntimeError, match="RequestMediator is not configured"):
        await processor._dispatch(Deposit(account_id="acc-1", amount="10.00"))
    with pytest.raises(RuntimeError, match="EventMediator is not configured"):
        await processor._dispatch(OrderCreated(order_id="ord-1"))

    configured = cqrs.InboxProcessor(
        MemoryInboxRepository(),
        _names(),
        request_mediator=_Mediator(),
        event_mediator=_Mediator(),
    )
    unsupported: typing.Any = object()
    with pytest.raises(TypeError, match="Unsupported inbox message type <class 'object'>"):
        await configured._dispatch(unsupported)


class _SessionMediator:
    def __init__(self, session, fail_accounts: set[str]) -> None:
        self._session = session
        self._fail_accounts = fail_accounts

    async def send(self, request: Deposit) -> None:
        self._session.add(LedgerRow(account_id=request.account_id))
        await self._session.flush()
        if request.account_id in self._fail_accounts:
            raise RuntimeError("handler failed")


@pytest.fixture
async def session_factory():
    descriptor, path = tempfile.mkstemp(suffix="-inbox-processor.sqlite")
    os.close(descriptor)
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{path}",
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as connection:
        await connection.run_sync(Ledger.metadata.create_all)
        await connection.run_sync(create_sqlite_inbox_table)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()
    os.remove(path)


@pytest.mark.sqlalchemy
async def test_handler_failure_does_not_roll_back_the_neighbor(session_factory):
    message_map = cqrs.InboxMessageMap()
    message_map.register("Deposit", Deposit)
    async with session_factory() as session:
        repository = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
        await repository.add(
            [
                ("bad", Deposit(account_id="bad", amount="1.00")),
                ("ok", Deposit(account_id="ok", amount="2.00")),
            ],
        )
        processor = cqrs.InboxProcessor(
            repository,
            message_map,
            request_mediator=_SessionMediator(session, {"bad"}),
        )
        for row in await repository.get_many(batch_size=10):
            await processor.process(row)

        visible = (await session.execute(select(LedgerRow.account_id))).scalars().all()
        assert list(visible) == ["ok"]
        await session.commit()

    async with session_factory() as session:
        ledger = (await session.execute(select(LedgerRow.account_id))).scalars().all()
        assert list(ledger) == ["ok"]
        rows = (await session.execute(select(InboxModel).order_by(InboxModel.id))).scalars().all()
        by_id = {row.message_id: row for row in rows}
        assert by_id["bad"].status is cqrs.InboxStatus.NOT_PROCESSED
        assert by_id["bad"].flush_counter == 1
        assert by_id["ok"].status is cqrs.InboxStatus.PROCESSED
        assert by_id["ok"].flush_counter == 0
