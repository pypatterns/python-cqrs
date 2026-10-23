"""
FastStream 0.5 subscriber: the Kafka batch goes straight into ``add``.

``batch=True`` is FastStream, not the inbox. The subscriber does not call a
mediator. ``inbox_repository`` opens a session, yields
``SqlAlchemyInboxRepository``, commits when the subscriber returns, and rolls
back when it raises. FastStream acks the batch after that generator exits, so
the offset is committed only after the database commit. ``False`` from ``add``
is still ack: that message was already stored. An exception rolls the batch
back and skips ack, so Kafka delivers it again.

Each idempotency key is ``{offset}-{timestamp}``, the id FastStream assigns
to one Kafka record. An event subscriber is the same shape with a
``NotificationEvent`` or ``DomainEvent`` in the batch instead of a command.

Run (Kafka must be reachable)::

    pip install -e ".[examples]"
    python examples/inbox/faststream_accept.py
"""

from __future__ import annotations

import typing

import faststream
from faststream import kafka
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import cqrs
from cqrs.inbox.sqlalchemy import create_sqlite_inbox_table


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

broker = kafka.KafkaBroker("localhost:9092")


async def inbox_repository() -> typing.AsyncIterator[cqrs.InboxRepository]:
    session = SessionLocal()
    try:
        yield cqrs.SqlAlchemyInboxRepository(session, message_map=MESSAGE_MAP)
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


@broker.subscriber(
    "account-deposits",
    batch=True,
    max_records=500,
    auto_commit=False,
    group_id="deposits-inbox",
)
async def on_deposits(
    bodies: list[dict],
    message: kafka.KafkaMessage,
    inbox: cqrs.InboxRepository = faststream.Depends(inbox_repository),
) -> None:
    records = message.raw_message
    if not isinstance(records, tuple):
        records = (records,)
    await inbox.add(
        [
            (
                f"{record.offset}-{record.timestamp}",
                ProcessDeposit(
                    account_id=body["account_id"],
                    amount=body["amount"],
                ),
            )
            for body, record in zip(bodies, records)
        ],
    )


async def init_schema() -> None:
    async with engine.begin() as connection:
        await connection.run_sync(create_sqlite_inbox_table)


if __name__ == "__main__":
    import asyncio

    async def _main() -> None:
        await init_schema()
        app = faststream.FastStream(broker)
        await app.run()

    asyncio.run(_main())
