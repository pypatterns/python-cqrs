"""
FastAPI accepts one command or one event per request and returns 202.

The route does not call a mediator. ``add`` receives a batch of one element.
``inbox_repository`` opens a session, yields ``SqlAlchemyInboxRepository``,
commits when the route returns, and rolls back when it raises. A repeat of
the same idempotency key is still 202: ``add`` returns ``[False]`` and does not
insert a second row.

Run::

    pip install -e ".[examples]"
    python examples/inbox/fastapi_accept.py
"""

from __future__ import annotations

import contextlib
import typing

import fastapi
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import cqrs
from cqrs.inbox.sqlalchemy import create_sqlite_inbox_table


class ProcessDeposit(cqrs.Request[None]):
    account_id: str
    amount: str


class OrderCreated(cqrs.DomainEvent):
    order_id: str


MESSAGE_MAP = cqrs.InboxMessageMap()
MESSAGE_MAP.register("ProcessDeposit", ProcessDeposit)
MESSAGE_MAP.register("OrderCreated", OrderCreated)

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
        await connection.run_sync(create_sqlite_inbox_table)


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


@contextlib.asynccontextmanager
async def lifespan(app: fastapi.FastAPI):
    await init_schema()
    yield
    await engine.dispose()


app = fastapi.FastAPI(lifespan=lifespan)


@app.post("/deposits", status_code=202)
async def accept_deposit(
    body: ProcessDeposit,
    idempotency_key: str = fastapi.Header(alias="Idempotency-Key"),
    inbox: cqrs.InboxRepository = fastapi.Depends(inbox_repository),
) -> dict:
    await inbox.add([(idempotency_key, body)])
    return {"accepted": True}


@app.post("/events/order-created", status_code=202)
async def accept_order_created(
    body: OrderCreated,
    message_id: str = fastapi.Header(alias="X-Message-Id"),
    inbox: cqrs.InboxRepository = fastapi.Depends(inbox_repository),
) -> dict:
    await inbox.add([(message_id, body)])
    return {"accepted": True}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
