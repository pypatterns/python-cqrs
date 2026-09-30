"""
Example: FastAPI + Transactional Outbox end-to-end

FastAPI route → command → handler that writes business state and an outbox
event in one SQLAlchemy transaction, then a publisher stub drains the outbox.

Use case: a write that must both persist an order and notify other services.
The handler commits the order row and the outbox row together. A separate
endpoint (the publisher stub) reads NEW outbox rows and "publishes" them to an
in-memory broker. Swap the stub for KafkaMessageBroker in production.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Default: SQLite, no Docker
--------------------------
   pip install -e ".[examples]"
   python examples/outbox/fastapi_outbox.py

The server listens on http://localhost:8000
Open http://localhost:8000/docs and:

1. POST /orders  {"order_id": "ord-1", "amount": 19.99}
2. GET  /orders               → business row is stored
3. GET  /outbox               → pending outbox row (status=new)
4. POST /outbox/publish       → publisher stub sends the event
5. GET  /published            → stub broker received the message
6. GET  /outbox               → empty (row marked produced)

Or with curl (server already running):

   curl -s -X POST http://localhost:8000/orders \
     -H "Content-Type: application/json" \
     -d '{"order_id": "ord-1", "amount": 19.99}'
   curl -s http://localhost:8000/orders
   curl -s http://localhost:8000/outbox
   curl -s -X POST http://localhost:8000/outbox/publish
   curl -s http://localhost:8000/published

Optional: MySQL via docker compose
----------------------------------
   docker compose -f docker-compose-dev.yml up -d mysql_dev
   pip install -e ".[examples]" asyncmy
   DATABASE_URL=mysql+asyncmy://cqrs:cqrs@localhost:3307/cqrs \
     python examples/outbox/fastapi_outbox.py

Optional: PostgreSQL via docker compose
---------------------------------------
   docker compose -f docker-compose-dev.yml up -d postgres_dev
   pip install -e ".[examples]" asyncpg
   DATABASE_URL=postgresql+asyncpg://cqrs:cqrs@localhost:5433/cqrs \
     python examples/outbox/fastapi_outbox.py

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. FastAPI route delegates to RequestMediator.send()
2. Handler writes an order row and an outbox event on the same session
3. SqlAlchemyOutboxedEventRepository.commit() persists both
4. EventProducer + an in-memory MessageBroker stub drains the outbox

================================================================================
REQUIREMENTS
================================================================================

   pip install -e ".[examples]"

Default DATABASE_URL is sqlite+aiosqlite:///./fastapi_outbox.db
(aiosqlite is included in the examples extra).

================================================================================
"""

from __future__ import annotations

import contextlib
import dataclasses
import logging
import os
import typing

import di
import fastapi
import pydantic
import sqlalchemy
import uvicorn
from di import dependent
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

import cqrs
from cqrs.message_brokers import protocol as broker_protocol
from cqrs.outbox.sqlalchemy import Base as OutboxBase
from cqrs.requests import bootstrap

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fastapi_outbox")

ORDER_CREATED_EVENT = "order_created"
ORDER_EVENTS_TOPIC = "order_events"

DEFAULT_DATABASE_URL = "sqlite+aiosqlite:///./fastapi_outbox.db"


class BusinessBase(DeclarativeBase):
    pass


class OrderRow(BusinessBase):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(sqlalchemy.String(64), primary_key=True)
    amount: Mapped[float] = mapped_column(sqlalchemy.Float, nullable=False)
    status: Mapped[str] = mapped_column(sqlalchemy.String(32), nullable=False)


class OrderCreatedPayload(pydantic.BaseModel, frozen=True):
    order_id: str
    amount: float


def _register_outbox_event() -> None:
    if cqrs.OutboxedEventMap.get(ORDER_CREATED_EVENT) is None:
        cqrs.OutboxedEventMap.register(
            ORDER_CREATED_EVENT,
            cqrs.NotificationEvent[OrderCreatedPayload],
        )


_register_outbox_event()


class CreateOrderCommand(cqrs.Request):
    order_id: str
    amount: float


class CreateOrderResponse(cqrs.Response):
    order_id: str
    status: str


@dataclasses.dataclass
class Persistence:
    """Shared SQLAlchemy session for business writes and the outbox."""

    session: AsyncSession
    outbox: cqrs.OutboxedEventRepository


class CreateOrderHandler(cqrs.RequestHandler[CreateOrderCommand, CreateOrderResponse]):
    def __init__(self, persistence: Persistence) -> None:
        self.persistence = persistence

    async def handle(self, request: CreateOrderCommand) -> CreateOrderResponse:
        order = OrderRow(
            id=request.order_id,
            amount=request.amount,
            status="created",
        )
        self.persistence.session.add(order)
        self.persistence.outbox.add(
            cqrs.NotificationEvent[OrderCreatedPayload](
                event_name=ORDER_CREATED_EVENT,
                topic=ORDER_EVENTS_TOPIC,
                payload=OrderCreatedPayload(
                    order_id=request.order_id,
                    amount=request.amount,
                ),
            ),
        )
        try:
            await self.persistence.outbox.commit()
            return CreateOrderResponse(order_id=order.id, status=order.status)
        finally:
            await self.persistence.session.close()


class StubMessageBroker(broker_protocol.MessageBroker):
    """In-memory publisher used instead of Kafka/RabbitMQ."""

    def __init__(self) -> None:
        self.published: list[broker_protocol.Message] = []

    async def send_message(self, message: broker_protocol.Message) -> None:
        self.published.append(message)
        logger.info(
            "Published %s (%s) to %s",
            message.message_name,
            message.message_id,
            message.topic,
        )


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrderCommand, CreateOrderHandler)


def setup_di(session_factory: async_sessionmaker[AsyncSession]) -> di.Container:
    container = di.Container()

    def persistence_factory() -> Persistence:
        session = session_factory()
        return Persistence(
            session=session,
            outbox=cqrs.SqlAlchemyOutboxedEventRepository(session),
        )

    container.bind(
        di.bind_by_type(
            dependent.Dependent(persistence_factory, scope="request"),
            Persistence,
        ),
    )
    return container


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


def build_engine(url: str) -> AsyncEngine:
    kwargs: dict[str, typing.Any] = {"echo": False}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_async_engine(url, **kwargs)


def _create_sqlite_outbox_table(sync_connection: typing.Any) -> None:
    """
    OutboxModel.id uses sqlalchemy.Identity(), which SQLite renders as
    ``BIGINT NOT NULL`` without AUTOINCREMENT. Recreate the table so inserts
    can omit ``id``. MySQL and PostgreSQL use OutboxBase.metadata as-is.
    """
    sync_connection.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id BLOB NOT NULL,
            event_id_bin BLOB NOT NULL,
            event_status VARCHAR(12) NOT NULL,
            flush_counter SMALLINT NOT NULL,
            event_name VARCHAR(255) NOT NULL,
            topic VARCHAR(255) NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
            payload BLOB NOT NULL,
            CONSTRAINT event_id_unique_index UNIQUE (event_id_bin, event_name)
        )
        """,
    )


async def init_schema(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(BusinessBase.metadata.create_all)
        if engine.dialect.name == "sqlite":
            await connection.run_sync(_create_sqlite_outbox_table)
        else:
            await connection.run_sync(OutboxBase.metadata.create_all)


def _outboxed_event_as_dict(item: cqrs.OutboxedEvent) -> dict[str, typing.Any]:
    return {
        "id": item.id,
        "topic": item.topic,
        "status": str(item.status),
        "event": item.event.to_dict(),
    }


engine = build_engine(database_url())
SessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    engine,
    expire_on_commit=False,
)
STUB_BROKER = StubMessageBroker()
api_router = fastapi.APIRouter()


def mediator_factory() -> cqrs.RequestMediator:
    return bootstrap.bootstrap(
        di_container=setup_di(SessionLocal),
        commands_mapper=commands_mapper,
    )


@api_router.post("/orders", status_code=fastapi.status.HTTP_201_CREATED)
async def create_order(
    command: CreateOrderCommand,
    mediator: cqrs.RequestMediator = fastapi.Depends(mediator_factory),
) -> CreateOrderResponse:
    return await mediator.send(command)


@api_router.get("/orders")
async def list_orders() -> list[dict[str, typing.Any]]:
    async with SessionLocal() as session:
        rows = (await session.execute(sqlalchemy.select(OrderRow))).scalars().all()
        return [{"id": row.id, "amount": row.amount, "status": row.status} for row in rows]


@api_router.get("/outbox")
async def list_outbox() -> list[dict[str, typing.Any]]:
    async with SessionLocal() as session:
        repository = cqrs.SqlAlchemyOutboxedEventRepository(session)
        items = await repository.get_many(batch_size=100)
        return [_outboxed_event_as_dict(item) for item in items]


@api_router.post("/outbox/publish")
async def publish_outbox() -> dict[str, int]:
    async with SessionLocal() as session:
        repository = cqrs.SqlAlchemyOutboxedEventRepository(session)
        producer = cqrs.EventProducer(STUB_BROKER, repository)
        batch = await repository.get_many(batch_size=100)
        for item in batch:
            await producer.send_message(item)
        await repository.commit()
        return {"published": len(batch)}


@api_router.get("/published")
async def list_published() -> list[dict[str, typing.Any]]:
    # JSON default: payload stays a dict; payload_bytes/content_type/headers are omitted.
    return [message.to_dict() for message in STUB_BROKER.published]


@contextlib.asynccontextmanager
async def lifespan(_app: fastapi.FastAPI) -> typing.AsyncIterator[None]:
    await init_schema(engine)
    yield
    await engine.dispose()


app = fastapi.FastAPI(
    title="python-cqrs FastAPI + Outbox example",
    lifespan=lifespan,
)
app.include_router(api_router)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
