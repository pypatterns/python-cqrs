import pytest

import cqrs
from cqrs.bootstrap import requests as requests_bootstrap
from cqrs.testing import TestContainer, test_harness

pytestmark = pytest.mark.sqlalchemy


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreatedNotice(cqrs.NotificationEvent):
    event_name: str = "order_created"
    topic: str = "order_events"
    payload: str = ""


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self, repository) -> None:
        self._repository = repository

    @property
    def events(self) -> list[cqrs.Event]:
        return []

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._repository.add(OrderCreatedNotice())
        return CreatedOrder(order_id="1")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


_SQLITE_OUTBOX = """
CREATE TABLE outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id BLOB NOT NULL,
    event_id_bin BLOB NOT NULL,
    event_status VARCHAR(32) NOT NULL,
    flush_counter SMALLINT NOT NULL DEFAULT 0,
    event_name VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    created_at DATETIME NOT NULL,
    payload BLOB NOT NULL,
    CONSTRAINT event_id_unique_index UNIQUE (event_id_bin, event_name)
)
"""


def _create_sqlite_outbox(sync_connection) -> None:
    sync_connection.exec_driver_sql(_SQLITE_OUTBOX)


async def test_sqlalchemy_outbox_produce_available_then_rollback() -> None:
    pytest.importorskip("aiosqlite")
    sqlalchemy_asyncio = pytest.importorskip("sqlalchemy.ext.asyncio")
    from cqrs.mapping.outbox import OutboxedEventMap

    engine = sqlalchemy_asyncio.create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(_create_sqlite_outbox)

    session = sqlalchemy_asyncio.AsyncSession(engine, expire_on_commit=False)
    try:
        event_map = OutboxedEventMap()
        event_map.register("order_created", OrderCreatedNotice)
        outbox = cqrs.SqlAlchemyOutboxedEventRepository(session, event_map=event_map)
        container = TestContainer()
        container.register(CreateOrderHandler, CreateOrderHandler(outbox))
        mediator = requests_bootstrap.bootstrap(container, commands_mapper=commands_mapper)
        async with test_harness.around(mediator, outbox=outbox) as harness:
            await mediator.send(CreateOrder(sku="book"))
            await harness.outbox.produce_available()
            assert harness.outbox.produced_count == 1
            harness.published.assert_one(OrderCreatedNotice, topic="order_events")
        await session.rollback()
    finally:
        await session.close()
        await engine.dispose()
