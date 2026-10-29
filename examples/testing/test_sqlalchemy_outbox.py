"""
SQLAlchemy outbox on sqlite. The connection rolls back. No Docker.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_sqlalchemy_outbox.py
"""

import typing

import di
import di.dependent
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

import cqrs
from cqrs.bootstrap import requests as bootstrap
from cqrs.testing import test_harness

pytestmark = pytest.mark.sqlalchemy


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreatedNotice(cqrs.NotificationEvent):
    event_name: str = "order_created"
    topic: str = "order_events"


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self, repository: cqrs.OutboxedEventRepository) -> None:
        self._repository = repository

    async def handle(self, request: CreateOrder):
        self._repository.add(OrderCreatedNotice(payload={"order_id": "1"}))
        return CreatedOrder(order_id="1")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


def bind_outbox(container: di.Container, repository: cqrs.OutboxedEventRepository) -> None:
    def outbox_repository() -> cqrs.OutboxedEventRepository:
        return repository

    container.bind(
        di.bind_by_type(
            di.dependent.Dependent(outbox_repository, scope="request"),
            cqrs.OutboxedEventRepository,
        ),
    )


def _create_sqlite_outbox_table(sync_connection) -> None:
    # OutboxModel.id uses Identity(), which SQLite does not autoincrement.
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


@pytest.fixture
async def session() -> typing.AsyncIterator[AsyncSession]:
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as connection:
        await connection.run_sync(_create_sqlite_outbox_table)
        yield AsyncSession(bind=connection)
        await connection.rollback()
    await engine.dispose()


async def test_produce_publishes_the_stored_row(session: AsyncSession) -> None:
    # Arrange
    event_map = cqrs.OutboxedEventMap()
    event_map.register("order_created", OrderCreatedNotice)
    outbox = cqrs.SqlAlchemyOutboxedEventRepository(session, event_map=event_map)
    container = di.Container()
    bind_outbox(container, outbox)
    mediator = bootstrap.bootstrap(di_container=container, commands_mapper=commands_mapper)
    async with test_harness.around(mediator, outbox=outbox) as harness:
        await mediator.send(CreateOrder(sku="book"))

        # Act
        await harness.outbox.produce_available()

        # Assert
        assert harness.outbox.produced_count == 1
