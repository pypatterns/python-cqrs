"""
Features that take no part in send: Mermaid diagrams, the zlib compressor,
outbox rebind for migrations, and a broker captured instead of Kafka or AMQP.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_outside_harness.py
"""

import di
import pytest
import sqlalchemy
from sqlalchemy import orm

import cqrs
from cqrs.bootstrap import requests as bootstrap
from cqrs.testing import CapturingMessageBroker, test_harness


class Paid(cqrs.Response):
    method: str


class PayOrder(cqrs.Request[Paid]):
    method: str


class CardHandler(cqrs.handlers.CORRequestHandler[PayOrder]):
    async def handle(self, request: PayOrder):
        if request.method != "card":
            return await self.next(request)
        return Paid(method="card")


class CashHandler(cqrs.handlers.CORRequestHandler[PayOrder]):
    async def handle(self, request: PayOrder):
        return Paid(method="cash")


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreatedNotice(cqrs.NotificationEvent):
    event_name: str = "order_created"
    topic: str = "order_events"


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder):
        self._events.append(OrderCreatedNotice(payload={"order_id": "1"}))
        return CreatedOrder(order_id="1")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


@pytest.fixture
def outbox_model():
    from cqrs.outbox.sqlalchemy import OutboxModel

    original_base = OutboxModel.__bases__[0]
    original_name = OutboxModel.__table__.name
    yield OutboxModel
    cqrs.rebind_outbox_model(OutboxModel, original_base, original_name)


def test_cor_sequence_diagram_names_each_step() -> None:
    diagram = cqrs.CoRMermaid([CardHandler, CashHandler])

    sequence = diagram.sequence()

    assert "CardHandler" in sequence
    assert "CashHandler" in sequence


def test_zlib_round_trip_returns_the_payload() -> None:
    compressor = cqrs.ZlibCompressor()
    payload = b'{"order_id": "1"}'

    restored = compressor.decompress(compressor.compress(payload))

    assert restored == payload


@pytest.mark.sqlalchemy
def test_rebind_puts_the_outbox_table_on_the_application_metadata(outbox_model) -> None:
    Base = orm.registry().generate_base()
    engine = sqlalchemy.create_engine("sqlite://")

    cqrs.rebind_outbox_model(outbox_model, Base, "app_outbox")
    Base.metadata.create_all(engine)

    assert "app_outbox" in sqlalchemy.inspect(engine).get_table_names()


async def test_capturing_broker_records_the_notice() -> None:
    broker = CapturingMessageBroker()
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        message_broker=broker,
        commands_mapper=commands_mapper,
    )
    async with test_harness.around(mediator, broker=broker) as harness:
        await mediator.send(CreateOrder(sku="book"))

        harness.published.assert_one(OrderCreatedNotice, topic="order_events")
