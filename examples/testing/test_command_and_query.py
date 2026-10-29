"""
Command, query, and dataclass request recorded by test_harness.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_command_and_query.py
"""

import di

import cqrs
from cqrs.testing import test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class OrderView(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class ReadOrder(cqrs.Request[OrderView]):
    order_id: str


class OrderCreated(cqrs.DomainEvent):
    order_id: str


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._events.append(OrderCreated(order_id="1"))
        return CreatedOrder(order_id="1")


class ReadOrderHandler(cqrs.RequestHandler[ReadOrder]):
    async def handle(self, request: ReadOrder) -> OrderView:
        return OrderView(order_id=request.order_id)


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


def queries_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ReadOrder, ReadOrderHandler)


async def test_command_is_on_requests_and_invocations() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        # Act
        result = await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        assert result.order_id == "1"
        assert harness.requests.contains(CreateOrder)
        harness.invocations.assert_entered(CreateOrderHandler)


async def test_dataclass_response_is_on_requests() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        # Act
        await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        assert harness.requests.of(CreateOrder)[0].response == CreatedOrder(order_id="1")


async def test_query_leaves_delivery_journals_empty() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, queries_mapper=queries_mapper) as harness:
        # Act
        view = await harness.mediator.send(ReadOrder(order_id="1"))

        # Assert
        assert view.order_id == "1"
        harness.handled.assert_empty()
        harness.published.assert_empty()
        harness.outbox.assert_empty()
