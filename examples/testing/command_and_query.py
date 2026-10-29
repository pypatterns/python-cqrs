"""
Command, query, and dataclass request recorded by test_harness.

Run:
   python examples/testing/command_and_query.py
"""

import asyncio

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


async def main() -> None:
    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        result = await harness.mediator.send(CreateOrder(sku="book"))
        assert result.order_id == "1"
        assert harness.requests.contains(CreateOrder)
        harness.invocations.assert_entered(CreateOrderHandler)

        await harness.mediator.send(CreateOrder(sku="book"))
        assert harness.requests.of(CreateOrder)[0].response == CreatedOrder(order_id="1")

    container = di.Container()
    async with test_harness(container, queries_mapper=queries_mapper) as harness:
        view = await harness.mediator.send(ReadOrder(order_id="1"))
        assert view.order_id == "1"
        harness.handled.assert_empty()
        harness.published.assert_empty()
        harness.outbox.assert_empty()


if __name__ == "__main__":
    asyncio.run(main())
