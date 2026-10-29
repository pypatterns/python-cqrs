"""
Middleware that returns a response and does not call the handler.

Run:
   python examples/testing/middleware.py
"""

import asyncio

import di

import cqrs
from cqrs.testing import test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder):
        return CreatedOrder(order_id="1")


class DenyMiddleware:
    async def __call__(self, request, handle):
        return CreatedOrder(order_id="denied")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


async def main() -> None:
    container = di.Container()
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        middlewares=[DenyMiddleware()],
    ) as harness:
        result = await harness.mediator.send(CreateOrder(sku="book"))
        assert result.order_id == "denied"
        harness.invocations.assert_not_entered(CreateOrderHandler)
        assert harness.middlewares.of(DenyMiddleware)[0].called_next is False


if __name__ == "__main__":
    asyncio.run(main())
