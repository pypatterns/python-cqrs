"""
Middleware that returns a response and does not call the handler.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_middleware.py
"""

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


async def test_middleware_answers_without_the_handler() -> None:
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
