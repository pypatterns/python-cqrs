"""
dishka container wrapped by test_harness.around.

Run:
   python examples/testing/dishka_container.py
"""

import asyncio

from dishka import Provider, Scope, make_async_container, provide

import cqrs
from cqrs.bootstrap import requests as bootstrap
from cqrs.container.dishka import DishkaCQRSContainer
from cqrs.testing import test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder):
        return CreatedOrder(order_id="1")


class AppProvider(Provider):
    create_order_handler = provide(CreateOrderHandler, scope=Scope.REQUEST)


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


async def main() -> None:
    container = make_async_container(AppProvider())
    mediator = bootstrap.bootstrap(
        di_container=DishkaCQRSContainer(container),
        commands_mapper=commands_mapper,
        scope_strategy=cqrs.ScopeStrategy.HANDLER,
    )
    async with test_harness.around(mediator) as harness:
        await mediator.send(CreateOrder(sku="book"))
        harness.invocations.assert_entered(CreateOrderHandler)


if __name__ == "__main__":
    asyncio.run(main())
