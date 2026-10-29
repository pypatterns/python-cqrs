"""
dependency-injector container wrapped by test_harness.around.

The adapter does not implement SupportsScope, so this bootstrap omits scope_strategy.

Run:
   python examples/testing/dependency_injector_container.py
"""

import asyncio

from dependency_injector import containers, providers

import cqrs
from cqrs.bootstrap import requests as bootstrap
from cqrs.container.dependency_injector import DependencyInjectorCQRSContainer
from cqrs.testing import test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder):
        return CreatedOrder(order_id="1")


class Container(containers.DeclarativeContainer):
    create_order_handler = providers.Factory(CreateOrderHandler)


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


async def main() -> None:
    container = Container()
    adapter = DependencyInjectorCQRSContainer()
    adapter.attach_external_container(container)
    mediator = bootstrap.bootstrap(
        di_container=adapter,
        commands_mapper=commands_mapper,
    )
    async with test_harness.around(mediator) as harness:
        await mediator.send(CreateOrder(sku="book"))
        assert harness.requests.contains(CreateOrder)


if __name__ == "__main__":
    asyncio.run(main())
