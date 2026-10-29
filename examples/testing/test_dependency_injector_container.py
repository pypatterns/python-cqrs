"""
dependency-injector container wrapped by test_harness.around.

The adapter does not implement SupportsScope, so this bootstrap omits scope_strategy.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_dependency_injector_container.py
"""

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


async def test_around_records_a_mediator_on_dependency_injector() -> None:
    # Arrange
    adapter = DependencyInjectorCQRSContainer()
    adapter.attach_external_container(Container())
    mediator = bootstrap.bootstrap(
        di_container=adapter,
        commands_mapper=commands_mapper,
    )
    async with test_harness.around(mediator) as harness:
        # Act
        await mediator.send(CreateOrder(sku="book"))

        # Assert
        assert harness.requests.contains(CreateOrder)
