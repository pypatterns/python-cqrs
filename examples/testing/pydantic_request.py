"""
Pydantic request. Validation fails in the constructor, before send.

Run:
   python examples/testing/pydantic_request.py
"""

import asyncio

import di
import pydantic
import pytest

import cqrs
from cqrs.testing import test_harness

# pydantic


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.PydanticRequest[CreatedOrder]):
    sku: str = pydantic.Field(min_length=1)


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder):
        return CreatedOrder(order_id="1")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


async def main() -> None:
    with pytest.raises(pydantic.ValidationError):
        CreateOrder(sku="")

    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        result = await harness.mediator.send(CreateOrder(sku="book"))
        assert result.order_id == "1"


if __name__ == "__main__":
    asyncio.run(main())
