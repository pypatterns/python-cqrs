"""
Pydantic request. Validation fails in the constructor, before send.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_pydantic_request.py
"""

import di
import pydantic
import pytest

import cqrs
from cqrs.testing import test_harness

pytestmark = pytest.mark.pydantic


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.PydanticRequest[CreatedOrder]):
    sku: str = pydantic.Field(min_length=1)


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder):
        return CreatedOrder(order_id="1")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


def test_empty_sku_fails_before_send() -> None:
    # Arrange
    sku = ""

    # Act
    with pytest.raises(pydantic.ValidationError) as raised:
        CreateOrder(sku=sku)

    # Assert
    assert raised.value.error_count() >= 1


async def test_valid_request_returns_the_response() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        # Act
        result = await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        assert result.order_id == "1"
