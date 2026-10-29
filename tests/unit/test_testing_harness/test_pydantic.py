import pytest

import cqrs
from cqrs.testing import TestContainer, test_harness

pytestmark = pytest.mark.pydantic


async def test_pydantic_validation_happens_before_send() -> None:
    pydantic = pytest.importorskip("pydantic")

    class CreatedOrder(cqrs.PydanticResponse):
        order_id: str

    class CreateOrder(cqrs.PydanticRequest[CreatedOrder]):
        sku: str = pydantic.Field(min_length=1)

    class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
        async def handle(self, request: CreateOrder) -> CreatedOrder:
            return CreatedOrder(order_id="1")

    def commands_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(CreateOrder, CreateOrderHandler)

    with pytest.raises(pydantic.ValidationError):
        CreateOrder(sku="")

    async with test_harness(TestContainer(), commands_mapper=commands_mapper) as harness:
        assert harness.mediator is not None
        result = await harness.mediator.send(CreateOrder(sku="book"))
        assert result.order_id == "1"
        assert harness.requests.contains(CreateOrder)
