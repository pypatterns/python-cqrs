import cqrs
from cqrs.testing import TestContainer, test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder) -> CreatedOrder:
        return CreatedOrder(order_id="1")


class DenyMiddleware:
    async def __call__(self, request: CreateOrder, handle) -> CreatedOrder:
        return CreatedOrder(order_id="denied")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


async def test_middleware_that_skips_handle_is_recorded() -> None:
    async with test_harness(
        TestContainer(),
        commands_mapper=commands_mapper,
        middlewares=[DenyMiddleware()],
    ) as harness:
        assert harness.mediator is not None
        result = await harness.mediator.send(CreateOrder(sku="book"))
        assert result.order_id == "denied"
        harness.invocations.assert_not_entered(CreateOrderHandler)
        assert harness.middlewares.of(DenyMiddleware)[0].called_next is False
        assert harness.requests.of(CreateOrder)[0].response.order_id == "denied"
