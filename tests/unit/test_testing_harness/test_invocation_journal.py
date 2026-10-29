import cqrs
from cqrs.testing import TestContainer, test_harness
from cqrs.testing.container import _RECORDING


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder) -> CreatedOrder:
        return CreatedOrder(order_id="1")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


async def test_shared_handler_records_in_the_active_harness() -> None:
    handler = CreateOrderHandler()
    container = TestContainer()
    container.register(CreateOrderHandler, handler)

    async with test_harness(container, commands_mapper=commands_mapper) as first:
        assert first.mediator is not None
        await first.mediator.send(CreateOrder(sku="book"))
        first.invocations.assert_entered(CreateOrderHandler)

    async with test_harness(container, commands_mapper=commands_mapper) as second:
        assert second.mediator is not None
        await second.mediator.send(CreateOrder(sku="book"))
        second.invocations.assert_entered(CreateOrderHandler)

    assert getattr(handler.handle, _RECORDING, False) is False


async def test_nested_harness_records_into_its_own_journal() -> None:
    handler = CreateOrderHandler()
    container = TestContainer()
    container.register(CreateOrderHandler, handler)

    async with test_harness(container, commands_mapper=commands_mapper) as outer:
        assert outer.mediator is not None
        await outer.mediator.send(CreateOrder(sku="book"))
        assert len(outer.invocations) == 1
        async with test_harness.around(outer.mediator) as inner:
            await outer.mediator.send(CreateOrder(sku="book"))
            inner.invocations.assert_entered(CreateOrderHandler)
            assert len(outer.invocations) == 1
        await outer.mediator.send(CreateOrder(sku="book"))
        assert len(outer.invocations) == 2

    assert getattr(handler.handle, _RECORDING, False) is False
