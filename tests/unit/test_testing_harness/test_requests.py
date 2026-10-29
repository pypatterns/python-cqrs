import cqrs
from cqrs.testing import TestContainer, test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreated(cqrs.DomainEvent):
    order_id: str


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._events.append(OrderCreated(order_id="1"))
        return CreatedOrder(order_id="1")


class Acknowledge(cqrs.Request[None]):
    sku: str


class AcknowledgeHandler(cqrs.RequestHandler[Acknowledge]):
    async def handle(self, request: Acknowledge) -> None:
        return None


class OrderView(cqrs.Response):
    order_id: str


class ReadOrder(cqrs.Request[OrderView]):
    order_id: str


class ReadOrderHandler(cqrs.RequestHandler[ReadOrder]):
    async def handle(self, request: ReadOrder) -> OrderView:
        return OrderView(order_id=request.order_id)


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)
    mapper.bind(Acknowledge, AcknowledgeHandler)


def queries_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ReadOrder, ReadOrderHandler)


async def test_command_stores_the_response() -> None:
    async with test_harness(TestContainer(), commands_mapper=commands_mapper) as harness:
        assert harness.mediator is not None
        result = await harness.mediator.send(CreateOrder(sku="book"))
        assert result.order_id == "1"
        assert harness.requests.contains(CreateOrder)
        assert result is harness.requests.of(CreateOrder)[0].response
        harness.invocations.assert_entered(CreateOrderHandler)


async def test_command_without_result() -> None:
    async with test_harness(TestContainer(), commands_mapper=commands_mapper) as harness:
        assert harness.mediator is not None
        result = await harness.mediator.send(Acknowledge(sku="book"))
        assert result is None
        assert harness.requests.of(Acknowledge)[0].response is None
        harness.invocations.assert_entered(AcknowledgeHandler)


async def test_query_leaves_delivery_channels_empty() -> None:
    async with test_harness(TestContainer(), queries_mapper=queries_mapper) as harness:
        assert harness.mediator is not None
        view = await harness.mediator.send(ReadOrder(order_id="1"))
        assert view.order_id == "1"
        harness.handled.assert_empty()
        harness.published.assert_empty()
        harness.outbox.assert_empty()


async def test_dataclass_response_round_trip() -> None:
    async with test_harness(TestContainer(), commands_mapper=commands_mapper) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        assert harness.requests.of(CreateOrder)[0].response == CreatedOrder(order_id="1")
