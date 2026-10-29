import cqrs
from cqrs.bootstrap import requests as requests_bootstrap
from cqrs.events.event_emitter import EventEmitter
from cqrs.mapping.events import EventMap
from cqrs.message_brokers.devnull import MESSAGE_BUS, DevnullMessageBroker
from cqrs.testing import CapturingMessageBroker, TestContainer, test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreated(cqrs.DomainEvent):
    order_id: str


class OrderCreatedNotice(cqrs.NotificationEvent):
    event_name: str = "order_created"
    topic: str = "order_events"
    payload: str = ""


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._events.append(OrderCreatedNotice())
        return CreatedOrder(order_id="1")


class RaisingHandler(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        raise RuntimeError("boom")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


def test_harness_is_not_on_the_package_root() -> None:
    assert "test_harness" not in cqrs.__all__


async def test_listener_default_does_not_change_emit() -> None:
    empty = EventEmitter(EventMap(), TestContainer(), DevnullMessageBroker())
    assert empty._listener is None
    assert await empty.emit(OrderCreated(order_id="1")) == ()

    event_map = EventMap()
    event_map.bind(OrderCreated, RaisingHandler)
    failing = EventEmitter(event_map, TestContainer(), DevnullMessageBroker())
    assert failing._listener is None
    assert await failing.emit(OrderCreated(order_id="1")) == []


async def test_around_restores_broker_and_leaves_message_bus_unchanged() -> None:
    container = TestContainer()
    mediator = requests_bootstrap.bootstrap(container, commands_mapper=commands_mapper)
    emitter = mediator._event_processor._event_emitter
    assert emitter is not None
    before = len(MESSAGE_BUS)
    assert isinstance(emitter._message_broker, DevnullMessageBroker)
    assert emitter._listener is None
    async with test_harness.around(mediator) as harness:
        await mediator.send(CreateOrder(sku="book"))
        harness.published.assert_one(OrderCreatedNotice, topic="order_events")
        assert len(MESSAGE_BUS) == before
        assert isinstance(emitter._message_broker, CapturingMessageBroker)
        assert emitter._listener is not None
    assert isinstance(emitter._message_broker, DevnullMessageBroker)
    assert emitter._listener is None
    assert len(MESSAGE_BUS) == before


async def test_capturing_broker_replaces_a_transport_adapter() -> None:
    container = TestContainer()
    broker = CapturingMessageBroker()
    mediator = requests_bootstrap.bootstrap(
        container,
        message_broker=broker,
        commands_mapper=commands_mapper,
    )
    async with test_harness.around(mediator, broker=broker) as harness:
        await mediator.send(CreateOrder(sku="book"))
        harness.published.assert_one(OrderCreatedNotice, topic="order_events")
        assert harness.broker.messages
