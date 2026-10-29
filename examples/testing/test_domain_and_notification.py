"""
Domain event, notification, JSON payload, swallowed handler error, and unhandled event.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_domain_and_notification.py
"""

import di

import cqrs
from cqrs.testing import test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreated(cqrs.DomainEvent):
    order_id: str


class InvoiceRequested(cqrs.DomainEvent):
    order_id: str


class OrderCreatedNotice(cqrs.NotificationEvent):
    event_name: str = "order_created"
    topic: str = "order_events"


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder):
        self._events.append(OrderCreated(order_id="1"))
        return CreatedOrder(order_id="1")


class NotifyingCreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder):
        self._events.append(OrderCreatedNotice(payload={"order_id": "1"}))
        return CreatedOrder(order_id="1")


class AuditHandler(cqrs.EventHandler[OrderCreated]):
    @property
    def events(self) -> list[cqrs.Event]:
        return [InvoiceRequested(order_id="1")]

    async def handle(self, event: OrderCreated) -> None:
        return None


class InvoiceHandler(cqrs.EventHandler[InvoiceRequested]):
    async def handle(self, event: InvoiceRequested) -> None:
        return None


class FailingAuditHandler(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        raise RuntimeError("audit failed")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


def notifying_commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, NotifyingCreateOrderHandler)


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderCreated, AuditHandler)
    mapper.bind(InvoiceRequested, InvoiceHandler)


def failing_domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderCreated, FailingAuditHandler)


async def test_domain_event_and_follow_up_are_handled() -> None:
    container = di.Container()
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
    ) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        assert harness.handled.contains(OrderCreated)
        assert harness.handled.contains(InvoiceRequested)
        harness.invocations.assert_entered(AuditHandler)
        harness.invocations.assert_entered(InvoiceHandler)
        harness.published.assert_empty()


async def test_notification_is_published_as_json() -> None:
    container = di.Container()
    async with test_harness(container, commands_mapper=notifying_commands_mapper) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        harness.published.assert_one(OrderCreatedNotice, topic="order_events")
        assert not harness.handled.contains(OrderCreatedNotice)
        message = harness.published.one(OrderCreatedNotice)
        assert message.content_type is None
        assert message.payload["event_name"] == "order_created"


async def test_domain_handler_error_is_swallowed() -> None:
    container = di.Container()
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        domain_events_mapper=failing_domain_events_mapper,
    ) as harness:
        result = await harness.mediator.send(CreateOrder(sku="book"))

        assert result.order_id == "1"
        assert harness.handler_errors.of(OrderCreated, FailingAuditHandler)


async def test_event_without_handler_is_unhandled() -> None:
    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        assert harness.unhandled.contains(OrderCreated)
        harness.invocations.assert_not_entered(AuditHandler)
