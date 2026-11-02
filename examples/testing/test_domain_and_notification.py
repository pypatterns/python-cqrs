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
    # Arrange
    container = di.Container()
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
    ) as harness:
        # Act
        await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        harness.handled.assert_contains(OrderCreated)
        harness.handled.assert_contains(
            OrderCreated,
            where=lambda event: event.order_id == "1",
        )
        harness.handled.assert_contains(InvoiceRequested)
        harness.invocations.assert_entered(AuditHandler)
        harness.invocations.assert_entered(InvoiceHandler)
        harness.published.assert_empty()
        harness.published.assert_not_contains(OrderCreated)


async def test_notification_is_published_as_json() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, commands_mapper=notifying_commands_mapper) as harness:
        # Act
        await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        harness.published.assert_one(OrderCreatedNotice, topic="order_events")
        message = harness.published.one(OrderCreatedNotice)
        assert message.payload["payload"]["order_id"] == "1"
        assert message.content_type is None
        assert message.payload["event_name"] == "order_created"
        harness.handled.assert_not_contains(OrderCreatedNotice)


async def test_domain_handler_error_is_swallowed() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        domain_events_mapper=failing_domain_events_mapper,
    ) as harness:
        # Act
        result = await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        assert result.order_id == "1"
        harness.handler_errors.assert_contains(OrderCreated, FailingAuditHandler)
        harness.requests.assert_contains(CreateOrder)


async def test_event_without_handler_is_unhandled() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        # Act
        await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        harness.unhandled.assert_contains(OrderCreated)
        harness.invocations.assert_not_entered(AuditHandler)
