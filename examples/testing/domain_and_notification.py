"""
Domain event, notification, JSON payload, swallowed handler error, and unhandled event.

Run:
   python examples/testing/domain_and_notification.py
"""

import asyncio

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


class AuditHandler(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        return None


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


async def domain_event() -> None:
    class AuditHandler(cqrs.EventHandler[OrderCreated]):
        @property
        def events(self) -> list[cqrs.Event]:
            return [InvoiceRequested(order_id="1")]

        async def handle(self, event: OrderCreated) -> None:
            return None

    class InvoiceHandler(cqrs.EventHandler[InvoiceRequested]):
        async def handle(self, event: InvoiceRequested) -> None:
            return None

    def domain_events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(OrderCreated, AuditHandler)
        mapper.bind(InvoiceRequested, InvoiceHandler)

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


async def notification_json() -> None:
    class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
        def __init__(self) -> None:
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> list[cqrs.Event]:
            return self._events

        async def handle(self, request: CreateOrder):
            self._events.append(OrderCreatedNotice(payload={"order_id": "1"}))
            return CreatedOrder(order_id="1")

    def commands_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(CreateOrder, CreateOrderHandler)

    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))
        harness.published.assert_one(OrderCreatedNotice, topic="order_events")
        assert not harness.handled.contains(OrderCreatedNotice)
        message = harness.published.one(OrderCreatedNotice)
        assert message.content_type is None
        assert message.payload["event_name"] == "order_created"


async def swallowed_handler_error() -> None:
    class AuditHandler(cqrs.EventHandler[OrderCreated]):
        async def handle(self, event: OrderCreated) -> None:
            raise RuntimeError("audit failed")

    def domain_events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(OrderCreated, AuditHandler)

    container = di.Container()
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
    ) as harness:
        result = await harness.mediator.send(CreateOrder(sku="book"))
        assert result.order_id == "1"
        assert harness.handler_errors.of(OrderCreated, AuditHandler)


async def unhandled_event() -> None:
    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))
        assert harness.unhandled.contains(OrderCreated)
        harness.invocations.assert_not_entered(AuditHandler)


async def main() -> None:
    await domain_event()
    await notification_json()
    await swallowed_handler_error()
    await unhandled_event()


if __name__ == "__main__":
    asyncio.run(main())
