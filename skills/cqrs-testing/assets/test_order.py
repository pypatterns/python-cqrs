"""Command, domain event, and notification recorded by test_harness.

Full demos: examples/testing/test_command_and_query.py, examples/testing/test_domain_and_notification.py.
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


class OrderCreatedNotice(cqrs.NotificationEvent):
    event_name: str = "order_created"
    topic: str = "order_events"


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._events.append(OrderCreated(order_id="1"))
        self._events.append(OrderCreatedNotice(payload={"order_id": "1"}))
        return CreatedOrder(order_id="1")


class AuditOrderCreated(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        return None


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderCreated, AuditOrderCreated)


async def test_place_order() -> None:
    container = di.Container()
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
    ) as harness:
        result = await harness.mediator.send(CreateOrder(sku="book"))

        assert result.order_id == "1"
        assert harness.requests.contains(CreateOrder)
        harness.invocations.assert_entered(CreateOrderHandler)
        harness.invocations.assert_entered(AuditOrderCreated)
        assert harness.handled.contains(OrderCreated)
        harness.published.assert_one(OrderCreatedNotice, topic="order_events")
        assert not harness.handled.contains(OrderCreatedNotice)


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_place_order())
