"""
FastStream TestKafkaBroker delivers a message to EventMediator or only into the inbox.

Run:
   python examples/testing/faststream_subscriber.py
"""

import asyncio

import di
from faststream.kafka import KafkaBroker, TestKafkaBroker

import cqrs
from cqrs.bootstrap import events as events_bootstrap
from cqrs.inbox.memory import MemoryInboxRepository
from cqrs.testing import test_harness


class OrderCreated(cqrs.DomainEvent):
    order_id: str


class CreateOrder(cqrs.Request[None]):
    sku: str


class AuditHandler(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        return None


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder):
        return None


def events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderCreated, AuditHandler)


def _inbox() -> MemoryInboxRepository:
    message_map = cqrs.InboxMessageMap()
    message_map.register("CreateOrder", CreateOrder)
    return MemoryInboxRepository(message_map=message_map)


async def event_mediator_path() -> None:
    event_mediator = events_bootstrap.bootstrap(
        di_container=di.Container(),
        events_mapper=events_mapper,
    )
    inbox = _inbox()
    broker = KafkaBroker()

    @broker.subscriber("order_events")
    async def on_order(body: dict) -> None:
        await event_mediator.send(OrderCreated(order_id=body["order_id"]))

    async with TestKafkaBroker(broker) as test_broker:
        async with test_harness.around(event_mediator, inbox=inbox) as harness:
            await test_broker.publish({"order_id": "1"}, "order_events")
            harness.invocations.assert_entered(AuditHandler)


async def inbox_only_path() -> None:
    event_mediator = events_bootstrap.bootstrap(
        di_container=di.Container(),
        events_mapper=events_mapper,
    )
    inbox = _inbox()
    broker = KafkaBroker()

    @broker.subscriber("order_events")
    async def on_order(body: dict) -> None:
        await inbox.add([("msg-1", CreateOrder(sku=body["order_id"]))])

    async with TestKafkaBroker(broker) as test_broker:
        async with test_harness.around(event_mediator, inbox=inbox) as harness:
            await test_broker.publish({"order_id": "1"}, "order_events")
            assert harness.inbox.pending_count == 1
            harness.invocations.assert_not_entered(CreateOrderHandler)


async def main() -> None:
    await event_mediator_path()
    await inbox_only_path()


if __name__ == "__main__":
    asyncio.run(main())
