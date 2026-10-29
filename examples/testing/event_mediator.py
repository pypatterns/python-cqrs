"""
EventMediator sends an event that arrived from outside this process.

The event mapper argument is events_mapper, the same name EventMediator bootstrap uses.

Run:
   python examples/testing/event_mediator.py
"""

import asyncio

import di

import cqrs
from cqrs.testing import test_harness


class OrderCreated(cqrs.DomainEvent):
    order_id: str


class AuditHandler(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        return None


def events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderCreated, AuditHandler)


async def main() -> None:
    container = di.Container()
    async with test_harness(container, events_mapper=events_mapper) as harness:
        await harness.event_mediator.send(OrderCreated(order_id="1"))
        harness.invocations.assert_entered(AuditHandler)
        assert harness.invocations.of(AuditHandler)[0].method == "handle"


if __name__ == "__main__":
    asyncio.run(main())
