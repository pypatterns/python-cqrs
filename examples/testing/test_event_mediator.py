"""
EventMediator sends an event that arrived from outside this process.

The event mapper argument is events_mapper, the same name EventMediator bootstrap uses.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_event_mediator.py
"""

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


async def test_event_mediator_enters_handle() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, events_mapper=events_mapper) as harness:
        # Act
        await harness.event_mediator.send(OrderCreated(order_id="1"))

        # Assert
        harness.invocations.assert_entered(AuditHandler)
        assert harness.invocations.of(AuditHandler)[0].method == "handle"
