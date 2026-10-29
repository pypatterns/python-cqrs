"""
Inbox success and failure, outbox produce, and a broker that fails the next send.

The harness builds memory repositories and does not bind them. The outbox
handler binds harness.outbox_repository as cqrs.OutboxedEventRepository.

Run:
   python examples/testing/outbox_inbox.py
"""

import asyncio

import di
import di.dependent

import cqrs
from cqrs.testing import test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreatedNotice(cqrs.NotificationEvent):
    event_name: str = "order_created"
    topic: str = "order_events"


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder):
        return CreatedOrder(order_id="1")


class FailingCreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder):
        raise RuntimeError("inbox handler failed")


class OutboxCreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self, repository: cqrs.OutboxedEventRepository) -> None:
        self._repository = repository

    async def handle(self, request: CreateOrder):
        self._repository.add(OrderCreatedNotice(payload={"order_id": "1"}))
        return CreatedOrder(order_id="1")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


def failing_commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, FailingCreateOrderHandler)


def outbox_commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, OutboxCreateOrderHandler)


def bind_outbox(container: di.Container, repository: cqrs.OutboxedEventRepository) -> None:
    def outbox_repository() -> cqrs.OutboxedEventRepository:
        return repository

    container.bind(
        di.bind_by_type(
            di.dependent.Dependent(outbox_repository, scope="request"),
            cqrs.OutboxedEventRepository,
        ),
    )


async def inbox_success() -> None:
    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        inserted = await harness.inbox.add([("msg-1", CreateOrder(sku="book"))])
        assert inserted == [True]
        duplicate = await harness.inbox.add([("msg-1", CreateOrder(sku="book"))])
        assert duplicate == [False]
        await harness.inbox.process_available()
        assert harness.requests.contains(CreateOrder)
        assert harness.inbox.one("msg-1").status is cqrs.InboxStatus.PROCESSED


async def inbox_failure() -> None:
    container = di.Container()
    async with test_harness(container, commands_mapper=failing_commands_mapper) as harness:
        await harness.inbox.add([("msg-2", CreateOrder(sku="book"))])
        await harness.inbox.process_available()
        row = harness.inbox.one("msg-2")
        assert row.status is cqrs.InboxStatus.NOT_PROCESSED
        assert row.flush_counter == 1


async def outbox_produce() -> None:
    container = di.Container()
    async with test_harness(container, commands_mapper=outbox_commands_mapper) as harness:
        bind_outbox(container, harness.outbox_repository)
        await harness.mediator.send(CreateOrder(sku="book"))
        assert harness.outbox.pending_count == 1
        harness.published.assert_empty()
        await harness.outbox.produce_available()
        assert harness.outbox.produced_count == 1
        harness.published.assert_one(OrderCreatedNotice, topic="order_events")


async def outbox_broker_fail() -> None:
    container = di.Container()
    async with test_harness(container, commands_mapper=outbox_commands_mapper) as harness:
        bind_outbox(container, harness.outbox_repository)
        await harness.mediator.send(CreateOrder(sku="book"))
        harness.broker.fail_next(ConnectionError("down"))
        await harness.outbox.produce_available()
        assert harness.outbox.not_produced_count == 1
        harness.published.assert_empty()


async def main() -> None:
    await inbox_success()
    await inbox_failure()
    await outbox_produce()
    await outbox_broker_fail()


if __name__ == "__main__":
    asyncio.run(main())
