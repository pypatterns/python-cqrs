"""
Inbox success and failure, outbox produce, and a broker that fails the next send.

The harness builds memory repositories and does not bind them. The outbox
handler binds harness.outbox_repository as cqrs.OutboxedEventRepository.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_outbox_inbox.py
"""

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


async def test_inbox_rejects_a_duplicate_message() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        inserted = await harness.inbox.add([("msg-1", CreateOrder(sku="book"))])

        # Act
        duplicate = await harness.inbox.add([("msg-1", CreateOrder(sku="book"))])

        # Assert
        assert inserted == [True]
        assert duplicate == [False]


async def test_inbox_processes_the_command() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        await harness.inbox.add([("msg-1", CreateOrder(sku="book"))])

        # Act
        await harness.inbox.process_available()

        # Assert
        assert harness.requests.contains(CreateOrder)
        assert harness.inbox.one("msg-1").status is cqrs.InboxStatus.PROCESSED


async def test_inbox_keeps_a_failed_row_not_processed() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, commands_mapper=failing_commands_mapper) as harness:
        await harness.inbox.add([("msg-2", CreateOrder(sku="book"))])

        # Act
        await harness.inbox.process_available()

        # Assert
        row = harness.inbox.one("msg-2")
        assert row.status is cqrs.InboxStatus.NOT_PROCESSED
        assert row.flush_counter == 1


async def test_outbox_holds_the_row_until_produce() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, commands_mapper=outbox_commands_mapper) as harness:
        bind_outbox(container, harness.outbox_repository)

        # Act
        await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        assert harness.outbox.pending_count == 1
        harness.published.assert_empty()


async def test_outbox_produce_publishes_the_row() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, commands_mapper=outbox_commands_mapper) as harness:
        bind_outbox(container, harness.outbox_repository)
        await harness.mediator.send(CreateOrder(sku="book"))

        # Act
        await harness.outbox.produce_available()

        # Assert
        assert harness.outbox.produced_count == 1
        harness.published.assert_one(OrderCreatedNotice, topic="order_events")


async def test_outbox_broker_error_leaves_the_row_not_produced() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, commands_mapper=outbox_commands_mapper) as harness:
        bind_outbox(container, harness.outbox_repository)
        await harness.mediator.send(CreateOrder(sku="book"))
        harness.broker.fail_next(ConnectionError("down"))

        # Act
        await harness.outbox.produce_available()

        # Assert
        assert harness.outbox.not_produced_count == 1
        harness.published.assert_empty()
