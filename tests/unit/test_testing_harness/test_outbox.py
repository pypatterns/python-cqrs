import cqrs
from cqrs.outbox.memory import MemoryOutboxRepository
from cqrs.outbox.repository import EventStatus
from cqrs.testing import TestContainer, test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreatedNotice(cqrs.NotificationEvent):
    event_name: str = "order_created"
    topic: str = "order_events"
    payload: str = ""


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self, repository: MemoryOutboxRepository) -> None:
        self._repository = repository

    @property
    def events(self) -> list[cqrs.Event]:
        return []

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._repository.add(OrderCreatedNotice())
        return CreatedOrder(order_id="1")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


async def test_outbox_is_published_only_after_produce() -> None:
    container = TestContainer()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        assert harness.mediator is not None
        container.register(CreateOrderHandler, CreateOrderHandler(harness.outbox_repository))
        await harness.mediator.send(CreateOrder(sku="book"))
        assert harness.outbox.pending_count == 1
        harness.published.assert_empty()
        await harness.outbox.produce_available()
        assert harness.outbox.produced_count == 1
        harness.published.assert_one(OrderCreatedNotice, topic="order_events")
        produced = harness.outbox_repository._rows[1]
        assert produced.status is EventStatus.PRODUCED


async def test_outbox_broker_error_leaves_not_produced() -> None:
    container = TestContainer()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        assert harness.mediator is not None
        container.register(CreateOrderHandler, CreateOrderHandler(harness.outbox_repository))
        await harness.mediator.send(CreateOrder(sku="book"))
        harness.broker.fail_next(ConnectionError("down"))
        await harness.outbox.produce_available()
        assert harness.outbox.not_produced_count == 1
        assert harness.outbox.pending_count == 1
        harness.published.assert_empty()
        await harness.outbox.produce_available()
        assert harness.outbox.pending_count == 0
        assert harness.outbox.produced_count == 1


async def test_memory_outbox_counters_are_per_instance() -> None:
    first = MemoryOutboxRepository()
    second = MemoryOutboxRepository()
    first.add(OrderCreatedNotice())
    second.add(OrderCreatedNotice())
    first_rows = await first.get_many()
    second_rows = await second.get_many()
    assert first_rows[0].id == 1
    assert second_rows[0].id == 1
    await first.update_status(1, EventStatus.PRODUCED)
    assert await first.get_many() == []
    assert first._rows[1].status is EventStatus.PRODUCED
    assert (await second.get_many())[0].status is EventStatus.NEW


async def test_memory_outbox_rollback_drops_rows_added_since_commit() -> None:
    repository = MemoryOutboxRepository()
    repository.add(OrderCreatedNotice())
    await repository.rollback()
    assert await repository.get_many() == []

    repository.add(OrderCreatedNotice())
    await repository.commit()
    kept = (await repository.get_many())[0]
    repository.add(OrderCreatedNotice())
    await repository.rollback()
    rows = await repository.get_many()
    assert [row.id for row in rows] == [kept.id]
    assert rows[0].status is EventStatus.NEW
