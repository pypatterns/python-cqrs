import cqrs
from cqrs.testing import TestContainer, test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreated(cqrs.DomainEvent):
    order_id: str


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder) -> CreatedOrder:
        return CreatedOrder(order_id="1")


class FailingHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder) -> CreatedOrder:
        raise RuntimeError("no")


class AuditHandler(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        return None


async def test_inbox_processes_a_request_and_an_event() -> None:
    def commands_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(CreateOrder, CreateOrderHandler)

    def events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(OrderCreated, AuditHandler)

    commits = 0

    async with test_harness(
        TestContainer(),
        commands_mapper=commands_mapper,
        events_mapper=events_mapper,
    ) as harness:
        assert harness.mediator is not None
        original_commit = harness.inbox_repository.commit

        async def commit() -> None:
            nonlocal commits
            commits += 1
            await original_commit()

        harness.inbox_repository.commit = commit
        inserted = await harness.inbox.add([("msg-1", CreateOrder(sku="book"))])
        assert inserted == [True]
        duplicate = await harness.inbox.add([("msg-1", CreateOrder(sku="book"))])
        assert duplicate == [False]
        event_inserted = await harness.inbox.add([("evt-1", OrderCreated(order_id="1"))])
        assert event_inserted == [True]
        await harness.inbox.process_available()
        assert harness.requests.contains(CreateOrder)
        harness.invocations.assert_entered(AuditHandler)
        assert harness.inbox.one("msg-1").status is cqrs.InboxStatus.PROCESSED
        assert harness.inbox.one("evt-1").status is cqrs.InboxStatus.PROCESSED
        assert commits == 0
        assert harness.inbox.pending_count == 0


async def test_inbox_failure_increments_flush_counter() -> None:
    def commands_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(CreateOrder, FailingHandler)

    async with test_harness(TestContainer(), commands_mapper=commands_mapper) as harness:
        await harness.inbox.add([("msg-2", CreateOrder(sku="book"))])
        await harness.inbox.process_available()
        row = harness.inbox.one("msg-2")
        assert row.status is cqrs.InboxStatus.NOT_PROCESSED
        assert row.flush_counter == 1
        assert harness.inbox.pending_count == 1
