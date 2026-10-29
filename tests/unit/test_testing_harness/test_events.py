import pytest

import cqrs
from cqrs.testing import TestContainer, test_harness


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
    payload: str = ""


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._events.append(OrderCreated(order_id="1"))
        return CreatedOrder(order_id="1")


class NoticeHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._events.append(OrderCreatedNotice())
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


class SearchHandler(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        return None


class RaisingAudit(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        raise RuntimeError("audit failed")


def _commands(handler: type):
    def commands_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(CreateOrder, handler)

    return commands_mapper


async def test_domain_event_follow_up_is_handled_not_published() -> None:
    def domain_events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(OrderCreated, AuditHandler)
        mapper.bind(OrderCreated, SearchHandler)
        mapper.bind(InvoiceRequested, InvoiceHandler)

    async with test_harness(
        TestContainer(),
        commands_mapper=_commands(CreateOrderHandler),
        domain_events_mapper=domain_events_mapper,
    ) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        assert harness.handled.contains(OrderCreated)
        assert harness.handled.contains(InvoiceRequested)
        harness.invocations.assert_entered(AuditHandler)
        harness.invocations.assert_entered(InvoiceHandler)
        harness.invocations.assert_entered(SearchHandler)
        harness.published.assert_empty()
        assert harness.published.contains(OrderCreated) is False
        with pytest.raises(AssertionError, match="handled") as caught:
            harness.published.assert_contains(OrderCreated)
        assert "invocations" in str(caught.value)


async def test_notification_is_published() -> None:
    async with test_harness(
        TestContainer(),
        commands_mapper=_commands(NoticeHandler),
    ) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        harness.published.assert_one(OrderCreatedNotice, topic="order_events")
        assert not harness.handled.contains(OrderCreatedNotice)


async def test_json_published_message_has_no_content_type() -> None:
    async with test_harness(
        TestContainer(),
        commands_mapper=_commands(NoticeHandler),
    ) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        message = harness.published.one(OrderCreatedNotice)
        assert message.content_type is None
        assert message.payload["event_name"] == "order_created"
        assert message.message_name
        assert message.message_id


async def test_parallel_domain_handlers_are_both_entered() -> None:
    def commands_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(CreateOrder, CreateOrderHandler)

    def domain_events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(OrderCreated, AuditHandler)
        mapper.bind(OrderCreated, SearchHandler)

    container = TestContainer()
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        concurrent_event_handle_enable=True,
        scope_strategy=cqrs.ScopeStrategy.HANDLER,
    ) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        harness.invocations.assert_entered(AuditHandler)
        harness.invocations.assert_entered(SearchHandler)


async def test_swallowed_domain_handler_error_keeps_the_response() -> None:
    def domain_events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(OrderCreated, RaisingAudit)

    async with test_harness(
        TestContainer(),
        commands_mapper=_commands(CreateOrderHandler),
        domain_events_mapper=domain_events_mapper,
    ) as harness:
        assert harness.mediator is not None
        result = await harness.mediator.send(CreateOrder(sku="book"))
        assert result.order_id == "1"
        assert harness.handler_errors.of(OrderCreated, RaisingAudit)


async def test_unhandled_domain_event_does_not_fail_send() -> None:
    async with test_harness(
        TestContainer(),
        commands_mapper=_commands(CreateOrderHandler),
    ) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        assert harness.unhandled.contains(OrderCreated)
        harness.invocations.assert_not_entered(AuditHandler)


async def test_event_mediator_records_invocations_and_follow_ups() -> None:
    def events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(OrderCreated, AuditHandler)
        mapper.bind(InvoiceRequested, InvoiceHandler)

    async with test_harness(TestContainer(), events_mapper=events_mapper) as harness:
        assert harness.event_mediator is not None
        await harness.event_mediator.send(OrderCreated(order_id="1"))
        harness.invocations.assert_entered(AuditHandler)
        assert harness.invocations.of(AuditHandler)[0].method == "handle"
        harness.invocations.assert_entered(InvoiceHandler)
        assert not harness.handled.contains(OrderCreated)
        assert not harness.handled.contains(InvoiceRequested)
