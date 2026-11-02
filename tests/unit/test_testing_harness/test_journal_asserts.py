import dataclasses
import re
import typing

import pytest

import cqrs
from cqrs.outbox.memory import MemoryOutboxRepository
from cqrs.saga.models import SagaContext
from cqrs.saga.storage.enums import SagaStatus
from cqrs.testing import TestContainer, test_harness
from cqrs.testing.report import _load_rich


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
        return CreatedOrder(order_id="1")


class NoticeHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._events.append(OrderCreatedNotice(payload={"order_id": "1"}))
        return CreatedOrder(order_id="1")


class OutboxHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self, repository: MemoryOutboxRepository) -> None:
        self._repository = repository

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._repository.add(OrderCreatedNotice(payload={"order_id": "1"}))
        return CreatedOrder(order_id="1")


class AuditHandler(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        return None


class RaisingAudit(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        raise RuntimeError("audit failed")


class MarkupAudit(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        raise RuntimeError("cannot open [/tmp/x]")


class DenyMiddleware:
    async def __call__(self, request: CreateOrder, handle) -> CreatedOrder:
        return CreatedOrder(order_id="denied")


@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str


class ReserveStock(cqrs.SagaStepHandler[OrderContext, None]):
    async def act(self, context: OrderContext):
        return self._generate_step_result(None)

    async def compensate(self, context: OrderContext) -> None:
        return None


class OrderSaga(cqrs.Saga[OrderContext]):
    steps = [ReserveStock]


def _commands(handler: type):
    def commands_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(CreateOrder, handler)

    return commands_mapper


def _domain_events(handler: type):
    def domain_events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(OrderCreated, handler)

    return domain_events_mapper


def _plain_report(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("cqrs.testing.report._load_rich", lambda: None)


def _hide_rich(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins
    import sys

    real_import = builtins.__import__

    def hide_rich(name: str, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        if name == "rich" or name.startswith("rich."):
            raise ImportError("No module named 'rich'")
        return real_import(name, *args, **kwargs)

    for key in [key for key in sys.modules if key == "rich" or key.startswith("rich.")]:
        monkeypatch.delitem(sys.modules, key, raising=False)
    monkeypatch.setattr(builtins, "__import__", hide_rich)


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


async def test_requests_assert_contains_one_and_empty() -> None:
    # Arrange
    async with test_harness(TestContainer(), commands_mapper=_commands(CreateOrderHandler)) as harness:
        # Act
        result = await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        assert result.order_id == "1"
        assert harness.requests.contains(CreateOrder) is True
        harness.requests.assert_contains(CreateOrder)
        harness.requests.assert_one(CreateOrder)
        call = harness.requests.one(CreateOrder)
        assert call.response == CreatedOrder(order_id="1")
        harness.published.assert_empty()
        harness.outbox.assert_empty()
        harness.handler_errors.assert_empty()
        harness.sagas.assert_empty()


async def test_invocations_assert_entered_still_works() -> None:
    # Arrange
    async with test_harness(TestContainer(), commands_mapper=_commands(CreateOrderHandler)) as harness:
        # Act
        await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        harness.invocations.assert_entered(CreateOrderHandler)
        harness.invocations.assert_contains(CreateOrderHandler, method="handle")
        harness.invocations.assert_not_entered(AuditHandler)
        harness.invocations.assert_not_contains(AuditHandler)


async def test_handled_where_filters_extra_matches() -> None:
    # Arrange
    async with test_harness(
        TestContainer(),
        commands_mapper=_commands(CreateOrderHandler),
        domain_events_mapper=_domain_events(AuditHandler),
    ) as harness:
        # Act
        await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        harness.handled.assert_contains(OrderCreated)
        harness.handled.assert_contains(OrderCreated, where=lambda event: event.order_id == "1")
        harness.handled.assert_one(OrderCreated)
        harness.handled.assert_not_contains(OrderCreatedNotice)
        with pytest.raises(AssertionError):
            harness.handled.assert_contains(OrderCreated, where=lambda event: event.order_id == "missing")


async def test_unhandled_assert_contains() -> None:
    # Arrange
    async with test_harness(TestContainer(), commands_mapper=_commands(CreateOrderHandler)) as harness:
        # Act
        await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        harness.unhandled.assert_contains(OrderCreated)
        harness.unhandled.assert_one(OrderCreated)
        harness.handled.assert_empty()


async def test_published_assert_one_topic_and_where() -> None:
    # Arrange
    async with test_harness(TestContainer(), commands_mapper=_commands(NoticeHandler)) as harness:
        # Act
        await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        harness.published.assert_contains(OrderCreatedNotice, topic="order_events")
        harness.published.assert_one(OrderCreatedNotice, topic="order_events")
        message = harness.published.one(
            OrderCreatedNotice,
            where=lambda item: item.payload["payload"]["order_id"] == "1",
        )
        assert message.topic == "order_events"
        harness.published.assert_not_contains(OrderCreated)
        harness.handled.assert_not_contains(OrderCreatedNotice)


async def test_outbox_records_event_type_until_produce() -> None:
    # Arrange
    container = TestContainer()
    async with test_harness(container, commands_mapper=_commands(OutboxHandler)) as harness:
        container.register(OutboxHandler, OutboxHandler(harness.outbox_repository))

        # Act
        await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        harness.outbox.assert_contains(OrderCreatedNotice)
        harness.outbox.assert_one(OrderCreatedNotice)
        assert harness.outbox.contains(OrderCreatedNotice) is True
        harness.published.assert_empty()


async def test_outbox_produce_publishes_the_recorded_event() -> None:
    # Arrange
    container = TestContainer()
    async with test_harness(container, commands_mapper=_commands(OutboxHandler)) as harness:
        container.register(OutboxHandler, OutboxHandler(harness.outbox_repository))
        await harness.mediator.send(CreateOrder(sku="book"))

        # Act
        await harness.outbox.produce_available()

        # Assert
        harness.published.assert_one(OrderCreatedNotice, topic="order_events")
        harness.outbox.assert_contains(OrderCreatedNotice)


async def test_requests_reject_unsupported_topic_filter() -> None:
    # Arrange
    async with test_harness(TestContainer()) as harness:
        # Act
        with pytest.raises(TypeError, match="requests journal does not support filter") as caught:
            harness.requests.assert_contains(CreateOrder, topic="x")

        # Assert
        assert "topic" in str(caught.value)


async def test_published_rejects_unsupported_method_filter() -> None:
    # Arrange
    async with test_harness(TestContainer()) as harness:
        # Act
        with pytest.raises(TypeError, match="published journal does not support filter") as caught:
            harness.published.assert_contains(OrderCreatedNotice, method="handle")

        # Assert
        assert "method" in str(caught.value)


async def test_inbox_assert_contains_by_type() -> None:
    # Arrange
    async with test_harness(TestContainer(), commands_mapper=_commands(CreateOrderHandler)) as harness:
        await harness.inbox.add([("msg-1", CreateOrder(sku="book"))])

        # Act
        await harness.inbox.process_available()

        # Assert
        harness.inbox.assert_contains(CreateOrder)
        assert harness.inbox.one("msg-1").status is cqrs.InboxStatus.PROCESSED
        harness.inbox.assert_not_contains(OrderCreated)
        harness.requests.assert_contains(CreateOrder)


async def test_middleware_assert_contains_called_next() -> None:
    # Arrange
    async with test_harness(
        TestContainer(),
        commands_mapper=_commands(CreateOrderHandler),
        middlewares=[DenyMiddleware()],
    ) as harness:
        # Act
        result = await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        assert result.order_id == "denied"
        harness.middlewares.assert_contains(DenyMiddleware, called_next=False)
        harness.middlewares.assert_one(DenyMiddleware)
        harness.invocations.assert_not_entered(CreateOrderHandler)


async def test_handler_errors_assert_contains() -> None:
    # Arrange
    async with test_harness(
        TestContainer(),
        commands_mapper=_commands(CreateOrderHandler),
        domain_events_mapper=_domain_events(RaisingAudit),
    ) as harness:
        # Act
        result = await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        assert result.order_id == "1"
        assert harness.handler_errors.of(OrderCreated, RaisingAudit)
        harness.handler_errors.assert_contains(OrderCreated, RaisingAudit)
        harness.handler_errors.assert_one(OrderCreated, RaisingAudit)
        harness.requests.assert_contains(CreateOrder)


async def test_sagas_assert_one() -> None:
    # Arrange
    def sagas_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    async with test_harness(TestContainer(), sagas_mapper=sagas_mapper) as harness:
        # Act
        async for _ in harness.saga_mediator.execute(OrderContext(order_id="1")):
            pass

        # Assert
        harness.sagas.assert_contains(OrderSaga)
        harness.sagas.assert_one(OrderSaga)
        saga = harness.sagas.one(OrderSaga)
        assert saga.status is SagaStatus.COMPLETED
        harness.invocations.assert_contains(ReserveStock, method="act")


async def test_assert_failure_report_lists_types_journals_and_hints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    _plain_report(monkeypatch)
    async with test_harness(
        TestContainer(),
        commands_mapper=_commands(CreateOrderHandler),
        domain_events_mapper=_domain_events(AuditHandler),
    ) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        # Act
        with pytest.raises(AssertionError) as caught:
            harness.published.assert_contains(OrderCreated)

        # Assert
        text = str(caught.value)
        assert "\x1b[" not in text
        assert "published.assert_contains(OrderCreated) failed" in text
        assert "looked for  OrderCreated" in text
        assert "Hints" in text
        assert "OrderCreated is in handled, not on the broker" in text
        assert "handled" in text
        assert "invocations" in text
        assert "requests" in text
        assert "unhandled" in text
        assert "outbox" in text
        assert "inbox" in text
        assert "sagas" in text
        assert "middlewares" in text
        assert "handler_errors" in text
        assert "yields" in text
        assert "CreateOrderHandler" in text
        assert "AuditHandler" in text
        assert "outbox is empty — this was not waiting for produce_available" in text


async def test_plain_report_when_rich_is_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    _hide_rich(monkeypatch)
    async with test_harness(TestContainer(), commands_mapper=_commands(CreateOrderHandler)) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        # Act
        with pytest.raises(AssertionError, match="looked for") as caught:
            harness.unhandled.assert_empty()

        # Assert
        text = str(caught.value)
        assert "unhandled.assert_empty() failed" in text
        assert "OrderCreated" in text
        assert "requests" in text
        assert "\x1b[" not in text
        assert "ImportError" not in type(caught.value).__name__
        assert "No module named 'rich'" not in text


async def test_plain_report_when_rich_render_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    def boom(*_args: typing.Any, **_kwargs: typing.Any) -> str:
        raise RuntimeError("rich exploded")

    monkeypatch.setattr("cqrs.testing.report.HarnessReport._render_rich", boom)
    async with test_harness(TestContainer(), commands_mapper=_commands(CreateOrderHandler)) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        # Act
        with pytest.raises(AssertionError, match="looked for") as caught:
            harness.unhandled.assert_empty()

        # Assert
        text = str(caught.value)
        assert "unhandled.assert_empty() failed" in text
        assert "OrderCreated" in text
        assert "requests" in text
        assert "rich exploded" not in text


@pytest.mark.skipif(_load_rich() is None, reason="rich is not installed")
async def test_rich_report_keeps_english_words() -> None:
    # Arrange
    async with test_harness(
        TestContainer(),
        commands_mapper=_commands(CreateOrderHandler),
        domain_events_mapper=_domain_events(AuditHandler),
    ) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        # Act
        with pytest.raises(AssertionError) as caught:
            harness.published.assert_contains(OrderCreated)

        # Assert
        text = _strip_ansi(str(caught.value))
        assert "published.assert_contains(OrderCreated) failed" in text
        assert "looked for" in text
        assert "handled" in text
        assert "OrderCreated" in text
        assert "Hints" in text


@pytest.mark.skipif(_load_rich() is None, reason="rich is not installed")
async def test_rich_report_escapes_markup_in_error_text() -> None:
    # Arrange
    async with test_harness(
        TestContainer(),
        commands_mapper=_commands(CreateOrderHandler),
        domain_events_mapper=_domain_events(MarkupAudit),
    ) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        # Act
        with pytest.raises(AssertionError) as caught:
            harness.handler_errors.assert_empty()

        # Assert
        text = _strip_ansi(str(caught.value))
        assert "cannot open [/tmp/x]" in text
        assert "MarkupError" not in type(caught.value).__name__
        assert "MarkupError" not in text
