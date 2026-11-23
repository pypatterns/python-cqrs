"""Command, saga, notification, subscriber, and the outbox/inbox path."""

from __future__ import annotations

import dataclasses

import di
import di.dependent

import cqrs
from cqrs.inbox.memory import MemoryInboxRepository
from cqrs.message_brokers.producer import EventProducer
from cqrs.saga.models import SagaContext
from cqrs.testing import test_harness
from cqrs.transport import InMemoryTransport, IncomingMessage, SubscribeOptions


class PlaceOrderResult(cqrs.Response):
    order_id: str


class PlaceOrder(cqrs.Request[PlaceOrderResult]):
    order_id: str


class OrderPlaced(cqrs.NotificationEvent):
    event_name: str = "order_placed"
    topic: str = "events"
    payload: str


class PlaceOrderHandler(cqrs.RequestHandler[PlaceOrder]):
    async def handle(self, request: PlaceOrder) -> PlaceOrderResult:
        return PlaceOrderResult(order_id=request.order_id)


class OutboxPlaceOrderHandler(cqrs.RequestHandler[PlaceOrder]):
    def __init__(self, repository: cqrs.OutboxedEventRepository) -> None:
        self._repository = repository

    async def handle(self, request: PlaceOrder) -> PlaceOrderResult:
        self._repository.add(OrderPlaced(payload=request.order_id))
        return PlaceOrderResult(order_id=request.order_id)


@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str


class NotifyWarehouse(cqrs.SagaStepHandler[OrderContext, None]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def act(self, context: OrderContext) -> cqrs.SagaStepResult[OrderContext, None]:
        self._events.append(OrderPlaced(payload=context.order_id))
        return self._generate_step_result(None)

    async def compensate(self, context: OrderContext) -> None:
        return None


class OrderSaga(cqrs.Saga[OrderContext]):
    steps = [NotifyWarehouse]


_handled: list[str] = []


class RecordOrder(cqrs.EventHandler[OrderPlaced]):
    async def handle(self, event: OrderPlaced) -> None:
        _handled.append(event.payload)


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(PlaceOrder, PlaceOrderHandler)


def outbox_commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(PlaceOrder, OutboxPlaceOrderHandler)


def sagas_mapper(mapper: cqrs.SagaMap) -> None:
    mapper.bind(OrderContext, OrderSaga)


def events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderPlaced, RecordOrder)


def _bind_outbox(container: di.Container, repository: cqrs.OutboxedEventRepository) -> None:
    def outbox_repository() -> cqrs.OutboxedEventRepository:
        return repository

    container.bind(
        di.bind_by_type(
            di.dependent.Dependent(outbox_repository, scope="request"),
            cqrs.OutboxedEventRepository,
        ),
    )


async def test_command_saga_publishes_to_the_subscriber() -> None:
    transport = InMemoryTransport()
    bodies: list[bytes] = []

    async def on_event(message: IncomingMessage) -> None:
        bodies.append(message.body)
        await message.ack()

    try:
        async with test_harness(
            commands_mapper=commands_mapper,
            sagas_mapper=sagas_mapper,
            message_broker=transport,
        ) as harness:
            await transport.subscribe("events", on_event, SubscribeOptions(group="warehouse"))
            placed = await harness.mediator.send(PlaceOrder(order_id="o-1"))
            async for _step in harness.saga_mediator.execute(OrderContext(order_id=placed.order_id)):
                pass
            await transport.drain()

            harness.published.assert_contains(OrderPlaced, topic="events")
            assert len(bodies) == 1
    finally:
        await transport.close()


async def test_outbox_reaches_inbox_through_the_transport() -> None:
    transport = InMemoryTransport()
    container = di.Container()
    message_map = cqrs.InboxMessageMap()
    message_map.register("order_placed", OrderPlaced)
    inbox = MemoryInboxRepository(message_map)
    _handled.clear()

    async def on_event(message: IncomingMessage) -> None:
        notice = message_map.deserialize("order_placed", message.body)
        await inbox.add([(str(notice.event_id), notice)])
        await message.ack()

    try:
        async with test_harness(
            container,
            commands_mapper=outbox_commands_mapper,
            events_mapper=events_mapper,
            message_broker=transport,
        ) as harness:
            _bind_outbox(container, harness.outbox_repository)
            await transport.subscribe("events", on_event, SubscribeOptions(group="billing"))
            await harness.mediator.send(PlaceOrder(order_id="o-1"))
            harness.outbox.assert_contains(OrderPlaced)
            harness.published.assert_empty()

            producer = EventProducer(transport, harness.outbox_repository)
            for row in await harness.outbox_repository.get_many():
                await producer.send_message(row)
            await harness.outbox_repository.commit()
            await transport.drain()

            processor = cqrs.InboxProcessor(
                inbox,
                message_map,
                event_mediator=harness.event_mediator,
            )
            rows = await inbox.get_many()
            assert len(rows) == 1
            for row in rows:
                await processor.process(row)
            assert await inbox.get_many() == []
            assert _handled == ["o-1"]
    finally:
        await transport.close()
