"""Domain event on events, outbox notification, inbox accept.
Full demos: examples/events/domain_event_handler.py, examples/outbox/fastapi_outbox.py.
"""

import dataclasses
import typing

import cqrs


@dataclasses.dataclass(frozen=True)
class OrderCreatedPayload:
    order_id: str
    amount: float


class OrderPlaced(cqrs.DomainEvent, frozen=True):
    order_id: str


class OrderCreated(cqrs.NotificationEvent[OrderCreatedPayload]):
    event_name: str = "order_created"
    topic: str = "order_events"
    payload: OrderCreatedPayload


class PlaceOrder(cqrs.Request[None]):
    order_id: str
    amount: float


class OrderRow:
    def __init__(self, order_id: str, amount: float) -> None:
        self.order_id = order_id
        self.amount = amount


class RowSession(typing.Protocol):
    def add(self, row: OrderRow) -> None: ...


class PlaceOrderHandler(cqrs.RequestHandler[PlaceOrder]):
    def __init__(self, session: RowSession, outbox: cqrs.OutboxedEventRepository) -> None:
        self._session = session
        self._outbox = outbox
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: PlaceOrder):
        self._session.add(OrderRow(request.order_id, request.amount))
        self._outbox.add(
            OrderCreated(
                event_name="order_created",
                topic="order_events",
                payload=OrderCreatedPayload(order_id=request.order_id, amount=request.amount),
            ),
        )
        self._events.append(OrderPlaced(order_id=request.order_id))
        # The caller commits. An inbox worker commits after process, together
        # with the inbox status. Do not commit inside handle on that path.


class OrderPlacedHandler(cqrs.EventHandler[OrderPlaced]):
    async def handle(self, event: OrderPlaced) -> None:
        """In-process reaction. The outbox notification is a separate fact."""


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderPlaced, OrderPlacedHandler)


def register_delivery(outbox_map: cqrs.OutboxedEventMap, inbox_map: cqrs.InboxMessageMap) -> None:
    outbox_map.register("order_created", OrderCreated)
    inbox_map.register("PlaceOrder", PlaceOrder)


async def accept_place_order(inbox: cqrs.InboxRepository, message_id: str, command: PlaceOrder) -> list[bool]:
    return await inbox.add([(message_id, command)])
