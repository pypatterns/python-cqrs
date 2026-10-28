"""Two domain handlers that do not share a session.

Full demo: examples/streaming/streaming_handler_parallel_events.py.
"""

import di

import cqrs
from cqrs.bootstrap import requests as bootstrap


class OrderPlaced(cqrs.DomainEvent, frozen=True):
    order_id: str


class PlaceOrder(cqrs.Request[None]):
    order_id: str


class PlaceOrderHandler(cqrs.RequestHandler[PlaceOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: PlaceOrder):
        self._events.append(OrderPlaced(order_id=request.order_id))


class AuditHandler(cqrs.EventHandler[OrderPlaced]):
    async def handle(self, event: OrderPlaced) -> None:
        return None


class MetricsHandler(cqrs.EventHandler[OrderPlaced]):
    async def handle(self, event: OrderPlaced) -> None:
        return None


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(PlaceOrder, PlaceOrderHandler)


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderPlaced, AuditHandler)
    mapper.bind(OrderPlaced, MetricsHandler)


def build_mediator() -> cqrs.RequestMediator:
    return bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        scope_strategy=cqrs.ScopeStrategy.HANDLER,
        concurrent_event_handle_enable=True,
        max_concurrent_event_handlers=2,
    )
