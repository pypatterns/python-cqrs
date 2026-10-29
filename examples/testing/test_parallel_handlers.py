"""
Parallel domain handlers, ScopeStrategy.SEND order, and ScopeStrategy.NONE set.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_parallel_handlers.py
"""

import di

import cqrs
from cqrs.testing import test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreated(cqrs.DomainEvent):
    order_id: str


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder):
        self._events.append(OrderCreated(order_id="1"))
        return CreatedOrder(order_id="1")


class AuditHandler(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        return None


class SearchHandler(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        return None


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderCreated, AuditHandler)
    mapper.bind(OrderCreated, SearchHandler)


async def test_parallel_handlers_both_enter() -> None:
    container = di.Container()
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        concurrent_event_handle_enable=True,
        scope_strategy=cqrs.ScopeStrategy.HANDLER,
    ) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        harness.invocations.assert_entered(AuditHandler)
        harness.invocations.assert_entered(SearchHandler)


async def test_scope_send_runs_handlers_in_order() -> None:
    container = di.Container()
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        scope_strategy=cqrs.ScopeStrategy.SEND,
    ) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        entered = [item.handler for item in harness.invocations if item.argument_type is OrderCreated]
        assert entered == [AuditHandler, SearchHandler]


async def test_scope_none_enters_both_handlers_in_any_order() -> None:
    container = di.Container()
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        concurrent_event_handle_enable=True,
        scope_strategy=cqrs.ScopeStrategy.NONE,
    ) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        handlers = {item.handler for item in harness.invocations if item.argument_type is OrderCreated}
        assert handlers == {AuditHandler, SearchHandler}
