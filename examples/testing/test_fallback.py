"""
Request fallback, event fallback, and an open circuit breaker.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_fallback.py
"""

import di

import cqrs
from cqrs.testing import test_harness


class CircuitOpen(Exception):
    pass


class OpenCircuitBreaker:
    async def call(self, identifier, func, *args, **kwargs):
        raise CircuitOpen()

    def is_circuit_breaker_error(self, exc: Exception) -> bool:
        return isinstance(exc, CircuitOpen)


class CreatedOrder(cqrs.Response):
    order_id: str
    source: str = "handler"


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreated(cqrs.DomainEvent):
    order_id: str


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder):
        raise RuntimeError("primary failed")


class CreateOrderFallback(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder):
        return CreatedOrder(order_id="1", source="fallback")


class EmittingCreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
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
        raise RuntimeError("audit failed")


class AuditFallback(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        return None


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(
        CreateOrder,
        cqrs.RequestHandlerFallback(CreateOrderHandler, CreateOrderFallback),
    )


def emitting_commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, EmittingCreateOrderHandler)


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(
        OrderCreated,
        cqrs.EventHandlerFallback(AuditHandler, AuditFallback),
    )


def breaker_commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(
        CreateOrder,
        cqrs.RequestHandlerFallback(
            CreateOrderHandler,
            CreateOrderFallback,
            circuit_breaker=OpenCircuitBreaker(),
        ),
    )


async def test_request_fallback_answers_after_the_primary_fails() -> None:
    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        result = await harness.mediator.send(CreateOrder(sku="book"))

        assert result.source == "fallback"
        harness.invocations.assert_entered(CreateOrderHandler)
        harness.invocations.assert_entered(CreateOrderFallback)


async def test_event_fallback_runs_after_the_primary_fails() -> None:
    container = di.Container()
    async with test_harness(
        container,
        commands_mapper=emitting_commands_mapper,
        domain_events_mapper=domain_events_mapper,
    ) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        harness.invocations.assert_entered(AuditHandler)
        harness.invocations.assert_entered(AuditFallback)


async def test_open_circuit_breaker_skips_the_primary() -> None:
    container = di.Container()
    async with test_harness(container, commands_mapper=breaker_commands_mapper) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        harness.invocations.assert_not_entered(CreateOrderHandler)
        harness.invocations.assert_entered(CreateOrderFallback)
