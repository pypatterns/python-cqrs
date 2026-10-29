import cqrs
from cqrs.testing import TestContainer, test_harness


class CreatedOrder(cqrs.Response):
    order_id: str
    source: str = "primary"


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

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._events.append(OrderCreated(order_id="1"))
        raise RuntimeError("primary failed")


class QuietCreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder) -> CreatedOrder:
        return CreatedOrder(order_id="1", source="primary")


class CreateOrderFallback(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder) -> CreatedOrder:
        return CreatedOrder(order_id="1", source="fallback")


class AuditHandler(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        raise RuntimeError("audit failed")


class AuditFallback(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        return None


class CircuitOpen(Exception):
    pass


class OpenCircuitBreaker:
    async def call(self, identifier, func, *args, **kwargs):
        raise CircuitOpen()

    def is_circuit_breaker_error(self, exc: Exception) -> bool:
        return isinstance(exc, CircuitOpen)


async def test_request_fallback_returns_the_fallback_response() -> None:
    def commands_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(
            CreateOrder,
            cqrs.RequestHandlerFallback(CreateOrderHandler, CreateOrderFallback),
        )

    async with test_harness(TestContainer(), commands_mapper=commands_mapper) as harness:
        assert harness.mediator is not None
        result = await harness.mediator.send(CreateOrder(sku="book"))
        assert result.source == "fallback"
        harness.invocations.assert_entered(CreateOrderHandler)
        harness.invocations.assert_entered(CreateOrderFallback)
        assert harness.requests.of(CreateOrder)[0].response.source == "fallback"


async def test_event_fallback_for_command_and_event_mediator() -> None:
    def domain_events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(OrderCreated, cqrs.EventHandlerFallback(AuditHandler, AuditFallback))

    class EmittingHandler(cqrs.RequestHandler[CreateOrder]):
        def __init__(self) -> None:
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> list[cqrs.Event]:
            return self._events

        async def handle(self, request: CreateOrder) -> CreatedOrder:
            self._events.append(OrderCreated(order_id="1"))
            return CreatedOrder(order_id="1")

    def emitting_commands(mapper: cqrs.RequestMap) -> None:
        mapper.bind(CreateOrder, EmittingHandler)

    async with test_harness(
        TestContainer(),
        commands_mapper=emitting_commands,
        domain_events_mapper=domain_events_mapper,
    ) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        harness.invocations.assert_entered(AuditHandler)
        harness.invocations.assert_entered(AuditFallback)

    def events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(OrderCreated, cqrs.EventHandlerFallback(AuditHandler, AuditFallback))

    async with test_harness(TestContainer(), events_mapper=events_mapper) as harness:
        assert harness.event_mediator is not None
        await harness.event_mediator.send(OrderCreated(order_id="1"))
        harness.invocations.assert_entered(AuditHandler)
        harness.invocations.assert_entered(AuditFallback)


async def test_request_circuit_breaker_skips_the_primary() -> None:
    def commands_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(
            CreateOrder,
            cqrs.RequestHandlerFallback(
                QuietCreateOrderHandler,
                CreateOrderFallback,
                circuit_breaker=OpenCircuitBreaker(),
            ),
        )

    async with test_harness(TestContainer(), commands_mapper=commands_mapper) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        harness.invocations.assert_not_entered(QuietCreateOrderHandler)
        harness.invocations.assert_entered(CreateOrderFallback)


async def test_event_circuit_breaker_skips_the_primary() -> None:
    class EmittingHandler(cqrs.RequestHandler[CreateOrder]):
        def __init__(self) -> None:
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> list[cqrs.Event]:
            return self._events

        async def handle(self, request: CreateOrder) -> CreatedOrder:
            self._events.append(OrderCreated(order_id="1"))
            return CreatedOrder(order_id="1")

    def commands_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(CreateOrder, EmittingHandler)

    breaker = OpenCircuitBreaker()

    def domain_events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(
            OrderCreated,
            cqrs.EventHandlerFallback(AuditHandler, AuditFallback, circuit_breaker=breaker),
        )

    async with test_harness(
        TestContainer(),
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
    ) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        harness.invocations.assert_not_entered(AuditHandler)
        harness.invocations.assert_entered(AuditFallback)

    def events_mapper(mapper: cqrs.EventMap) -> None:
        mapper.bind(
            OrderCreated,
            cqrs.EventHandlerFallback(
                AuditHandler,
                AuditFallback,
                circuit_breaker=OpenCircuitBreaker(),
            ),
        )

    async with test_harness(TestContainer(), events_mapper=events_mapper) as harness:
        assert harness.event_mediator is not None
        await harness.event_mediator.send(OrderCreated(order_id="1"))
        harness.invocations.assert_not_entered(AuditHandler)
        harness.invocations.assert_entered(AuditFallback)
