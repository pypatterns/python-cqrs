import di
from di import dependent

import cqrs
from cqrs.testing import TestContainer, test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreated(cqrs.DomainEvent):
    order_id: str


class Session:
    pass


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._events.append(OrderCreated(order_id="1"))
        return CreatedOrder(order_id="1")


class AuditHandler(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        return None


class SearchHandler(cqrs.EventHandler[OrderCreated]):
    async def handle(self, event: OrderCreated) -> None:
        return None


class ScopedCreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self, session: Session) -> None:
        self.session = session
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._events.append(OrderCreated(order_id="1"))
        return CreatedOrder(order_id="1")


class ScopedAuditHandler(cqrs.EventHandler[OrderCreated]):
    def __init__(self, session: Session) -> None:
        self.session = session

    async def handle(self, event: OrderCreated) -> None:
        return None


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderCreated, AuditHandler)
    mapper.bind(OrderCreated, SearchHandler)


async def test_send_scope_runs_domain_handlers_in_order() -> None:
    container = di.Container()
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        scope_strategy=cqrs.ScopeStrategy.SEND,
    ) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        entered = [item.handler for item in harness.invocations if item.argument_type is OrderCreated]
        assert entered == [AuditHandler, SearchHandler]


async def test_handler_and_none_scopes_record_both_handlers() -> None:
    async with test_harness(
        TestContainer(),
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        concurrent_event_handle_enable=True,
        scope_strategy=cqrs.ScopeStrategy.HANDLER,
    ) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        handlers = {item.handler for item in harness.invocations if item.argument_type is OrderCreated}
        assert handlers == {AuditHandler, SearchHandler}

    async with test_harness(
        TestContainer(),
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        concurrent_event_handle_enable=True,
        scope_strategy=cqrs.ScopeStrategy.NONE,
    ) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        handlers = {item.handler for item in harness.invocations if item.argument_type is OrderCreated}
        assert handlers == {AuditHandler, SearchHandler}


async def test_di_send_scope_shares_the_session() -> None:
    def scoped_commands(mapper: cqrs.RequestMap) -> None:
        mapper.bind(CreateOrder, ScopedCreateOrderHandler)

    def scoped_events(mapper: cqrs.EventMap) -> None:
        mapper.bind(OrderCreated, ScopedAuditHandler)

    container = di.Container()
    container.bind(di.bind_by_type(dependent.Dependent(Session, scope="request"), Session))
    async with test_harness(
        container,
        commands_mapper=scoped_commands,
        domain_events_mapper=scoped_events,
        scope_strategy=cqrs.ScopeStrategy.SEND,
    ) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        command_session = harness.invocations.of(ScopedCreateOrderHandler)[0].handler_instance.session
        audit_session = harness.invocations.of(ScopedAuditHandler)[0].handler_instance.session
        assert command_session is audit_session
