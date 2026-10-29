"""
ScopeStrategy.SEND on di.Container gives the command handler and the domain
handler the same request-scoped session.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_scoped_session.py
"""

import di
import di.dependent

import cqrs
from cqrs.testing import test_harness


class Session:
    pass


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreated(cqrs.DomainEvent):
    order_id: str


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self, session: Session) -> None:
        self.session = session
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder):
        self._events.append(OrderCreated(order_id="1"))
        return CreatedOrder(order_id="1")


class AuditHandler(cqrs.EventHandler[OrderCreated]):
    def __init__(self, session: Session) -> None:
        self.session = session

    async def handle(self, event: OrderCreated) -> None:
        return None


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderCreated, AuditHandler)


async def test_scope_send_shares_one_session() -> None:
    # Arrange
    container = di.Container()
    container.bind(di.bind_by_type(di.dependent.Dependent(Session, scope="request"), Session))
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        scope_strategy=cqrs.ScopeStrategy.SEND,
    ) as harness:
        # Act
        await harness.mediator.send(CreateOrder(sku="book"))

        # Assert
        command_session = harness.invocations.of(CreateOrderHandler)[0].handler_instance.session
        audit_session = harness.invocations.of(AuditHandler)[0].handler_instance.session
        assert command_session is audit_session
