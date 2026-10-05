"""SagaDispatcher must collect events from Sequence, not only list."""

import typing

from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.events import DomainEvent
from cqrs.models.event import IEvent
from cqrs.mapping.requests import SagaMap
from cqrs.saga.saga import Saga
from cqrs.handlers.saga import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.memory import MemorySagaStorage

from .conftest import OrderContext, ReserveInventoryResponse, SagaContainer


class InventoryReserved(DomainEvent, frozen=True):
    order_id: str


class TupleEventsStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    def __init__(self) -> None:
        self._events: tuple[IEvent, ...] = ()

    @property
    def events(self) -> typing.Sequence[IEvent]:
        return self._events

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        self._events = (InventoryReserved(order_id=context.order_id),)
        return self._generate_step_result(
            ReserveInventoryResponse(inventory_id="inv_123", reserved=True),
        )

    async def compensate(self, context: OrderContext) -> None:
        return None


class TupleEventsSaga(Saga[OrderContext]):
    steps = [TupleEventsStep]


async def test_saga_dispatcher_collects_tuple_events() -> None:
    container = SagaContainer()
    saga_map = SagaMap()
    saga_map.bind(OrderContext, TupleEventsSaga)
    dispatcher = SagaDispatcher(
        saga_map=saga_map,
        container=container,  # type: ignore[arg-type]
        storage=MemorySagaStorage(),
    )
    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    results = [result async for result in dispatcher.dispatch(context)]

    assert len(results) == 1
    assert len(results[0].events) == 1
    event = results[0].events[0]
    assert isinstance(event, InventoryReserved)
    assert event.order_id == "123"


async def test_saga_dispatcher_collects_empty_default_events() -> None:
    class EmptyEventsStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
        async def act(
            self,
            context: OrderContext,
        ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
            return self._generate_step_result(
                ReserveInventoryResponse(inventory_id="inv_123", reserved=True),
            )

        async def compensate(self, context: OrderContext) -> None:
            return None

    class EmptyEventsSaga(Saga[OrderContext]):
        steps = [EmptyEventsStep]

    container = SagaContainer()
    saga_map = SagaMap()
    saga_map.bind(OrderContext, EmptyEventsSaga)
    dispatcher = SagaDispatcher(
        saga_map=saga_map,
        container=container,  # type: ignore[arg-type]
        storage=MemorySagaStorage(),
    )

    results = [
        result
        async for result in dispatcher.dispatch(
            OrderContext(order_id="123", user_id="user1", amount=100.0),
        )
    ]

    assert len(results) == 1
    assert results[0].events == []
