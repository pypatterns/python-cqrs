"""One saga step and its compensation.

Full demo: examples/saga/saga.py.
Recovery demo: examples/saga/saga_recovery.py.
"""

import dataclasses
import uuid

import cqrs
from cqrs.saga.models import SagaContext


@dataclasses.dataclass
class OrderContext(SagaContext):
    """Ids later steps and compensations read. One instance for the whole run."""

    order_id: str
    reservation_id: str | None = None


class Inventory:
    async def reserve(self, order_id: str) -> str:
        return "reservation-" + order_id

    async def release(self, reservation_id: str) -> None:
        del reservation_id


class ReserveInventory(cqrs.SagaStepHandler[OrderContext, cqrs.Response]):
    def __init__(self, inventory: Inventory) -> None:
        self._inventory = inventory

    async def act(self, context: OrderContext) -> cqrs.SagaStepResult:
        context.reservation_id = await self._inventory.reserve(context.order_id)
        return self._generate_step_result(cqrs.Response())

    async def compensate(self, context: OrderContext) -> None:
        if context.reservation_id:
            await self._inventory.release(context.reservation_id)


class OrderSaga(cqrs.Saga[OrderContext]):
    steps = [ReserveInventory]


def sagas_mapper(mapper: cqrs.SagaMap) -> None:
    mapper.bind(OrderContext, OrderSaga)


async def run_order(mediator: cqrs.SagaMediator, order_id: str, saga_id: uuid.UUID) -> None:
    context = OrderContext(order_id=order_id)
    async for _step in mediator.execute(context, saga_id=saga_id):
        continue
