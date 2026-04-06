"""
Example: Saga Recovery Scheduler (while + sleep)

This example demonstrates how to run a simple recovery scheduler that periodically
scans for stuck or failed sagas and recovers them. The scheduler uses a plain
while loop with asyncio.sleep, suitable for a dedicated worker process or
a background task.

PROBLEM: Recovering Failed Sagas in Production
=============================================

When sagas run in production, processes can crash, time out, or be restarted.
Incomplete sagas (RUNNING, COMPENSATING, FAILED) must be recovered so that:
- Forward execution can complete
- Compensation can finish
- The system reaches eventual consistency

A recovery job must:
1. Find sagas that need recovery (not currently being executed)
2. Avoid picking the same saga twice (e.g. limit by recovery_attempts)
3. Run periodically without blocking the main application

SOLUTION: While Loop + get_sagas_for_recovery
=============================================

Use get_sagas_for_recovery(limit=..., max_recovery_attempts=..., stale_after_seconds=...)
to select only "stale" sagas (updated_at older than threshold), then call
recover_saga for each. On recovery failure, recover_saga calls
increment_recovery_attempts under the hood so the saga can be retried or
excluded later; callers only need to call recover_saga.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/saga_recovery_scheduler.py

The example will:
- Create an in-memory storage and one interrupted saga (simulated crash)
- Run the recovery scheduler loop for a few iterations
- Recover the interrupted saga on the first iteration
- Show that subsequent iterations find no sagas to recover

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Recovery scheduler loop:
   - while True with asyncio.sleep(interval_seconds)
   - get_sagas_for_recovery(limit, max_recovery_attempts, stale_after_seconds)
   - Per-saga recover_saga() only; increment_recovery_attempts is done inside recover_saga on failure
   - When storage supports create_run() (e.g. MemorySagaStorage, SqlAlchemySagaStorage),
     saga execution uses one session per saga and checkpoint commits

2. Staleness filter (stale_after_seconds):
   - Only sagas not updated recently are considered (avoids recovering
     sagas that are currently being executed by another worker)

3. Max recovery attempts:
   - Sagas that fail recovery too many times are excluded from selection
   - After increment_recovery_attempts, they can be retried until max is reached

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - pydantic (for context models)

This example declares its own domain model (OrderContext), step handlers,
services, saga (OrderSaga), and container; it does not depend on other examples.

================================================================================
"""

import asyncio
import dataclasses
import datetime
import logging
import typing
import uuid

from cqrs import container as cqrs_container
from cqrs.events.event import Event
from cqrs.response import Response
from cqrs.saga.models import SagaContext
from cqrs.saga.recovery import recover_saga
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.memory import MemorySagaStorage
from cqrs.saga.storage.protocol import ISagaStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# Domain Models
# ============================================================================


@dataclasses.dataclass
class OrderContext(SagaContext):
    """Shared context passed between all saga steps."""

    order_id: str
    user_id: str
    items: list[str]
    total_amount: float
    shipping_address: str

    inventory_reservation_id: typing.Optional[str] = None
    payment_id: typing.Optional[str] = None
    shipment_id: typing.Optional[str] = None


# ============================================================================
# Step Responses
# ============================================================================


class ReserveInventoryResponse(Response):
    """Response from inventory reservation step."""

    reservation_id: str
    items_reserved: list[str]


class ProcessPaymentResponse(Response):
    """Response from payment processing step."""

    payment_id: str
    amount_charged: float
    transaction_id: str


class ShipOrderResponse(Response):
    """Response from shipping step."""

    shipment_id: str
    tracking_number: str
    estimated_delivery: str


# ============================================================================
# Domain Events (minimal for step handlers)
# ============================================================================


class InventoryReservedEvent(Event, frozen=True):
    """Event emitted when inventory is reserved."""

    order_id: str
    reservation_id: str
    items: list[str]


class PaymentProcessedEvent(Event, frozen=True):
    """Event emitted when payment is processed."""

    order_id: str
    payment_id: str
    amount: float


class OrderShippedEvent(Event, frozen=True):
    """Event emitted when order is shipped."""

    order_id: str
    shipment_id: str
    tracking_number: str


# ============================================================================
# Mock Services
# ============================================================================


class InventoryService:
    """Mock inventory service for reserving and releasing items."""

    def __init__(self) -> None:
        self._reservations: dict[str, list[str]] = {}
        self._available_items: dict[str, int] = {
            "item_1": 10,
            "item_2": 5,
            "item_3": 8,
        }

    async def reserve_items(self, order_id: str, items: list[str]) -> str:
        reservation_id = f"reservation_{order_id}"
        reserved_items = []
        for item_id in items:
            if item_id not in self._available_items:
                raise ValueError(f"Item {item_id} not found")
            if self._available_items[item_id] <= 0:
                raise ValueError(f"Insufficient inventory for {item_id}")
            self._available_items[item_id] -= 1
            reserved_items.append(item_id)
        self._reservations[reservation_id] = reserved_items
        logger.info("  ✓ Reserved items %s for order %s", reserved_items, order_id)
        return reservation_id

    async def release_items(self, reservation_id: str) -> None:
        if reservation_id not in self._reservations:
            return
        items = self._reservations[reservation_id]
        for item_id in items:
            self._available_items[item_id] += 1
        del self._reservations[reservation_id]
        logger.info("  ↻ Released items %s from reservation %s", items, reservation_id)


class PaymentService:
    """Mock payment service for processing payments and refunds."""

    def __init__(self) -> None:
        self._payments: dict[str, float] = {}
        self._transaction_counter = 0

    async def charge(self, order_id: str, amount: float) -> tuple[str, str]:
        if amount <= 0:
            raise ValueError("Payment amount must be positive")
        self._transaction_counter += 1
        payment_id = f"payment_{order_id}"
        transaction_id = f"txn_{self._transaction_counter:06d}"
        self._payments[payment_id] = amount
        logger.info(
            "  ✓ Charged $%.2f for order %s (transaction: %s)",
            amount,
            order_id,
            transaction_id,
        )
        return payment_id, transaction_id

    async def refund(self, payment_id: str) -> None:
        if payment_id not in self._payments:
            return
        amount = self._payments[payment_id]
        del self._payments[payment_id]
        logger.info("  ↻ Refunded $%.2f for payment %s", amount, payment_id)


class ShippingService:
    """Mock shipping service for creating shipments."""

    def __init__(self) -> None:
        self._shipments: dict[str, str] = {}
        self._tracking_counter = 0

    async def create_shipment(
        self,
        order_id: str,
        items: list[str],
        address: str,
    ) -> tuple[str, str]:
        if not address:
            raise ValueError("Shipping address is required")
        self._tracking_counter += 1
        shipment_id = f"shipment_{order_id}"
        tracking_number = f"TRACK{self._tracking_counter:08d}"
        self._shipments[shipment_id] = tracking_number
        logger.info(
            "  ✓ Created shipment %s for order %s (tracking: %s)",
            shipment_id,
            order_id,
            tracking_number,
        )
        return shipment_id, tracking_number

    async def cancel_shipment(self, shipment_id: str) -> None:
        if shipment_id not in self._shipments:
            return
        tracking_number = self._shipments[shipment_id]
        del self._shipments[shipment_id]
        logger.info(
            "  ↻ Cancelled shipment %s (tracking: %s)",
            shipment_id,
            tracking_number,
        )


# ============================================================================
# Saga Step Handlers
# ============================================================================


class ReserveInventoryStep(
    SagaStepHandler[OrderContext, ReserveInventoryResponse],
):
    """Step 1: Reserve inventory items for the order."""

    def __init__(self, inventory_service: InventoryService) -> None:
        self._inventory_service = inventory_service
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        reservation_id = await self._inventory_service.reserve_items(
            order_id=context.order_id,
            items=context.items,
        )
        context.inventory_reservation_id = reservation_id
        self._events.append(
            InventoryReservedEvent(
                order_id=context.order_id,
                reservation_id=reservation_id,
                items=context.items,
            ),
        )
        response = ReserveInventoryResponse(
            reservation_id=reservation_id,
            items_reserved=context.items,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        if context.inventory_reservation_id:
            await self._inventory_service.release_items(
                context.inventory_reservation_id,
            )


class ProcessPaymentStep(
    SagaStepHandler[OrderContext, ProcessPaymentResponse],
):
    """Step 2: Process payment for the order."""

    def __init__(self, payment_service: PaymentService) -> None:
        self._payment_service = payment_service
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ProcessPaymentResponse]:
        payment_id, transaction_id = await self._payment_service.charge(
            order_id=context.order_id,
            amount=context.total_amount,
        )
        context.payment_id = payment_id
        self._events.append(
            PaymentProcessedEvent(
                order_id=context.order_id,
                payment_id=payment_id,
                amount=context.total_amount,
            ),
        )
        response = ProcessPaymentResponse(
            payment_id=payment_id,
            amount_charged=context.total_amount,
            transaction_id=transaction_id,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        if context.payment_id:
            await self._payment_service.refund(context.payment_id)


class ShipOrderStep(SagaStepHandler[OrderContext, ShipOrderResponse]):
    """Step 3: Create shipment for the order."""

    def __init__(self, shipping_service: ShippingService) -> None:
        self._shipping_service = shipping_service
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ShipOrderResponse]:
        shipment_id, tracking_number = await self._shipping_service.create_shipment(
            order_id=context.order_id,
            items=context.items,
            address=context.shipping_address,
        )
        context.shipment_id = shipment_id
        self._events.append(
            OrderShippedEvent(
                order_id=context.order_id,
                shipment_id=shipment_id,
                tracking_number=tracking_number,
            ),
        )
        response = ShipOrderResponse(
            shipment_id=shipment_id,
            tracking_number=tracking_number,
            estimated_delivery="2024-12-25",
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        if context.shipment_id:
            await self._shipping_service.cancel_shipment(context.shipment_id)


# ============================================================================
# Saga Definition
# ============================================================================


class OrderSaga(Saga[OrderContext]):
    """Order processing saga with three steps."""

    steps = [
        ReserveInventoryStep,
        ProcessPaymentStep,
        ShipOrderStep,
    ]


# ============================================================================
# Container
# ============================================================================


class SimpleContainer(cqrs_container.Container[typing.Any]):
    """Simple container for resolving step handlers."""

    def __init__(
        self,
        inventory_service: InventoryService,
        payment_service: PaymentService,
        shipping_service: ShippingService,
    ) -> None:
        self._services = {
            InventoryService: inventory_service,
            PaymentService: payment_service,
            ShippingService: shipping_service,
        }
        self._external_container: typing.Any = None

    @property
    def external_container(self) -> typing.Any:
        return self._external_container

    def attach_external_container(self, container: typing.Any) -> None:
        self._external_container = container

    async def resolve(self, type_: type) -> typing.Any:
        if type_ in self._services:
            return self._services[type_]
        if type_ == ReserveInventoryStep:
            return ReserveInventoryStep(self._services[InventoryService])
        if type_ == ProcessPaymentStep:
            return ProcessPaymentStep(self._services[PaymentService])
        if type_ == ShipOrderStep:
            return ShipOrderStep(self._services[ShippingService])
        raise ValueError(f"Unknown type: {type_}")


# ============================================================================
# Scheduler configuration
# ============================================================================

RECOVERY_INTERVAL_SECONDS = 2
RECOVERY_BATCH_LIMIT = 10
MAX_RECOVERY_ATTEMPTS = 5
STALE_AFTER_SECONDS = 60


# ============================================================================
# Recovery scheduler
# ============================================================================


def make_container() -> SimpleContainer:
    """Create a fresh container with services (e.g. after process restart)."""
    return SimpleContainer(
        inventory_service=InventoryService(),
        payment_service=PaymentService(),
        shipping_service=ShippingService(),
    )


async def run_recovery_iteration(
    storage: ISagaStorage,
    saga: OrderSaga,
    context_builder: typing.Type[OrderContext],
) -> int:
    """
    Run one recovery iteration: fetch stale sagas, recover each.

    recover_saga() increments recovery_attempts on failure under the hood;
    the caller only calls recover_saga().

    Returns the number of sagas processed (recovered or failed).
    """
    ids = await storage.get_sagas_for_recovery(
        limit=RECOVERY_BATCH_LIMIT,
        max_recovery_attempts=MAX_RECOVERY_ATTEMPTS,
        stale_after_seconds=STALE_AFTER_SECONDS,
    )
    if not ids:
        return 0

    container = make_container()
    processed = 0
    for saga_id in ids:
        try:
            logger.info(f"Recovering saga {saga_id}...")
            await recover_saga(saga, saga_id, context_builder, container, storage)
            logger.info(f"Saga {saga_id} recovered successfully.")
            processed += 1
        except RuntimeError as e:
            if "recovered in" in str(e) and "state" in str(e):
                logger.info("Saga %s recovery completed compensation", saga_id)
                processed += 1
            else:
                logger.exception("Saga %s recovery failed", saga_id)
                processed += 1
        except Exception:
            logger.exception("Saga %s recovery failed", saga_id)
            processed += 1
    return processed


async def recovery_loop(
    storage: ISagaStorage,
    *,
    interval_seconds: float = RECOVERY_INTERVAL_SECONDS,
    max_iterations: typing.Optional[int] = None,
) -> None:
    """
    Run the recovery scheduler loop.

    Args:
        storage: Saga storage (e.g. MemorySagaStorage or SqlAlchemySagaStorage).
        interval_seconds: Sleep duration between iterations.
        max_iterations: If set, stop after this many iterations (for demo).
                        None = run until cancelled.
    """
    saga = OrderSaga()
    iteration = 0
    while True:
        iteration += 1
        logger.info(f"Recovery iteration {iteration}")
        try:
            processed = await run_recovery_iteration(
                storage,
                saga,
                OrderContext,
            )
            if processed > 0:
                logger.info(f"Processed {processed} saga(s) this iteration.")
            else:
                logger.debug("No sagas to recover.")
        except asyncio.CancelledError:
            logger.info("Recovery loop cancelled.")
            raise
        except Exception:
            logger.exception("Recovery iteration failed")

        if max_iterations is not None and iteration >= max_iterations:
            logger.info(f"Reached max_iterations={max_iterations}, stopping.")
            break
        await asyncio.sleep(interval_seconds)


# ============================================================================
# Demo: create one interrupted saga, then run scheduler
# ============================================================================


async def create_interrupted_saga(storage: MemorySagaStorage) -> uuid.UUID:
    """
    Create one saga in RUNNING state (simulating crash after first step).
    Returns without recovering.
    """
    saga_id = uuid.uuid4()
    context = OrderContext(
        order_id="order_scheduler_demo",
        user_id="user_1",
        items=["item_1"],
        total_amount=99.99,
        shipping_address="123 Main St",
    )

    await storage.create_saga(
        saga_id=saga_id,
        name="order_saga",
        context=context.to_dict(),
    )
    await storage.update_status(saga_id, SagaStatus.RUNNING)
    await storage.log_step(
        saga_id,
        "ReserveInventoryStep",
        "act",
        SagaStepStatus.STARTED,
    )
    await storage.log_step(
        saga_id,
        "ReserveInventoryStep",
        "act",
        SagaStepStatus.COMPLETED,
    )
    ctx_dict = context.to_dict()
    ctx_dict["inventory_reservation_id"] = "reservation_order_scheduler_demo"
    await storage.update_context(saga_id, ctx_dict)

    logger.info("Created interrupted saga %s (RUNNING, one step done).", saga_id)
    return saga_id


async def main() -> None:
    """
    Run the saga recovery scheduler demo and display its outcome.

    Sets up an in-memory saga storage, creates a simulated interrupted saga and marks it stale, runs the recovery loop for three iterations (using the module's recovery_loop and recovery configuration constants), then loads and prints the final saga state.
    """
    print("\n" + "=" * 70)
    print("SAGA RECOVERY SCHEDULER EXAMPLE")
    print("=" * 70)
    print("\nThis example demonstrates:")
    print("  1. A simple while-loop recovery scheduler with asyncio.sleep")
    print(
        "  2. get_sagas_for_recovery(limit, max_recovery_attempts, stale_after_seconds)",
    )
    print(
        "  3. recover_saga() per saga (increment_recovery_attempts on failure is internal)",
    )

    storage = MemorySagaStorage()  # supports create_run(): scoped run when executing sagas

    saga_id = await create_interrupted_saga(storage)
    storage._sagas[saga_id]["updated_at"] = datetime.datetime.now(
        datetime.timezone.utc,
    ) - datetime.timedelta(seconds=STALE_AFTER_SECONDS + 10)

    print("\nRunning recovery loop for 3 iterations (interval=2s)...")
    print("  Iteration 1 should recover the interrupted saga.")
    print("  Iteration 2 and 3 should find no sagas.\n")

    await recovery_loop(
        storage,
        interval_seconds=RECOVERY_INTERVAL_SECONDS,
        max_iterations=3,
    )

    status, context_data, _ = await storage.load_saga_state(saga_id)
    print("\n" + "-" * 70)
    print(f"Final state of saga {saga_id}:")
    print(f"  Status: {status}")
    print("-" * 70)
    print("\nEXAMPLE COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
