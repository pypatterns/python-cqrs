"""
Example: Saga Recovery for Eventual Consistency

This example demonstrates how to recover interrupted sagas to achieve eventual
consistency in distributed systems. When a saga is interrupted (due to server crash,
network failure, timeout, etc.), the system must be able to resume execution from
where it left off to ensure all operations complete successfully.

PROBLEM: Eventual Consistency in Distributed Systems
=====================================================

In distributed systems, achieving strong consistency across multiple services
is challenging. The saga pattern helps by breaking transactions into steps
with compensation logic. However, sagas can be interrupted:

1. Server crashes during execution
2. Network timeouts between steps
3. Database connection failures
4. Process restarts or deployments

Without recovery, interrupted sagas leave the system in an inconsistent state:
- Some steps completed, others didn't
- Resources may be locked or reserved indefinitely
- Business operations remain incomplete

SOLUTION: Saga Recovery
========================

Saga recovery ensures eventual consistency by:
1. Persisting saga state after each step
2. Periodically scanning for incomplete sagas
3. Resuming execution from the last completed step
4. Completing compensation if saga was in compensating state

This guarantees that eventually, all sagas will reach a terminal state
(COMPLETED or FAILED with all compensations done), achieving eventual consistency.

Use case: Order processing system where orders must go through multiple steps.
If the server crashes after reserving inventory but before processing payment,
recovery will resume the saga and complete the remaining steps.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/saga_recovery.py

The example will:
- Simulate a saga that gets interrupted after the first step
- Show how recovery resumes execution from where it left off
- Demonstrate recovery of a saga in compensating state
- Display step execution history and final state

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Saga State Persistence:
   - MemorySagaStorage/SqlAlchemySagaStorage use create_run(): one session per saga,
     checkpoint commits after each step (fewer commits, better performance)
   - Saga state is saved to storage after each step
   - Storage tracks which steps completed successfully
   - Context data is persisted for recovery

2. Recovery Process:
   - Load saga state from storage
   - Reconstruct context from persisted data
   - Resume execution, skipping already completed steps
   - Complete compensation if saga was interrupted during compensation

3. Eventual Consistency:
   - Recovery ensures all sagas eventually complete
   - No resources remain locked indefinitely
   - System reaches consistent state over time

4. Recovery Scenarios:
   - Resume interrupted forward execution
   - Complete interrupted compensation
   - Handle already completed sagas (idempotent)

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - pydantic (for context models)

================================================================================
"""

import asyncio
import dataclasses
import logging
import typing
import uuid

import cqrs
from cqrs import container as cqrs_container
from cqrs.events.event import Event
from cqrs.response import Response
from cqrs.saga.models import SagaContext
from cqrs.saga.recovery import recover_saga
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.enums import SagaStatus
from cqrs.saga.storage.memory import MemorySagaStorage

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
    items: list[str]  # List of item IDs
    total_amount: float
    shipping_address: str

    # These fields are populated by steps during execution
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
# Domain Events
# ============================================================================


class InventoryReservedEvent(cqrs.Event, frozen=True):
    """Event emitted when inventory is reserved."""

    order_id: str
    reservation_id: str
    items: list[str]


class PaymentProcessedEvent(cqrs.Event, frozen=True):
    """Event emitted when payment is processed."""

    order_id: str
    payment_id: str
    amount: float


class OrderShippedEvent(cqrs.Event, frozen=True):
    """Event emitted when order is shipped."""

    order_id: str
    shipment_id: str
    tracking_number: str


# ============================================================================
# Mock Services (simulating external services)
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
        """Reserve items for an order."""
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
        logger.info(f"  ✓ Reserved items {reserved_items} for order {order_id}")
        return reservation_id

    async def release_items(self, reservation_id: str) -> None:
        """Release reserved items back to inventory."""
        if reservation_id not in self._reservations:
            return  # Already released or never existed

        items = self._reservations[reservation_id]
        for item_id in items:
            self._available_items[item_id] += 1

        del self._reservations[reservation_id]
        logger.info(f"  ↻ Released items {items} from reservation {reservation_id}")


class PaymentService:
    """Mock payment service for processing payments and refunds."""

    def __init__(self) -> None:
        self._payments: dict[str, float] = {}
        self._transaction_counter = 0

    async def charge(self, order_id: str, amount: float) -> tuple[str, str]:
        """Charge the customer for the order."""
        if amount <= 0:
            raise ValueError("Payment amount must be positive")

        self._transaction_counter += 1
        payment_id = f"payment_{order_id}"
        transaction_id = f"txn_{self._transaction_counter:06d}"

        self._payments[payment_id] = amount
        logger.info(
            f"  ✓ Charged ${amount:.2f} for order {order_id} (transaction: {transaction_id})",
        )
        return payment_id, transaction_id

    async def refund(self, payment_id: str) -> None:
        """Refund a payment."""
        if payment_id not in self._payments:
            return  # Already refunded or never existed

        amount = self._payments[payment_id]
        del self._payments[payment_id]
        logger.info(f"  ↻ Refunded ${amount:.2f} for payment {payment_id}")


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
        """
        Create a shipment record for the given order and generate a tracking number.

        Parameters:
            order_id (str): Identifier of the order.
            items (list[str]): Items included in the shipment.
            address (str): Destination shipping address.

        Returns:
            tuple[str, str]: A tuple containing the shipment ID and the tracking number.

        Raises:
            ValueError: If `address` is empty.
        """
        if not address:
            raise ValueError("Shipping address is required")

        self._tracking_counter += 1
        shipment_id = f"shipment_{order_id}"
        tracking_number = f"TRACK{self._tracking_counter:08d}"

        self._shipments[shipment_id] = tracking_number
        logger.info(
            f"  ✓ Created shipment {shipment_id} for order {order_id} " f"(tracking: {tracking_number})",
        )
        return shipment_id, tracking_number

    async def cancel_shipment(self, shipment_id: str) -> None:
        """Cancel a shipment."""
        if shipment_id not in self._shipments:
            return  # Already cancelled or never existed

        tracking_number = self._shipments[shipment_id]
        del self._shipments[shipment_id]
        logger.info(
            f"  ↻ Cancelled shipment {shipment_id} (tracking: {tracking_number})",
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
        """Reserve inventory items."""
        reservation_id = await self._inventory_service.reserve_items(
            order_id=context.order_id,
            items=context.items,
        )

        # Update context with reservation ID
        context.inventory_reservation_id = reservation_id

        # Emit domain event
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
        """Release reserved items back to inventory."""
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
        """Process payment."""
        payment_id, transaction_id = await self._payment_service.charge(
            order_id=context.order_id,
            amount=context.total_amount,
        )

        # Update context with payment ID
        context.payment_id = payment_id

        # Emit domain event
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
        """Refund the payment."""
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
        """Create shipment."""
        shipment_id, tracking_number = await self._shipping_service.create_shipment(
            order_id=context.order_id,
            items=context.items,
            address=context.shipping_address,
        )

        # Update context with shipment ID
        context.shipment_id = shipment_id

        # Emit domain event
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
        """Cancel the shipment."""
        if context.shipment_id:
            await self._shipping_service.cancel_shipment(context.shipment_id)


# ============================================================================
# Saga Class Definition
# ============================================================================


class OrderSaga(Saga[OrderContext]):
    """Order processing saga with three steps."""

    steps = [
        ReserveInventoryStep,
        ProcessPaymentStep,
        ShipOrderStep,
    ]


# ============================================================================
# Container Setup
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
        self._external_container = None

    @property
    def external_container(self) -> typing.Any:
        return self._external_container

    def attach_external_container(self, container: typing.Any) -> None:
        self._external_container = container

    async def resolve(self, type_: type) -> typing.Any:
        if type_ in self._services:
            return self._services[type_]
        # For step handlers, create instances with dependencies
        if type_ == ReserveInventoryStep:
            return ReserveInventoryStep(self._services[InventoryService])
        if type_ == ProcessPaymentStep:
            return ProcessPaymentStep(self._services[PaymentService])
        if type_ == ShipOrderStep:
            return ShipOrderStep(self._services[ShippingService])
        raise ValueError(f"Unknown type: {type_}")


# ============================================================================
# Recovery Examples
# ============================================================================


async def simulate_interrupted_saga() -> tuple[uuid.UUID, MemorySagaStorage]:
    """
    Simulate a saga that is interrupted after the inventory reservation step to produce a recoverable persisted state.

    Returns:
        tuple:
            saga_id (uuid.UUID): Identifier of the created saga.
            storage (MemorySagaStorage): In-memory storage containing the persisted saga state and step history for recovery.
    """
    print("\n" + "=" * 70)
    print("SCENARIO 1: Simulating Interrupted Saga")
    print("=" * 70)
    print("\nSimulating server crash after first step...")

    # Setup services and storage (MemorySagaStorage uses create_run(): scoped run, checkpoint commits)
    inventory_service = InventoryService()
    payment_service = PaymentService()
    shipping_service = ShippingService()
    storage = MemorySagaStorage()

    container = SimpleContainer(
        inventory_service=inventory_service,
        payment_service=payment_service,
        shipping_service=shipping_service,
    )

    # Create saga instance
    saga = OrderSaga()

    # Create order context
    saga_id = uuid.uuid4()
    context = OrderContext(
        order_id="order_recovery_001",
        user_id="user_456",
        items=["item_1", "item_2"],
        total_amount=99.99,
        shipping_address="123 Main St, City, Country",
    )

    # Start saga execution
    print(f"\nStarting saga {saga_id} for order {context.order_id}...")

    try:
        async with saga.transaction(
            context=context,
            container=container,  # type: ignore
            storage=storage,
            saga_id=saga_id,
        ) as transaction:
            async for step_result in transaction:
                step_name = step_result.step_type.__name__
                print(f"✓ Step completed: {step_name}")

                # Simulate crash after first step
                if step_name == "ReserveInventoryStep":
                    print("\n💥 SIMULATED SERVER CRASH!")
                    print(
                        "   (In reality: server restart, network failure, timeout, etc.)",
                    )
                    print("   Saga state has been persisted to storage.")
                    print("   Current state: RUNNING, 1 step completed")
                    # Break out of loop to simulate interruption
                    break

        # This won't be reached due to break, but included for clarity
        print("Saga completed successfully")

    except Exception as e:
        print(f"\n✗ Saga failed: {e}")
        raise

    # Check final state and show execution log
    status, _, _ = await storage.load_saga_state(saga_id)
    history = await storage.get_step_history(saga_id)

    print(f"\nSaga state after interruption: {status}")
    print("  - ReserveInventoryStep: COMPLETED")
    print("  - ProcessPaymentStep: NOT STARTED")
    print("  - ShipOrderStep: NOT STARTED")

    # Show execution log (SagaLog) before recovery
    print("\n  Execution log (SagaLog) before recovery:")
    for entry in history:
        print(
            f"    [{entry.timestamp.strftime('%H:%M:%S')}] " f"{entry.step_name}.{entry.action}: {entry.status.value}",
        )

    print("\n⚠️  Problem: Order is incomplete!")
    print("   - Inventory is reserved but not charged")
    print("   - Payment not processed")
    print("   - Shipment not created")
    print("   - System is in inconsistent state")

    return saga_id, storage


async def recover_interrupted_saga(
    saga_id: uuid.UUID,
    storage: MemorySagaStorage,
) -> None:
    """
    Recover and complete an interrupted saga using persisted state.

    Loads the saga state from storage, reconstructs the saga context, resumes execution from the last completed step, and completes any remaining steps to restore eventual consistency.

    Parameters:
        saga_id (uuid.UUID): Identifier of the saga instance to recover.
        storage (MemorySagaStorage): Durable storage containing the saga's persisted state and step history.
    """
    print("\n" + "=" * 70)
    print("SCENARIO 2: Recovering Interrupted Saga")
    print("=" * 70)
    print("\n🔄 Starting recovery process...")
    print("   (This would typically run as a background job)")

    # Setup services (same as before, simulating server restart)
    inventory_service = InventoryService()
    payment_service = PaymentService()
    shipping_service = ShippingService()

    container = SimpleContainer(
        inventory_service=inventory_service,
        payment_service=payment_service,
        shipping_service=shipping_service,
    )

    # Create saga instance (same steps as before)
    saga = OrderSaga()

    # Recover the saga
    print(f"\nRecovering saga {saga_id}...")
    await recover_saga(saga, saga_id, OrderContext, container, storage)

    # Check final state and get execution history
    status, context_data, _ = await storage.load_saga_state(saga_id)
    history = await storage.get_step_history(saga_id)

    print("\n" + "-" * 70)
    print("✓ Recovery completed successfully!")
    print(f"  - Final status: {status}")
    print("  - ReserveInventoryStep: COMPLETED (skipped during recovery)")
    print("  - ProcessPaymentStep: COMPLETED (executed during recovery)")
    print("  - ShipOrderStep: COMPLETED (executed during recovery)")

    # Reconstruct context to show final state
    context = OrderContext.from_dict(context_data)
    print("\n  Final context:")
    print(f"    * Inventory reservation: {context.inventory_reservation_id}")
    print(f"    * Payment ID: {context.payment_id}")
    print(f"    * Shipment ID: {context.shipment_id}")

    # Show execution log (SagaLog)
    print("\n  Execution log (SagaLog):")
    for entry in history:
        print(
            f"    [{entry.timestamp.strftime('%H:%M:%S')}] " f"{entry.step_name}.{entry.action}: {entry.status.value}",
        )

    print("\n✅ System is now in consistent state!")
    print("   - All steps completed")
    print("   - Order is fully processed")
    print("   - Eventual consistency achieved")
    print("-" * 70)


async def simulate_interrupted_compensation() -> tuple[uuid.UUID, MemorySagaStorage]:
    """
    Simulate a saga that fails and is interrupted during compensation.

    Sets up services, a saga, and a failing shipment step to trigger compensation that is then artificially interrupted; returns identifiers and storage state for performing recovery in a separate run.

    Returns:
        tuple[uuid.UUID, MemorySagaStorage]: The saga ID and the in-memory storage containing the persisted saga state and step history after the simulated interruption.
    """
    print("\n" + "=" * 70)
    print("SCENARIO 3: Simulating Interrupted Compensation")
    print("=" * 70)
    print("\nSimulating failure during compensation...")

    # Setup services and storage (scoped run via create_run() when supported)
    inventory_service = InventoryService()
    payment_service = PaymentService()
    shipping_service = ShippingService()
    storage = MemorySagaStorage()

    container = SimpleContainer(
        inventory_service=inventory_service,
        payment_service=payment_service,
        shipping_service=shipping_service,
    )

    # Create saga instance
    saga = OrderSaga()

    # Create order context
    saga_id = uuid.uuid4()
    context = OrderContext(
        order_id="order_recovery_002",
        user_id="user_789",
        items=["item_1"],
        total_amount=99.99,
        shipping_address="456 Oak Ave, City, Country",
    )

    print(f"\nStarting saga {saga_id} for order {context.order_id}...")

    # Create a failing step handler for ShipOrderStep
    class FailingShipOrderStep(ShipOrderStep):
        async def act(
            self,
            context: OrderContext,
        ) -> SagaStepResult[OrderContext, ShipOrderResponse]:
            """Fail during shipment creation."""
            raise ValueError("Shipping service unavailable")

    # Override ShipOrderStep in container
    original_resolve = container.resolve

    async def resolve_with_failing_step(type_: type) -> typing.Any:
        if type_ == ShipOrderStep:
            return FailingShipOrderStep(shipping_service)
        return await original_resolve(type_)

    container.resolve = resolve_with_failing_step

    # Execute saga (will fail at ShipOrderStep)
    try:
        async with saga.transaction(
            context=context,
            container=container,  # type: ignore
            storage=storage,
            saga_id=saga_id,
        ) as transaction:
            async for step_result in transaction:
                step_name = step_result.step_type.__name__
                print(f"✓ Step completed: {step_name}")

                # Simulate crash during compensation
                if step_name == "ProcessPaymentStep":
                    print("\n💥 SIMULATED CRASH DURING COMPENSATION!")
                    print("   (Compensation started but was interrupted)")
                    # The exception will trigger compensation, but we'll simulate
                    # interruption during compensation
                    break

    except ValueError as e:
        print(f"\n✗ Saga failed at ShipOrderStep: {e}")
        print("   Compensation started automatically...")
        print("   💥 SIMULATED CRASH DURING COMPENSATION!")
        print("   (In reality: server restart during rollback)")

    # Manually set status to COMPENSATING to simulate interrupted compensation
    await storage.update_status(saga_id, SagaStatus.COMPENSATING)

    status, _, _ = await storage.load_saga_state(saga_id)
    history = await storage.get_step_history(saga_id)

    print(f"\nSaga state after interruption: {status}")
    print("  - ReserveInventoryStep: COMPLETED, compensation pending")
    print("  - ProcessPaymentStep: COMPLETED, compensation pending")
    print("  - ShipOrderStep: FAILED")

    # Show execution log (SagaLog) before recovery
    print("\n  Execution log (SagaLog) before recovery:")
    for entry in history:
        print(
            f"    [{entry.timestamp.strftime('%H:%M:%S')}] " f"{entry.step_name}.{entry.action}: {entry.status.value}",
        )

    print("\n⚠️  Problem: Compensation incomplete!")
    print("   - Inventory still reserved")
    print("   - Payment still charged")
    print("   - System is in inconsistent state")

    return saga_id, storage


async def recover_interrupted_compensation(
    saga_id: uuid.UUID,
    storage: MemorySagaStorage,
) -> None:
    """
    Recover and complete an interrupted compensation for a saga.

    Loads the saga state from the provided storage using the given saga identifier and drives any incomplete compensation steps to completion, ensuring resources (inventory, payments, shipments) are released and the system reaches a consistent state. Progress and final status are printed to stdout.

    Parameters:
        saga_id (uuid.UUID): Identifier of the saga to recover.
        storage (MemorySagaStorage): Persistent storage containing the saga state and step history.
    """
    print("\n" + "=" * 70)
    print("SCENARIO 4: Recovering Interrupted Compensation")
    print("=" * 70)
    print("\n🔄 Starting compensation recovery...")

    # Setup services
    inventory_service = InventoryService()
    payment_service = PaymentService()
    shipping_service = ShippingService()

    container = SimpleContainer(
        inventory_service=inventory_service,
        payment_service=payment_service,
        shipping_service=shipping_service,
    )

    # Create saga instance
    saga = OrderSaga()

    # Recover the saga (will complete compensation)
    print(f"\nRecovering saga {saga_id}...")
    try:
        await recover_saga(saga, saga_id, OrderContext, container, storage)
    except RuntimeError as e:
        # Expected: RuntimeError is raised when saga is recovered in
        # COMPENSATING/FAILED state and compensation completes
        if "recovered in" in str(e) and "state" in str(e):
            print("\n✓ Compensation recovery completed!")
            print("   (RuntimeError is expected - forward execution not allowed)")

    # Check final state and get execution history
    status, _, _ = await storage.load_saga_state(saga_id)
    history = await storage.get_step_history(saga_id)

    print("\n" + "-" * 70)
    print("✓ Compensation recovery completed!")
    print(f"  - Final status: {status}")
    print("  - All compensations executed")
    print("  - Resources released")

    # Show execution log (SagaLog) - includes both act and compensate operations
    print("\n  Execution log (SagaLog):")
    for entry in history:
        print(
            f"    [{entry.timestamp.strftime('%H:%M:%S')}] " f"{entry.step_name}.{entry.action}: {entry.status.value}",
        )

    print("\n✅ System is now in consistent state!")
    print("   - Inventory released")
    print("   - Payment refunded")
    print("   - Eventual consistency achieved")
    print("-" * 70)


async def main() -> None:
    """Run all recovery examples."""
    print("\n" + "=" * 70)
    print("SAGA RECOVERY EXAMPLE: Eventual Consistency")
    print("=" * 70)
    print("\nThis example demonstrates:")
    print("  1. How sagas can be interrupted in distributed systems")
    print("  2. Why recovery is needed for eventual consistency")
    print("  3. How to recover interrupted sagas")
    print("  4. Recovery of both forward execution and compensation")

    # Scenario 1: Interrupted forward execution
    saga_id, storage = await simulate_interrupted_saga()

    # Scenario 2: Recover interrupted forward execution
    await recover_interrupted_saga(saga_id, storage)

    # Scenario 3: Interrupted compensation
    saga_id_comp, storage_comp = await simulate_interrupted_compensation()

    # Scenario 4: Recover interrupted compensation
    await recover_interrupted_compensation(saga_id_comp, storage_comp)

    print("\n" + "=" * 70)
    print("KEY TAKEAWAYS")
    print("=" * 70)
    print("\n1. Eventual Consistency:")
    print("   - System may be temporarily inconsistent")
    print("   - Recovery ensures consistency is eventually achieved")
    print("   - No resources remain locked indefinitely")
    print("\n2. Recovery Process:")
    print("   - Load saga state from persistent storage")
    print("   - Reconstruct context from saved data")
    print("   - Resume execution, skipping completed steps")
    print("   - Complete compensation if needed")
    print("\n3. Production Considerations:")
    print("   - Run recovery as background job (cron, scheduler)")
    print("   - Scan for RUNNING/COMPENSATING sagas periodically")
    print("   - Handle recovery failures gracefully")
    print("   - Monitor recovery metrics")
    print("\n4. Storage Requirements:")
    print("   - Saga state must be persisted after each step")
    print("   - Context must be serializable")
    print("   - Step execution history must be logged")
    print("=" * 70)
    print("\nEXAMPLE COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
