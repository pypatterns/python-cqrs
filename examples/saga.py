"""
Example: Saga Pattern for Distributed Transactions

This example demonstrates the saga pattern for managing distributed transactions
across multiple services or operations. The saga pattern enables eventual consistency
by executing a series of steps where each step can be compensated if a subsequent
step fails.

Use case: Order processing system where an order must go through multiple steps:
1. Reserve inventory items
2. Process payment
3. Ship the order

If any step fails, all previously completed steps must be compensated (rolled back)
to maintain data consistency. This pattern is essential for distributed systems
where traditional two-phase commit is not feasible.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/saga.py

The example will:
- Execute a successful order processing saga with all steps completing using SagaMediator
- Demonstrate automatic compensation when a step fails
- Show how compensation happens in reverse order
- Display step results and compensation status

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Saga Pattern Implementation:
   - Define a saga context that holds shared state across all steps
   - Create step handlers that implement act() and compensate() methods
   - Create saga mediator using bootstrap.bootstrap() with SagaMap registration
   - Execute steps sequentially using mediator.stream() with saga_id for persistence
   - Automatic compensation on failure

2. Step Handler Definition:
   - Each step inherits from SagaStepHandler[Context, Response]
   - act() method performs the step's action and returns SagaStepResult
   - compensate() method undoes the action if saga fails
   - Steps can emit domain events via the events property

3. Saga Execution:
   - Create saga mediator using bootstrap.bootstrap()
   - Register sagas in SagaMap via sagas_mapper
   - Use mediator.stream(context, saga_id) to execute saga
   - Iterate over stream to execute steps sequentially
   - Each iteration yields SagaStepResult with step response
   - Saga state and step history are persisted to storage

4. Saga Storage and Logging:
   - SagaStorage persists saga state and execution history
   - MemorySagaStorage and SqlAlchemySagaStorage support create_run(): execution
     uses one session per saga and checkpoint commits (fewer commits, better performance)
   - Each step execution is logged (act/compensate, status, timestamp)
   - Storage enables recovery of interrupted sagas
   - Use storage.get_step_history() to view execution log

5. Automatic Compensation:
   - If any step raises an exception, compensation is triggered automatically
   - Completed steps are compensated in reverse order
   - Original exception is re-raised after compensation
   - Compensation steps are also logged to storage

6. Dependency Injection:
   - Step handlers are resolved from DI container
   - Services can be injected into step handlers
   - Enables testability and loose coupling

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

import di
from di import dependent

import cqrs
from cqrs.events.event import Event
from cqrs.response import Response
from cqrs.saga import bootstrap
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.memory import MemorySagaStorage

logging.basicConfig(level=logging.INFO)


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
        print(f"  ✓ Reserved items {reserved_items} for order {order_id}")
        return reservation_id

    async def release_items(self, reservation_id: str) -> None:
        """Release reserved items back to inventory."""
        if reservation_id not in self._reservations:
            return  # Already released or never existed

        items = self._reservations[reservation_id]
        for item_id in items:
            self._available_items[item_id] += 1

        del self._reservations[reservation_id]
        print(f"  ↻ Released items {items} from reservation {reservation_id}")


class PaymentService:
    """Mock payment service for processing payments and refunds."""

    def __init__(self) -> None:
        self._payments: dict[str, float] = {}
        self._transaction_counter = 0

    async def charge(self, order_id: str, amount: float) -> tuple[str, str]:
        """Charge the customer for the order."""
        if amount <= 0:
            raise ValueError("Payment amount must be positive")
        if amount > 10000:  # Simulate payment failure for large amounts
            raise ValueError("Payment amount exceeds limit")

        self._transaction_counter += 1
        payment_id = f"payment_{order_id}"
        transaction_id = f"txn_{self._transaction_counter:06d}"

        self._payments[payment_id] = amount
        print(
            f"  ✓ Charged ${amount:.2f} for order {order_id} (transaction: {transaction_id})",
        )
        return payment_id, transaction_id

    async def refund(self, payment_id: str) -> None:
        """Refund a payment."""
        if payment_id not in self._payments:
            return  # Already refunded or never existed

        amount = self._payments[payment_id]
        del self._payments[payment_id]
        print(f"  ↻ Refunded ${amount:.2f} for payment {payment_id}")


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
        Create a shipment for an order and record its tracking number.

        Parameters:
            order_id (str): Identifier of the order to ship.
            items (list[str]): List of item identifiers included in the shipment.
            address (str): Shipping address; must not be empty.

        Returns:
            tuple[str, str]: A tuple containing the created `shipment_id` and its `tracking_number`.

        Raises:
            ValueError: If `address` is empty.
        """
        if not address:
            raise ValueError("Shipping address is required")

        self._tracking_counter += 1
        shipment_id = f"shipment_{order_id}"
        tracking_number = f"TRACK{self._tracking_counter:08d}"

        self._shipments[shipment_id] = tracking_number
        print(
            f"  ✓ Created shipment {shipment_id} for order {order_id} " f"(tracking: {tracking_number})",
        )
        return shipment_id, tracking_number

    async def cancel_shipment(self, shipment_id: str) -> None:
        """Cancel a shipment."""
        if shipment_id not in self._shipments:
            return  # Already cancelled or never existed

        tracking_number = self._shipments[shipment_id]
        del self._shipments[shipment_id]
        print(f"  ↻ Cancelled shipment {shipment_id} (tracking: {tracking_number})")


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
# Main Example
# ============================================================================


async def run_successful_saga() -> None:
    """
    Run an example order-processing saga and print the per-step progress and final results.

    Sets up mock services, dependency injection, and in-memory saga storage; executes the OrderSaga with a generated saga ID, prints each completed step, then prints the final saga status, context fields (inventory reservation, payment ID, shipment ID) and the persisted execution log. If saga execution fails, the failure is printed and the exception is re-raised.
    """
    print("\n" + "=" * 70)
    print("SCENARIO 1: Successful Order Processing Saga")
    print("=" * 70)

    # Setup services
    inventory_service = InventoryService()
    payment_service = PaymentService()
    shipping_service = ShippingService()

    # Create saga storage for persistence.
    # MemorySagaStorage (and SqlAlchemySagaStorage) support create_run():
    # execution uses one session per saga and checkpoint commits for better performance.
    # In production, use SqlAlchemySagaStorage or another persistent storage.
    storage = MemorySagaStorage()

    # Setup DI container
    di_container = di.Container()
    # Register services using bind_by_type with lambda functions that return instances
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: inventory_service, scope="request"),
            InventoryService,
        ),
    )
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: payment_service, scope="request"),
            PaymentService,
        ),
    )
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: shipping_service, scope="request"),
            ShippingService,
        ),
    )
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: storage, scope="request"),
            MemorySagaStorage,
        ),
    )

    # Register step handlers
    reserve_step = ReserveInventoryStep(inventory_service)
    payment_step = ProcessPaymentStep(payment_service)
    ship_step = ShipOrderStep(shipping_service)

    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: reserve_step, scope="request"),
            ReserveInventoryStep,
        ),
    )
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: payment_step, scope="request"),
            ProcessPaymentStep,
        ),
    )
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: ship_step, scope="request"),
            ShipOrderStep,
        ),
    )

    # Define saga mapper
    def saga_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    # Create saga mediator using bootstrap
    mediator = bootstrap.bootstrap(
        di_container=di_container,
        sagas_mapper=saga_mapper,
        saga_storage=storage,
    )

    # Create order context
    saga_id = uuid.uuid4()
    context = OrderContext(
        order_id="order_123",
        user_id="user_456",
        items=["item_1", "item_2"],
        total_amount=99.99,
        shipping_address="123 Main St, City, Country",
    )

    # Execute saga with saga_id for persistence using mediator.stream()
    print(f"\nProcessing order {context.order_id} (saga_id: {saga_id})...")
    step_results = []

    try:
        async for step_result in mediator.stream(context, saga_id=saga_id):
            step_results.append(step_result)
            step_name = step_result.step_type.__name__
            print(f"\n✓ Step completed: {step_name}")

        # Get execution history from storage
        history = await storage.get_step_history(saga_id)
        status, _, _ = await storage.load_saga_state(saga_id)

        print("\n" + "-" * 70)
        print("✓ Saga completed successfully!")
        print(f"  - Saga ID: {saga_id}")
        print(f"  - Final status: {status}")
        print(f"  - Steps executed: {len(step_results)}")
        print("  - Final context:")
        print(f"    * Inventory reservation: {context.inventory_reservation_id}")
        print(f"    * Payment ID: {context.payment_id}")
        print(f"    * Shipment ID: {context.shipment_id}")
        print("\n  - Execution log (SagaLog):")
        for entry in history:
            print(
                f"    [{entry.timestamp.strftime('%H:%M:%S')}] "
                f"{entry.step_name}.{entry.action}: {entry.status.value}",
            )
        print("-" * 70)

    except Exception as e:
        print(f"\n✗ Saga failed: {e}")
        raise


async def run_failing_saga() -> None:
    """Demonstrate saga compensation when a step fails."""
    print("\n" + "=" * 70)
    print("SCENARIO 2: Saga with Compensation (Payment Failure)")
    print("=" * 70)

    # Setup services
    inventory_service = InventoryService()
    payment_service = PaymentService()
    shipping_service = ShippingService()

    # Create saga storage for persistence
    storage = MemorySagaStorage()

    # Setup DI container
    di_container = di.Container()
    # Register services using bind_by_type with lambda functions that return instances
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: inventory_service, scope="request"),
            InventoryService,
        ),
    )
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: payment_service, scope="request"),
            PaymentService,
        ),
    )
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: shipping_service, scope="request"),
            ShippingService,
        ),
    )
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: storage, scope="request"),
            MemorySagaStorage,
        ),
    )

    # Register step handlers
    reserve_step = ReserveInventoryStep(inventory_service)
    payment_step = ProcessPaymentStep(payment_service)
    ship_step = ShipOrderStep(shipping_service)

    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: reserve_step, scope="request"),
            ReserveInventoryStep,
        ),
    )
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: payment_step, scope="request"),
            ProcessPaymentStep,
        ),
    )
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: ship_step, scope="request"),
            ShipOrderStep,
        ),
    )

    # Define saga mapper
    def saga_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    # Create saga mediator using bootstrap
    mediator = bootstrap.bootstrap(
        di_container=di_container,
        sagas_mapper=saga_mapper,
        saga_storage=storage,
    )

    # Create order context with amount that will fail payment
    saga_id = uuid.uuid4()
    context = OrderContext(
        order_id="order_456",
        user_id="user_789",
        items=["item_1"],
        total_amount=15000.0,  # This will exceed payment limit
        shipping_address="456 Oak Ave, City, Country",
    )

    # Execute saga (will fail at payment step)
    print(f"\nProcessing order {context.order_id} (saga_id: {saga_id})...")
    print("  (This will fail at payment step due to amount limit)")

    try:
        async for step_result in mediator.stream(context, saga_id=saga_id):
            step_name = step_result.step_type.__name__
            print(f"\n✓ Step completed: {step_name}")

        print("\n✗ Unexpected: Saga should have failed!")

    except ValueError as e:
        print(f"\n✗ Payment step failed: {e}")

        # Get execution history from storage to show compensation
        history = await storage.get_step_history(saga_id)
        status, _, _ = await storage.load_saga_state(saga_id)

        print("\n" + "-" * 70)
        print("✓ Compensation executed automatically!")
        print(f"  - Saga ID: {saga_id}")
        print(f"  - Final status: {status}")
        print("  - Inventory reservation was released")
        print("  - Compensation happened in reverse order")
        print("\n  - Execution log (SagaLog):")
        for entry in history:
            print(
                f"    [{entry.timestamp.strftime('%H:%M:%S')}] "
                f"{entry.step_name}.{entry.action}: {entry.status.value}",
            )
        print("-" * 70)


async def main() -> None:
    """Run all saga examples."""
    print("\n" + "=" * 70)
    print("SAGA PATTERN EXAMPLE")
    print("=" * 70)
    print("\nThis example demonstrates:")
    print("  1. Successful saga execution with all steps completing")
    print("  2. Automatic compensation when a step fails")
    print("  3. How compensation happens in reverse order")

    # Run successful saga
    await run_successful_saga()

    # Run failing saga with compensation
    await run_failing_saga()

    print("\n" + "=" * 70)
    print("EXAMPLE COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
