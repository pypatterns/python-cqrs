"""
Example: Generating Mermaid Diagrams for Saga

This example demonstrates how to generate Mermaid diagrams from Saga instances.
Mermaid diagrams are useful for documentation and visualization of saga structure
and execution flow.

The example generates two types of diagrams:
1. Sequence Diagram - Shows the execution flow and compensation logic
2. Class Diagram - Shows the type structure, relationships, and dependencies

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/saga_mermaid.py

The example will:
- Generate a Sequence diagram showing saga execution flow
- Generate a Class diagram showing saga structure and types
- Print both diagrams to console
- Provide instructions for viewing diagrams in online Mermaid editor

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Mermaid Diagram Generation:
   - Use SagaMermaid class to generate diagrams from Saga instances
   - Generate Sequence diagrams for execution flow visualization
   - Generate Class diagrams for type structure visualization

2. Diagram Types:
   - Sequence Diagram: Shows step execution order, success/failure flows,
     and compensation in reverse order
   - Class Diagram: Shows Saga class, step handlers, context types,
     response types, and relationships between them

3. Usage:
   - Diagrams can be copied and pasted into Mermaid Live Editor
   - Useful for documentation and understanding saga structure
   - Helps visualize complex saga flows and dependencies

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)

Online Mermaid Editor:
   - https://mermaid.live/ (recommended)
   - https://mermaid-js.github.io/mermaid-live-editor/

================================================================================
"""

import typing

from cqrs import container as cqrs_container
from cqrs.events.event import Event
from cqrs.response import Response
from cqrs.saga.fallback import Fallback
from cqrs.saga.mermaid import SagaMermaid
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult

# Import types and step handlers from saga.py example
# In a real scenario, these would be imported from your domain modules
import dataclasses


# ============================================================================
# Domain Models (same as in saga.py)
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
# Saga Step Handlers (simplified versions for diagram generation)
# ============================================================================


class ReserveInventoryStep(
    SagaStepHandler[OrderContext, ReserveInventoryResponse],
):
    """Step 1: Reserve inventory items for the order."""

    def __init__(self) -> None:
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        """Reserve inventory items."""
        response = ReserveInventoryResponse(
            reservation_id="reservation_123",
            items_reserved=context.items,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        """Release reserved items back to inventory."""
        pass


class ProcessPaymentStep(
    SagaStepHandler[OrderContext, ProcessPaymentResponse],
):
    """Step 2: Process payment for the order."""

    def __init__(self) -> None:
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ProcessPaymentResponse]:
        """Process payment."""
        response = ProcessPaymentResponse(
            payment_id="payment_123",
            amount_charged=context.total_amount,
            transaction_id="txn_123",
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        """Refund the payment."""
        pass


class ShipOrderStep(SagaStepHandler[OrderContext, ShipOrderResponse]):
    """Step 3: Create shipment for the order."""

    def __init__(self) -> None:
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ShipOrderResponse]:
        """Create shipment."""
        response = ShipOrderResponse(
            shipment_id="shipment_123",
            tracking_number="TRACK123456",
            estimated_delivery="2024-12-25",
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        """Cancel the shipment."""
        pass


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
# Fallback Example: Saga with Fallback Step
# ============================================================================


class ReserveInventoryFallbackStep(
    SagaStepHandler[OrderContext, ReserveInventoryResponse],
):
    """Fallback step: Reserve inventory using alternative service."""

    def __init__(self) -> None:
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        """Reserve inventory using fallback service."""
        response = ReserveInventoryResponse(
            reservation_id=f"fallback_reservation_{context.order_id}",
            items_reserved=context.items,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        """Release reserved items from fallback service."""
        pass


class OrderSagaWithFallback(Saga[OrderContext]):
    """Order processing saga with Fallback step for inventory reservation."""

    steps = [
        Fallback(
            step=ReserveInventoryStep,
            fallback=ReserveInventoryFallbackStep,
        ),
        ProcessPaymentStep,
        ShipOrderStep,
    ]


# ============================================================================
# Simple Container (for example purposes)
# ============================================================================


class SimpleContainer(cqrs_container.Container[typing.Any]):
    """Simple container for resolving step handlers."""

    def __init__(self) -> None:
        self._services: dict[type, typing.Any] = {}
        self._external_container = None

    @property
    def external_container(self) -> typing.Any:
        return self._external_container

    def attach_external_container(self, container: typing.Any) -> None:
        self._external_container = container

    async def resolve(self, type_: type) -> typing.Any:
        if type_ == ReserveInventoryStep:
            return ReserveInventoryStep()
        if type_ == ProcessPaymentStep:
            return ProcessPaymentStep()
        if type_ == ShipOrderStep:
            return ShipOrderStep()
        raise ValueError(f"Unknown type: {type_}")


# ============================================================================
# Main Example
# ============================================================================


def print_diagram(title: str, diagram: str, diagram_type: str) -> None:
    """Print diagram with formatting and instructions."""
    print("\n" + "=" * 80)
    print(f"{title}")
    print("=" * 80)
    print(f"\n📋 {diagram_type} Diagram Code:")
    print("\n" + "─" * 80)
    print("```mermaid")
    print(diagram)
    print("```")
    print("─" * 80)
    print("\n📝 Instructions:")
    print("   1. Copy the code block above (including ```mermaid markers)")
    print("   2. Go to https://mermaid.live/")
    print("   3. Paste the code into the editor")
    print("   4. View the rendered diagram")
    print("\n💡 Alternative: You can also use this diagram in:")
    print("   • GitHub/GitLab markdown files (with Mermaid support)")
    print("   • Documentation tools (Confluence, Notion, etc.)")
    print("   • README.md files")
    print("=" * 80)


def main() -> None:
    """Generate and display Mermaid diagrams for saga."""
    print("\n" + "=" * 80)
    print("SAGA MERMAID DIAGRAM GENERATION EXAMPLE")
    print("=" * 80)
    print("\nThis example demonstrates how to generate Mermaid diagrams")
    print("from Saga instances for documentation and visualization purposes.")

    # Example 1: Regular Saga
    print("\n" + "=" * 80)
    print("EXAMPLE 1: Regular Saga (without Fallback)")
    print("=" * 80)

    saga = OrderSaga()
    generator = SagaMermaid(saga)

    # Generate Sequence Diagram
    print("\n📊 Generating Sequence Diagram...")
    sequence_diagram = generator.sequence()
    print_diagram(
        "SEQUENCE DIAGRAM - Saga Execution Flow",
        sequence_diagram,
        "Sequence",
    )

    # Generate Class Diagram
    print("\n📊 Generating Class Diagram...")
    class_diagram = generator.class_diagram()
    print_diagram("CLASS DIAGRAM - Saga Type Structure", class_diagram, "Class")

    # Example 2: Saga with Fallback
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Saga with Fallback Step")
    print("=" * 80)
    print("\nThis example shows how Fallback steps are visualized in Mermaid diagrams.")
    print("The Fallback wrapper includes both primary and fallback step handlers.")

    saga_with_fallback = OrderSagaWithFallback()
    generator_fallback = SagaMermaid(saga_with_fallback)

    # Generate Sequence Diagram with Fallback
    print("\n📊 Generating Sequence Diagram with Fallback...")
    sequence_diagram_fallback = generator_fallback.sequence()
    print_diagram(
        "SEQUENCE DIAGRAM - Saga with Fallback Execution Flow",
        sequence_diagram_fallback,
        "Sequence",
    )

    # Generate Class Diagram with Fallback
    print("\n📊 Generating Class Diagram with Fallback...")
    class_diagram_fallback = generator_fallback.class_diagram()
    print_diagram(
        "CLASS DIAGRAM - Saga with Fallback Type Structure",
        class_diagram_fallback,
        "Class",
    )

    print("\n" + "=" * 80)
    print("✅ EXAMPLE COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print("\nAll diagrams have been generated and are ready to use!")
    print("\nQuick start:")
    print("  • Copy the code blocks above (they include ```mermaid markers)")
    print("  • Paste into https://mermaid.live/ to view rendered diagrams")
    print("  • Or use directly in markdown files with Mermaid support")
    print("\n💡 Note: Fallback steps show both primary and fallback handlers")
    print("   in the diagrams, demonstrating the fallback execution flow.")
    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    main()
