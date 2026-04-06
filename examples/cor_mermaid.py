"""
Example: Generating Mermaid Diagrams for Chain of Responsibility Request Handlers

This example demonstrates how to generate Mermaid diagrams from Chain of Responsibility
handler chains. Mermaid diagrams are useful for documentation and visualization of
handler structure and execution flow.

The example generates two types of diagrams:
1. Sequence Diagram - Shows the execution flow through the chain
2. Class Diagram - Shows the type structure, relationships, and dependencies

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/cor_mermaid.py

The example will:
- Generate a Sequence diagram showing chain execution flow
- Generate a Class diagram showing handler structure and types
- Print both diagrams to console
- Provide instructions for viewing diagrams in online Mermaid editor

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Mermaid Diagram Generation:
   - Use CoRMermaid class to generate diagrams from handler chains
   - Generate Sequence diagrams for execution flow visualization
   - Generate Class diagrams for type structure visualization

2. Diagram Types:
   - Sequence Diagram: Shows request flow through handlers, successful handling,
     and pass-through scenarios
   - Class Diagram: Shows CORRequestHandler base class, handler classes, request types,
     response types, and relationships between them

3. Usage:
   - Diagrams can be copied and pasted into Mermaid Live Editor
   - Useful for documentation and understanding handler chain structure
   - Helps visualize complex handler flows and dependencies

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

import cqrs
from cqrs.events.event import Event
from cqrs.requests.cor_request_handler import CORRequestHandler
from cqrs.requests.mermaid import CoRMermaid


# ============================================================================
# Domain Models
# ============================================================================


class ProcessPaymentCommand(cqrs.Request):
    """Payment processing command."""

    amount: float
    payment_method: str
    user_id: str


class PaymentResult(cqrs.Response):
    """Payment processing result."""

    success: bool
    transaction_id: typing.Optional[str] = None
    message: str = ""


class PaymentProcessedEvent(cqrs.Event, frozen=True):
    """Event emitted when payment is processed."""

    transaction_id: str
    amount: float
    user_id: str


# ============================================================================
# Chain of Responsibility Handlers
# ============================================================================


class CreditCardHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    """Handler for credit card payments."""

    def __init__(self) -> None:
        super().__init__()
        self._events: typing.List[Event] = []

    @property
    def events(self) -> typing.List[Event]:
        return self._events.copy()

    async def handle(self, request: ProcessPaymentCommand) -> typing.Optional[PaymentResult]:
        """Process credit card payment."""
        if request.payment_method == "credit_card":
            transaction_id = f"cc_{request.user_id}_{int(request.amount * 100)}"

            self._events.append(
                PaymentProcessedEvent(
                    transaction_id=transaction_id,
                    amount=request.amount,
                    user_id=request.user_id,
                ),
            )

            return PaymentResult(
                success=True,
                transaction_id=transaction_id,
                message="Payment processed via credit card",
            )

        # Pass to next handler
        return await self.next(request)


class PayPalHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    """Handler for PayPal payments."""

    def __init__(self) -> None:
        super().__init__()
        self._events: typing.List[Event] = []

    @property
    def events(self) -> typing.List[Event]:
        return self._events.copy()

    async def handle(self, request: ProcessPaymentCommand) -> typing.Optional[PaymentResult]:
        """Process PayPal payment."""
        if request.payment_method == "paypal":
            transaction_id = f"pp_{request.user_id}_{int(request.amount * 100)}"

            self._events.append(
                PaymentProcessedEvent(
                    transaction_id=transaction_id,
                    amount=request.amount,
                    user_id=request.user_id,
                ),
            )

            return PaymentResult(
                success=True,
                transaction_id=transaction_id,
                message="Payment processed via PayPal",
            )

        # Pass to next handler
        return await self.next(request)


class BankTransferHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    """Handler for bank transfer payments."""

    def __init__(self) -> None:
        super().__init__()
        self._events: typing.List[Event] = []

    @property
    def events(self) -> typing.List[Event]:
        return self._events.copy()

    async def handle(self, request: ProcessPaymentCommand) -> typing.Optional[PaymentResult]:
        """Process bank transfer payment."""
        if request.payment_method == "bank_transfer":
            transaction_id = f"bt_{request.user_id}_{int(request.amount * 100)}"

            self._events.append(
                PaymentProcessedEvent(
                    transaction_id=transaction_id,
                    amount=request.amount,
                    user_id=request.user_id,
                ),
            )

            return PaymentResult(
                success=True,
                transaction_id=transaction_id,
                message="Payment processed via bank transfer",
            )

        # Pass to next handler
        return await self.next(request)


class DefaultPaymentHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    """Default handler for unsupported payment methods."""

    @property
    def events(self) -> typing.List[Event]:
        return []

    async def handle(self, request: ProcessPaymentCommand) -> typing.Optional[PaymentResult]:
        """Handle unsupported payment methods."""
        return PaymentResult(
            success=False,
            message=f"Unsupported payment method: {request.payment_method}",
        )


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
    """Generate and display Mermaid diagrams for chain of responsibility handlers."""
    print("\n" + "=" * 80)
    print("CHAIN OF RESPONSIBILITY MERMAID DIAGRAM GENERATION EXAMPLE")
    print("=" * 80)
    print("\nThis example demonstrates how to generate Mermaid diagrams")
    print("from Chain of Responsibility handler chains for documentation")
    print("and visualization purposes.")

    # Create handler chain
    handlers = [
        CreditCardHandler,
        PayPalHandler,
        BankTransferHandler,
        DefaultPaymentHandler,
    ]

    # Create Mermaid generator
    generator = CoRMermaid(handlers)

    # Generate Sequence Diagram
    print("\n📊 Generating Sequence Diagram...")
    sequence_diagram = generator.sequence()
    print_diagram(
        "SEQUENCE DIAGRAM - Chain Execution Flow",
        sequence_diagram,
        "Sequence",
    )

    # Generate Class Diagram
    print("\n📊 Generating Class Diagram...")
    class_diagram = generator.class_diagram()
    print_diagram(
        "CLASS DIAGRAM - Handler Type Structure",
        class_diagram,
        "Class",
    )

    print("\n" + "=" * 80)
    print("✅ EXAMPLE COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print("\nBoth diagrams have been generated and are ready to use!")
    print("\nQuick start:")
    print("  • Copy the code blocks above (they include ```mermaid markers)")
    print("  • Paste into https://mermaid.live/ to view rendered diagrams")
    print("  • Or use directly in markdown files with Mermaid support")
    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    main()
