"""Tests for Mermaid diagram generator for Chain of Responsibility."""

import typing

import cqrs
from cqrs.requests.cor_request_handler import CORRequestHandler
from cqrs.requests.mermaid import CoRMermaid


class ProcessPaymentCommand(cqrs.Request):
    amount: float
    payment_method: str
    user_id: str


class PaymentResult(cqrs.Response):
    success: bool
    transaction_id: typing.Optional[str] = None
    message: str = ""


class CreditCardHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    @property
    def events(self) -> typing.List:
        return []

    async def handle(self, request: ProcessPaymentCommand) -> typing.Optional[PaymentResult]:
        if request.payment_method == "credit_card":
            return PaymentResult(
                success=True,
                transaction_id="cc_123",
                message="Payment processed via credit card",
            )
        return await self.next(request)


class PayPalHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    @property
    def events(self) -> typing.List:
        return []

    async def handle(self, request: ProcessPaymentCommand) -> typing.Optional[PaymentResult]:
        if request.payment_method == "paypal":
            return PaymentResult(
                success=True,
                transaction_id="pp_123",
                message="Payment processed via PayPal",
            )
        return await self.next(request)


class BankTransferHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    @property
    def events(self) -> typing.List:
        return []

    async def handle(self, request: ProcessPaymentCommand) -> typing.Optional[PaymentResult]:
        if request.payment_method == "bank_transfer":
            return PaymentResult(
                success=True,
                transaction_id="bt_123",
                message="Payment processed via bank transfer",
            )
        return await self.next(request)


class DefaultPaymentHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    @property
    def events(self) -> typing.List:
        return []

    async def handle(self, request: ProcessPaymentCommand) -> typing.Optional[PaymentResult]:
        return PaymentResult(
            success=False,
            message=f"Unsupported payment method: {request.payment_method}",
        )


def test_to_mermaid_empty_handlers() -> None:
    """Test that Mermaid handles empty handlers list correctly."""
    handlers: list[type[CORRequestHandler[typing.Any, typing.Any]]] = []
    generator = CoRMermaid(handlers)

    diagram = generator.sequence()

    assert "sequenceDiagram" in diagram
    assert "participant C as Chain" in diagram
    assert "No handlers configured" in diagram


def test_to_mermaid_single_handler() -> None:
    """Test that Mermaid generates correct diagram for single handler."""
    handlers = [CreditCardHandler]
    generator = CoRMermaid(handlers)

    diagram = generator.sequence()

    # Check basic structure
    assert "sequenceDiagram" in diagram
    assert "participant C as Chain" in diagram
    assert "participant H1 as CreditCardHandler" in diagram

    # Check flow structure
    assert "Chain Execution Flow" in diagram
    assert "C->>H1: handle(request)" in diagram
    assert "alt" in diagram
    assert "CreditCardHandler can handle" in diagram
    assert "H1-->>C: result" in diagram
    assert "Handler processed, chain stops" in diagram


def test_to_mermaid_multiple_handlers() -> None:
    """Test that Mermaid generates correct diagram for multiple handlers."""
    handlers = [
        CreditCardHandler,
        PayPalHandler,
        BankTransferHandler,
        DefaultPaymentHandler,
    ]
    generator = CoRMermaid(handlers)

    diagram = generator.sequence()

    # Check participants
    assert "participant C as Chain" in diagram
    assert "participant H1 as CreditCardHandler" in diagram
    assert "participant H2 as PayPalHandler" in diagram
    assert "participant H3 as BankTransferHandler" in diagram
    assert "participant H4 as DefaultPaymentHandler" in diagram

    # Check flow structure
    assert "Chain Execution Flow" in diagram
    assert "C->>H1: handle(request)" in diagram

    # Check that alt/else blocks are used with handler names
    assert "alt" in diagram
    assert "else" in diagram
    assert "end" in diagram

    # Check that handler names (not aliases) are used in alt conditions
    assert "CreditCardHandler can handle" in diagram
    assert "PayPalHandler can handle" in diagram
    assert "BankTransferHandler can handle" in diagram
    assert "DefaultPaymentHandler can handle" in diagram

    # Check that handlers can return result or pass to next
    assert "H1-->>C: result" in diagram
    assert "H1->>H2: next(request)" in diagram
    assert "H2-->>C: result" in diagram
    assert "H2->>H3: next(request)" in diagram
    assert "H3-->>C: result" in diagram
    assert "H3->>H4: next(request)" in diagram
    assert "H4-->>C: result" in diagram

    # Check chain stopping message
    assert "Handler processed, chain stops" in diagram or "Handler processed" in diagram


def test_class_diagram_empty_handlers() -> None:
    """Test that class_diagram() handles empty handlers list correctly."""
    handlers: list[type[CORRequestHandler[typing.Any, typing.Any]]] = []
    generator = CoRMermaid(handlers)

    diagram = generator.class_diagram()

    assert "classDiagram" in diagram
    assert "class CORRequestHandler" in diagram
    assert "No handlers configured" in diagram


def test_class_diagram_basic_structure() -> None:
    """Test that class_diagram() generates correct basic structure."""
    handlers = [CreditCardHandler, PayPalHandler, BankTransferHandler]
    generator = CoRMermaid(handlers)

    diagram = generator.class_diagram()

    # Check basic structure
    assert "classDiagram" in diagram
    assert "class CORRequestHandler" in diagram
    assert "<<abstract>>" in diagram
    assert "+handle(request) typing.Optional[Response ]" in diagram
    assert "+next(request) typing.Optional[Response ]" in diagram
    assert "+set_next(handler) CORRequestHandler" in diagram

    # Check handler classes
    assert "class CreditCardHandler" in diagram
    assert "class PayPalHandler" in diagram
    assert "class BankTransferHandler" in diagram

    # Check handler methods
    assert "+handle(request) typing.Optional[Response ]" in diagram
    assert "+next(request) typing.Optional[Response ]" in diagram
    assert "+events: List[Event]" in diagram

    # Check types are included
    assert "class ProcessPaymentCommand" in diagram
    assert "class PaymentResult" in diagram


def test_class_diagram_types() -> None:
    """Test that class_diagram() includes request and response types."""
    handlers = [CreditCardHandler, PayPalHandler, BankTransferHandler]
    generator = CoRMermaid(handlers)

    diagram = generator.class_diagram()

    # Check that request and response types are included
    assert "class ProcessPaymentCommand" in diagram
    assert "class PaymentResult" in diagram


def test_class_diagram_relationships() -> None:
    """Test that class_diagram() includes relationships between classes."""
    handlers = [CreditCardHandler, PayPalHandler, BankTransferHandler]
    generator = CoRMermaid(handlers)

    diagram = generator.class_diagram()

    # Check inheritance relationships
    assert "CORRequestHandler <|-- CreditCardHandler" in diagram
    assert "CORRequestHandler <|-- PayPalHandler" in diagram
    assert "CORRequestHandler <|-- BankTransferHandler" in diagram

    # Check chain relationships (set_next)
    assert (
        "CreditCardHandler --> PayPalHandler" in diagram or "CreditCardHandler --> PayPalHandler : set_next" in diagram
    )
    assert (
        "PayPalHandler --> BankTransferHandler" in diagram
        or "PayPalHandler --> BankTransferHandler : set_next" in diagram
    )

    # Check Handler to Request relationships
    assert (
        "CreditCardHandler ..> ProcessPaymentCommand" in diagram
        or "CreditCardHandler ..> ProcessPaymentCommand : uses" in diagram
    )

    # Check Handler to Response relationships
    assert (
        "CreditCardHandler ..> PaymentResult" in diagram or "CreditCardHandler ..> PaymentResult : returns" in diagram
    )
