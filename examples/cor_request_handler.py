"""
Example: Chain of Responsibility Request Handlers

This example demonstrates the chain of responsibility pattern with CQRS.
Multiple handlers are linked together to process a request until one
successfully handles it or all handlers are exhausted.

Use case: Payment processing system where different handlers process
different payment methods in order of preference (credit card -> PayPal -> bank transfer).
If none can handle the request, a default handler returns an error.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/cor_request_handler.py

The example will:
- Process multiple payment requests with different payment methods
- Chain handlers will process requests in order until one handles it
- Display successful payments with transaction IDs
- Show fallback handling for unsupported payment methods
- Generate events for successful payments

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Chain of Responsibility Pattern:
   - Multiple handlers linked together using set_next()
   - Each handler either processes the request or passes it to the next handler
   - build_chain() function creates the chain from a list of handlers

2. CORRequestHandler Usage:
   - Define handlers as CORRequestHandler subclasses
   - Implement handle() method with chain logic
   - Pass requests down the chain using super().handle()

3. Handler Registration:
   - Register chain of handlers using mapper functions
   - Map request types to handler chains instead of single handlers
   - Use COR handlers in the same mediator as regular handlers

4. Event Generation:
   - Handlers can generate events when processing requests
   - Events are collected and can be published by the mediator

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - di (dependency injection)

================================================================================
"""

import asyncio
import logging
import typing
from collections import defaultdict

import di

import cqrs
from cqrs.requests import bootstrap
from cqrs.requests.cor_request_handler import CORRequestHandler

logging.basicConfig(level=logging.DEBUG)

# Simple in-memory storage for demo
TRANSACTIONS = defaultdict[str, typing.List[str]](lambda: [])


class ProcessPaymentCommand(cqrs.Request):
    amount: float
    payment_method: str
    user_id: str


class PaymentResult(cqrs.Response):
    success: bool
    transaction_id: typing.Optional[str] = None
    message: str = ""


class PaymentProcessedEvent(cqrs.Event, frozen=True):
    transaction_id: str
    amount: float
    user_id: str


class CreditCardHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    @property
    def events(self) -> typing.List[cqrs.Event]:
        return self._events

    def __init__(self) -> None:
        super().__init__()
        self._events: typing.List[cqrs.Event] = []

    async def handle(self, request: ProcessPaymentCommand) -> typing.Optional[PaymentResult]:
        if request.payment_method == "credit_card":
            transaction_id = f"cc_{request.user_id}_{int(request.amount * 100)}"
            TRANSACTIONS["credit_card"].append(transaction_id)

            self._events.append(
                PaymentProcessedEvent(
                    transaction_id=transaction_id,
                    amount=request.amount,
                    user_id=request.user_id,
                ),
            )

            print(f"CreditCard: Processed ${request.amount} for user {request.user_id}")
            return PaymentResult(
                success=True,
                transaction_id=transaction_id,
                message="Payment processed via credit card",
            )

        # Pass to next handler
        return await self.next(request)


class PayPalHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    @property
    def events(self) -> typing.List[cqrs.Event]:
        return self._events

    def __init__(self) -> None:
        super().__init__()
        self._events: typing.List[cqrs.Event] = []

    async def handle(self, request: ProcessPaymentCommand) -> typing.Optional[PaymentResult]:
        if request.payment_method == "paypal":
            transaction_id = f"pp_{request.user_id}_{int(request.amount * 100)}"
            TRANSACTIONS["paypal"].append(transaction_id)

            self._events.append(
                PaymentProcessedEvent(
                    transaction_id=transaction_id,
                    amount=request.amount,
                    user_id=request.user_id,
                ),
            )

            print(f"PayPal: Processed ${request.amount} for user {request.user_id}")
            return PaymentResult(
                success=True,
                transaction_id=transaction_id,
                message="Payment processed via PayPal",
            )

        # Pass to next handler
        return await self.next(request)


class BankTransferHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    @property
    def events(self) -> typing.List[cqrs.Event]:
        return self._events

    def __init__(self) -> None:
        super().__init__()
        self._events: typing.List[cqrs.Event] = []

    async def handle(self, request: ProcessPaymentCommand) -> typing.Optional[PaymentResult]:
        if request.payment_method == "bank_transfer":
            transaction_id = f"bt_{request.user_id}_{int(request.amount * 100)}"
            TRANSACTIONS["bank_transfer"].append(transaction_id)

            self._events.append(
                PaymentProcessedEvent(
                    transaction_id=transaction_id,
                    amount=request.amount,
                    user_id=request.user_id,
                ),
            )

            print(
                f"BankTransfer: Processed ${request.amount} for user {request.user_id}",
            )
            return PaymentResult(
                success=True,
                transaction_id=transaction_id,
                message="Payment processed via bank transfer",
            )

        # Pass to next handler
        return await self.next(request)


class DefaultPaymentHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    @property
    def events(self) -> typing.List[cqrs.Event]:
        return []

    async def handle(self, request: ProcessPaymentCommand) -> typing.Optional[PaymentResult]:
        # Default handler always handles the request (end of chain)
        print(
            f"Default: Unsupported payment method '{request.payment_method}' for user {request.user_id}",
        )
        return PaymentResult(
            success=False,
            message=f"Unsupported payment method: {request.payment_method}",
        )


def payment_mapper(mapper: cqrs.RequestMap) -> None:
    """Register the chain of payment handlers."""

    mapper.bind(
        ProcessPaymentCommand,
        [
            CreditCardHandler,
            PayPalHandler,
            BankTransferHandler,
            DefaultPaymentHandler,
        ],
    )


async def main():
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=payment_mapper,
    )

    # Test different payment methods
    print("=== Testing Chain of Responsibility Payment Processing ===\n")

    # Test supported payment methods
    print("Processing credit card payment...")
    result1: PaymentResult = await mediator.send(
        ProcessPaymentCommand(
            amount=100.0,
            payment_method="credit_card",
            user_id="user1",
        ),
    )
    print(f"Result: {result1.message}\n")

    print("Processing PayPal payment...")
    result2: PaymentResult = await mediator.send(
        ProcessPaymentCommand(
            amount=50.0,
            payment_method="paypal",
            user_id="user2",
        ),
    )
    print(f"Result: {result2.message}\n")

    print("Processing bank transfer payment...")
    result3: PaymentResult = await mediator.send(
        ProcessPaymentCommand(
            amount=200.0,
            payment_method="bank_transfer",
            user_id="user3",
        ),
    )
    print(f"Result: {result3.message}\n")

    # Test unsupported payment method (goes to default handler)
    print("Processing unsupported payment method...")
    result4: PaymentResult = await mediator.send(
        ProcessPaymentCommand(
            amount=75.0,
            payment_method="crypto",
            user_id="user4",
        ),
    )
    print(f"Result: {result4.message}\n")

    print("=== Summary ===")
    print(f"Total credit card transactions: {len(TRANSACTIONS['credit_card'])}")
    print(f"Total PayPal transactions: {len(TRANSACTIONS['paypal'])}")
    print(f"Total bank transfer transactions: {len(TRANSACTIONS['bank_transfer'])}")

    # Verify results
    assert result1.success
    assert result1.transaction_id is not None
    assert result1.transaction_id.startswith("cc_")
    assert result2.success
    assert result2.transaction_id is not None
    assert result2.transaction_id.startswith("pp_")
    assert result3.success
    assert result3.transaction_id is not None
    assert result3.transaction_id.startswith("bt_")
    assert not result4.success
    assert result4.transaction_id is None

    print("All handlers processed requests correctly!")


if __name__ == "__main__":
    asyncio.run(main())
