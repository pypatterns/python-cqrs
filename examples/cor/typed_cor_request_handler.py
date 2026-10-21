"""
Example: typed chain-of-responsibility handler (one type argument).

The command is ``Request[PaymentResult]``. Each step is
``CORRequestHandler[ProcessPaymentCommand]`` and does not annotate ``handle``.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

   python examples/cor/typed_cor_request_handler.py

================================================================================
REQUIREMENTS
================================================================================

   - cqrs (this package)
   - di (dependency injection)

No Docker and no pydantic extra.
================================================================================
"""

import asyncio

import di

import cqrs
from cqrs.bootstrap import requests as bootstrap
from cqrs.handlers.cor import CORRequestHandler


class PaymentResult(cqrs.Response):
    success: bool
    transaction_id: str | None = None


class ProcessPaymentCommand(cqrs.Request[PaymentResult]):
    amount: int
    payment_method: str


class CreditCardHandler(CORRequestHandler[ProcessPaymentCommand]):
    async def handle(self, request: ProcessPaymentCommand):
        if request.payment_method == "credit_card":
            print("credit_card", request.amount)
            return PaymentResult(success=True, transaction_id=f"cc_{request.amount}")
        return await self.next(request)


class DefaultPaymentHandler(CORRequestHandler[ProcessPaymentCommand]):
    async def handle(self, request: ProcessPaymentCommand):
        print("default", request.payment_method)
        return PaymentResult(success=False, transaction_id=None)


def payment_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ProcessPaymentCommand, [CreditCardHandler, DefaultPaymentHandler])


async def main() -> None:
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=payment_mapper,
    )
    paid = await mediator.send(
        ProcessPaymentCommand(amount=10, payment_method="credit_card"),
    )
    assert paid.success
    assert paid.transaction_id == "cc_10"
    rejected = await mediator.send(
        ProcessPaymentCommand(amount=5, payment_method="cash"),
    )
    assert rejected.success is False
    assert rejected.transaction_id is None
    print("chain ok")


if __name__ == "__main__":
    asyncio.run(main())
