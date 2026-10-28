"""One request, two strategies, first match wins.

Full demo: examples/cor/typed_cor_request_handler.py.
"""

import di

import cqrs
from cqrs.bootstrap import requests as bootstrap
from cqrs.handlers import CORRequestHandler


class PaymentResult(cqrs.Response):
    success: bool
    transaction_id: str | None = None


class ProcessPayment(cqrs.Request[PaymentResult]):
    amount: int
    payment_method: str


class CreditCardHandler(CORRequestHandler[ProcessPayment]):
    async def handle(self, request: ProcessPayment):
        if request.payment_method != "credit_card":
            return await self.next(request)
        return PaymentResult(success=True, transaction_id="cc")


class DefaultPaymentHandler(CORRequestHandler[ProcessPayment]):
    async def handle(self, request: ProcessPayment):
        return PaymentResult(success=False, transaction_id=None)


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ProcessPayment, [CreditCardHandler, DefaultPaymentHandler])


def build_mediator() -> cqrs.RequestMediator:
    return bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=commands_mapper,
    )
