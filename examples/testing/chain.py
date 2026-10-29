"""
Chain of responsibility: the matching step does not call next.

Run:
   python examples/testing/chain.py
"""

import asyncio

import di

import cqrs
from cqrs.testing import test_harness


class Paid(cqrs.Response):
    method: str


class PayOrder(cqrs.Request[Paid]):
    method: str


class CardHandler(cqrs.handlers.CORRequestHandler[PayOrder]):
    async def handle(self, request: PayOrder):
        if request.method != "card":
            return await self.next(request)
        return Paid(method="card")


class CashHandler(cqrs.handlers.CORRequestHandler[PayOrder]):
    async def handle(self, request: PayOrder):
        return Paid(method="cash")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(PayOrder, [CardHandler, CashHandler])


async def main() -> None:
    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        result = await harness.mediator.send(PayOrder(method="card"))
        assert result.method == "card"
        harness.invocations.assert_entered(CardHandler)
        harness.invocations.assert_not_entered(CashHandler)


if __name__ == "__main__":
    asyncio.run(main())
