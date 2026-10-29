"""
Chain of responsibility: the matching step does not call next.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_chain.py
"""

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


async def test_matching_step_does_not_call_next() -> None:
    # Arrange
    container = di.Container()
    async with test_harness(container, commands_mapper=commands_mapper) as harness:
        # Act
        result = await harness.mediator.send(PayOrder(method="card"))

        # Assert
        assert result.method == "card"
        harness.invocations.assert_entered(CardHandler)
        harness.invocations.assert_not_entered(CashHandler)
