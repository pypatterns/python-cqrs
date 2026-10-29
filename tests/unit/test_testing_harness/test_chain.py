import cqrs
from cqrs.testing import TestContainer, test_harness


class Paid(cqrs.Response):
    method: str


class PayOrder(cqrs.Request[Paid]):
    method: str


class CardPaid(cqrs.DomainEvent):
    method: str


class CashPaid(cqrs.DomainEvent):
    method: str


class CardHandler(cqrs.handlers.CORRequestHandler[PayOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: PayOrder) -> Paid | None:
        if request.method != "card":
            return await self.next(request)
        self._events.append(CardPaid(method="card"))
        return Paid(method="card")


class CashHandler(cqrs.handlers.CORRequestHandler[PayOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: PayOrder) -> Paid | None:
        if request.method != "cash":
            return await self.next(request)
        self._events.append(CashPaid(method="cash"))
        return Paid(method="cash")


class CardPaidHandler(cqrs.EventHandler[CardPaid]):
    async def handle(self, event: CardPaid) -> None:
        return None


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(PayOrder, [CardHandler, CashHandler])


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(CardPaid, CardPaidHandler)


async def test_chain_enters_only_the_matching_step() -> None:
    async with test_harness(
        TestContainer(),
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
    ) as harness:
        assert harness.mediator is not None
        result = await harness.mediator.send(PayOrder(method="card"))
        assert result is not None
        assert result.method == "card"
        harness.invocations.assert_entered(CardHandler)
        harness.invocations.assert_not_entered(CashHandler)
        assert harness.handled.contains(CardPaid)
        assert not harness.handled.contains(CashPaid)
