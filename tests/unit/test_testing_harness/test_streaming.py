import cqrs
from cqrs.testing import TestContainer, test_harness


class ExportRow(cqrs.Response):
    index: int


class ExportOrders(cqrs.Request[ExportRow]):
    batch: int


class RowExported(cqrs.DomainEvent):
    index: int


class RowExportedHandler(cqrs.EventHandler[RowExported]):
    async def handle(self, event: RowExported) -> None:
        return None


class ExportOrdersHandler(cqrs.StreamingRequestHandler[ExportOrders]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return list(self._events)

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(self, request: ExportOrders):
        yield ExportRow(index=0)
        self._events.append(RowExported(index=0))
        yield ExportRow(index=1)


class FailingExport(cqrs.StreamingRequestHandler[ExportOrders]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return list(self._events)

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(self, request: ExportOrders):
        yield ExportRow(index=0)
        raise RuntimeError("mid")


class ExportOrdersFallback(cqrs.StreamingRequestHandler[ExportOrders]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return list(self._events)

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(self, request: ExportOrders):
        yield ExportRow(index=0)
        yield ExportRow(index=1)


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(RowExported, RowExportedHandler)


async def test_stream_records_yields_and_keeps_events_after_clear() -> None:
    def streaming_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(ExportOrders, ExportOrdersHandler)

    async with test_harness(
        TestContainer(),
        streaming_mapper=streaming_mapper,
        domain_events_mapper=domain_events_mapper,
    ) as harness:
        assert harness.streaming_mediator is not None
        items = []
        async for item in harness.streaming_mediator.stream(ExportOrders(batch=2)):
            items.append(item)
            if item.index == 0:
                assert not harness.handled.contains(RowExported)
            if item.index == 1:
                assert harness.handled.contains(RowExported)
        assert [item.index for item in items] == [0, 1]
        assert items == harness.yields
        assert harness.handled.contains(RowExported)


async def test_streaming_fallback_keeps_values_already_yielded() -> None:
    def streaming_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(
            ExportOrders,
            cqrs.RequestHandlerFallback(FailingExport, ExportOrdersFallback),
        )

    async with test_harness(
        TestContainer(),
        streaming_mapper=streaming_mapper,
    ) as harness:
        assert harness.streaming_mediator is not None
        items = [item async for item in harness.streaming_mediator.stream(ExportOrders(batch=2))]
        harness.invocations.assert_entered(FailingExport)
        harness.invocations.assert_entered(ExportOrdersFallback)
        assert [item.index for item in items] == [0, 0, 1]
        assert [item.index for item in harness.yields] == [0, 0, 1]
