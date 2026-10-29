"""
Streaming handler and streaming fallback. clear_events does not erase harness yields.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_streaming.py
"""

import di

import cqrs
from cqrs.testing import test_harness


class Row(cqrs.Response):
    index: int


class ExportOrders(cqrs.Request[Row]):
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
        for index in range(request.batch):
            self._events.append(RowExported(index=index))
            yield Row(index=index)


class FailingExportOrdersHandler(cqrs.StreamingRequestHandler[ExportOrders]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return list(self._events)

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(self, request: ExportOrders):
        yield Row(index=0)
        raise RuntimeError("stream failed")


class ExportOrdersFallback(cqrs.StreamingRequestHandler[ExportOrders]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return list(self._events)

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(self, request: ExportOrders):
        for index in range(request.batch):
            yield Row(index=index)


def streaming_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ExportOrders, ExportOrdersHandler)


def fallback_streaming_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(
        ExportOrders,
        cqrs.RequestHandlerFallback(FailingExportOrdersHandler, ExportOrdersFallback),
    )


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(RowExported, RowExportedHandler)


async def test_stream_records_yields_and_events() -> None:
    container = di.Container()
    async with test_harness(
        container,
        streaming_mapper=streaming_mapper,
        domain_events_mapper=domain_events_mapper,
    ) as harness:
        items = [item async for item in harness.streaming_mediator.stream(ExportOrders(batch=2))]

        assert [item.index for item in items] == [0, 1]
        assert items == harness.yields
        assert harness.handled.contains(RowExported)


async def test_stream_fallback_restarts_the_stream() -> None:
    container = di.Container()
    async with test_harness(container, streaming_mapper=fallback_streaming_mapper) as harness:
        items = [item async for item in harness.streaming_mediator.stream(ExportOrders(batch=2))]

        harness.invocations.assert_entered(FailingExportOrdersHandler)
        harness.invocations.assert_entered(ExportOrdersFallback)
        assert [item.index for item in harness.yields] == [0, 0, 1]
        assert [item.index for item in items] == [0, 0, 1]
