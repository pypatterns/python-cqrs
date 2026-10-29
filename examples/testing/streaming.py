"""
Streaming handler and streaming fallback. clear_events does not erase harness yields.

Run:
   python examples/testing/streaming.py
"""

import asyncio

import di

import cqrs
from cqrs.testing import test_harness


class Row(cqrs.Response):
    index: int


class ExportOrders(cqrs.Request[Row]):
    batch: int


class RowExported(cqrs.DomainEvent):
    index: int


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


async def stream_rows() -> None:
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

    def streaming_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(ExportOrders, ExportOrdersHandler)

    container = di.Container()
    async with test_harness(container, streaming_mapper=streaming_mapper) as harness:
        items = [item async for item in harness.streaming_mediator.stream(ExportOrders(batch=2))]
        assert [item.index for item in items] == [0, 1]
        assert items == harness.yields
        assert harness.handled.contains(RowExported)


async def stream_fallback() -> None:
    class ExportOrdersHandler(cqrs.StreamingRequestHandler[ExportOrders]):
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

    def streaming_mapper(mapper: cqrs.RequestMap) -> None:
        mapper.bind(
            ExportOrders,
            cqrs.RequestHandlerFallback(ExportOrdersHandler, ExportOrdersFallback),
        )

    container = di.Container()
    async with test_harness(container, streaming_mapper=streaming_mapper) as harness:
        items = [item async for item in harness.streaming_mediator.stream(ExportOrders(batch=2))]
        harness.invocations.assert_entered(ExportOrdersHandler)
        harness.invocations.assert_entered(ExportOrdersFallback)
        assert [item.index for item in harness.yields] == [0, 0, 1]
        assert [item.index for item in items] == [0, 0, 1]


async def main() -> None:
    await stream_rows()
    await stream_fallback()


if __name__ == "__main__":
    asyncio.run(main())
