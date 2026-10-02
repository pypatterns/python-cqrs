"""Benchmarks for StreamingRequestHandler (default Request/Response)."""

import asyncio
import typing

import cqrs
import di
import pytest
from cqrs.models.event import IEvent
from cqrs.bootstrap import requests as bootstrap
from cqrs.handlers.request import StreamingRequestHandler


class ProcessItemsCommand(cqrs.Request):
    item_ids: list[str]


class ProcessItemResult(cqrs.Response):
    item_id: str
    status: str


class StreamingHandler(StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult]):
    def __init__(self) -> None:
        self._events: list[IEvent] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(
        self,
        request: ProcessItemsCommand,
    ) -> typing.AsyncIterator[ProcessItemResult]:
        for item_id in request.item_ids:
            self._events.append(
                cqrs.NotificationEvent(
                    event_name="ItemProcessed",
                    payload={"item_id": item_id},
                ),
            )
            yield ProcessItemResult(item_id=item_id, status="processed")


def streaming_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ProcessItemsCommand, StreamingHandler)


@pytest.fixture
def streaming_mediator():
    return bootstrap.bootstrap_streaming(
        di_container=di.Container(),
        commands_mapper=streaming_mapper,
    )


@pytest.mark.benchmark
def test_benchmark_stream_single_item(streaming_mediator, benchmark):
    """Benchmark streaming handler with single item."""

    async def run():
        request = ProcessItemsCommand(item_ids=["item_1"])
        results = []
        async for result in streaming_mediator.stream(request):
            results.append(result)
        return results

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_stream_ten_items(streaming_mediator, benchmark):
    """Benchmark streaming handler with 10 items."""

    async def run():
        request = ProcessItemsCommand(
            item_ids=[f"item_{i}" for i in range(10)],
        )
        results = []
        async for result in streaming_mediator.stream(request):
            results.append(result)
        return results

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_stream_hundred_items(streaming_mediator, benchmark):
    """Benchmark streaming handler with 100 items."""

    async def run():
        request = ProcessItemsCommand(
            item_ids=[f"item_{i}" for i in range(100)],
        )
        results = []
        async for result in streaming_mediator.stream(request):
            results.append(result)
        return results

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_stream_ten_requests_five_items_each(streaming_mediator, benchmark):
    """Benchmark 10 streaming requests with 5 items each."""

    async def run():
        for i in range(10):
            request = ProcessItemsCommand(
                item_ids=[f"item_{i}_{j}" for j in range(5)],
            )
            async for _ in streaming_mediator.stream(request):
                pass

    benchmark(lambda: asyncio.run(run()))
