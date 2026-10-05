"""
Example: Streaming Request Handler Fallback

This example demonstrates RequestHandlerFallback with StreamingRequestHandler.
When the primary streaming handler fails (e.g. raises after yielding some items),
the fallback streaming handler is used and its stream is consumed.

Use case: Stream results from a primary source (e.g. live API); if the stream
fails mid-way, switch to a fallback stream (e.g. cached or degraded results).

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/streaming/streaming_handler_fallback.py

The example will:
- Start streaming from the primary handler (yields a few items then raises)
- After the exception, the fallback streaming handler runs and yields items
- Collect and print all results from both handlers

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. RequestHandlerFallback with streaming handlers:
   - primary and fallback are both StreamingRequestHandler (async generators).
   - Dispatcher runs primary.handle(request); if it raises, runs fallback.handle(request).

2. Flow:
   - mediator.stream(request) yields results from the primary handler.
   - When the primary raises, the dispatcher catches and continues with the
     fallback handler's stream.

3. Optional failure_exceptions and circuit_breaker:
   - Same as for non-streaming RequestHandlerFallback.

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - di (dependency injection)

================================================================================
"""

from collections.abc import AsyncIterator

import asyncio
import logging
import typing

import di

import cqrs
from cqrs.bootstrap import requests as bootstrap

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STREAM_SOURCE: list[str] = []  # "primary" or "fallback" per yield


# -----------------------------------------------------------------------------
# Request and response (streaming)
# -----------------------------------------------------------------------------


class StreamItemsCommand(cqrs.Request):
    item_ids: list[str]


class StreamItemResult(cqrs.Response):
    item_id: str
    status: str
    source: str  # "primary" or "fallback"


# -----------------------------------------------------------------------------
# Primary streaming handler (yields twice then raises)
# -----------------------------------------------------------------------------


class PrimaryStreamItemsHandler(
    cqrs.StreamingRequestHandler[StreamItemsCommand, StreamItemResult],
):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(
        self,
        request: StreamItemsCommand,
    ) -> AsyncIterator[StreamItemResult]:
        for i, item_id in enumerate(request.item_ids):
            if i >= 2:
                logger.info("Primary streaming handler raising after 2 items")
                raise ConnectionError("Stream connection lost")
            STREAM_SOURCE.append("primary")
            yield StreamItemResult(
                item_id=item_id,
                status="processed",
                source="primary",
            )


# -----------------------------------------------------------------------------
# Fallback streaming handler (yields all items)
# -----------------------------------------------------------------------------


class FallbackStreamItemsHandler(
    cqrs.StreamingRequestHandler[StreamItemsCommand, StreamItemResult],
):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(
        self,
        request: StreamItemsCommand,
    ) -> AsyncIterator[StreamItemResult]:
        for item_id in request.item_ids:
            STREAM_SOURCE.append("fallback")
            yield StreamItemResult(
                item_id=item_id,
                status="from_fallback",
                source="fallback",
            )


# -----------------------------------------------------------------------------
# Mapper and bootstrap
# -----------------------------------------------------------------------------


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(
        StreamItemsCommand,
        cqrs.RequestHandlerFallback(
            primary=PrimaryStreamItemsHandler,
            fallback=FallbackStreamItemsHandler,
            failure_exceptions=(ConnectionError, TimeoutError),
        ),
    )


async def main() -> None:
    STREAM_SOURCE.clear()

    mediator = bootstrap.bootstrap_streaming(
        di_container=di.Container(),
        commands_mapper=commands_mapper,
    )

    print("\n" + "=" * 60)
    print("STREAMING HANDLER FALLBACK EXAMPLE")
    print("=" * 60)
    print("\nStreaming items (primary will fail after 2 items, then fallback runs)...\n")

    request = StreamItemsCommand(item_ids=["id1", "id2", "id3", "id4"])
    results: list[StreamItemResult] = []
    async for response in mediator.stream(request):
        if response is not None:
            r = typing.cast(StreamItemResult, response)
            results.append(r)
            print(f"  Yield: item_id={r.item_id}, status={r.status}, source={r.source}")

    print("\n   Handlers that yielded (in order): " + str(STREAM_SOURCE))
    assert "primary" in STREAM_SOURCE and "fallback" in STREAM_SOURCE
    assert results[0].source == "primary" and results[1].source == "primary"
    assert any(r.source == "fallback" for r in results)
    print("\n  ✓ Primary stream failed; fallback stream completed.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
