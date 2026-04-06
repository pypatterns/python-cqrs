"""
Example: Chain of Responsibility with Request Handler Fallback

This example shows how to combine a COR (Chain of Responsibility) handler
with RequestHandlerFallback. The primary handler is a RequestHandler that
delegates to a COR chain; when the chain raises (e.g. downstream failure),
the fallback handler is invoked.

Use case: A request is first tried through a chain of handlers (e.g. try
cache, then DB, then external API). If the whole chain fails (e.g. connection
error), a fallback handler returns a default/cached response.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/cor_request_fallback.py

The example will:
- Send a command that is handled by a COR chain (primary path)
- For source="error", the chain raises and fallback handler runs
- For source="a" or "b", the chain handles the request successfully

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. RequestHandlerFallback with COR as primary:
   - Primary is a RequestHandler that delegates to a COR chain (injected via DI).
   - Fallback is a simple RequestHandler used when the chain raises.

2. Building the chain:
   - Create COR handler instances, build_chain(), then bind the chain entry
     (first handler) in the container so the wrapper can receive it.

3. Flow:
   - mediator.send(request) dispatches to primary (CORChainWrapperHandler).
   - Wrapper calls the chain; if the chain raises, dispatcher catches and
     invokes fallback.

4. Optional failure_exceptions:
   - Restrict fallback to specific exception types (e.g. ConnectionError).

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - di (dependency injection)

================================================================================
"""

import asyncio
import logging
import typing

import di
from di import dependent

import cqrs
from cqrs.requests import bootstrap
from cqrs.requests.cor_request_handler import (
    CORRequestHandler,
    build_chain,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HANDLER_SOURCE: list[str] = []  # "chain" or "fallback"


# -----------------------------------------------------------------------------
# Command and response
# -----------------------------------------------------------------------------


class FetchDataCommand(cqrs.Request):
    source: str  # "a" | "b" | "error"


class FetchDataResult(cqrs.Response):
    data: str
    source: str  # "chain" or "fallback"


# -----------------------------------------------------------------------------
# COR handlers (chain)
# -----------------------------------------------------------------------------


class SourceAHandler(CORRequestHandler[FetchDataCommand, FetchDataResult]):
    @property
    def events(self) -> list[cqrs.Event]:
        return []

    async def handle(self, request: FetchDataCommand) -> typing.Optional[FetchDataResult]:
        if request.source == "a":
            logger.info("COR chain: SourceAHandler handled source=a")
            HANDLER_SOURCE.append("chain")
            return FetchDataResult(data="data_from_a", source="chain")
        return await self.next(request)


class SourceBHandler(CORRequestHandler[FetchDataCommand, FetchDataResult]):
    @property
    def events(self) -> list[cqrs.Event]:
        return []

    async def handle(self, request: FetchDataCommand) -> typing.Optional[FetchDataResult]:
        if request.source == "b":
            logger.info("COR chain: SourceBHandler handled source=b")
            HANDLER_SOURCE.append("chain")
            return FetchDataResult(data="data_from_b", source="chain")
        return await self.next(request)


class DefaultChainHandler(CORRequestHandler[FetchDataCommand, FetchDataResult]):
    """Last in chain: handles unknown or raises for source='error'."""

    @property
    def events(self) -> list[cqrs.Event]:
        return []

    async def handle(self, request: FetchDataCommand) -> typing.Optional[FetchDataResult]:
        if request.source == "error":
            logger.info("COR chain: DefaultChainHandler raising ConnectionError for source=error")
            raise ConnectionError("Downstream service unavailable")
        logger.info("COR chain: DefaultChainHandler handled (unknown source)")
        HANDLER_SOURCE.append("chain")
        return FetchDataResult(data="default_data", source="chain")


# -----------------------------------------------------------------------------
# Wrapper: RequestHandler that delegates to the COR chain
# -----------------------------------------------------------------------------


class CORChainWrapperHandler(
    cqrs.RequestHandler[FetchDataCommand, FetchDataResult],
):
    """Primary 'handler' that runs the COR chain; chain is injected as the first link."""

    def __init__(self, chain_entry: SourceAHandler) -> None:
        self._chain_entry = chain_entry

    @property
    def events(self) -> list[cqrs.Event]:
        return []

    async def handle(self, request: FetchDataCommand) -> FetchDataResult:
        result = await self._chain_entry.handle(request)
        if result is None:
            raise ValueError("COR chain did not handle the request")
        return result


# -----------------------------------------------------------------------------
# Fallback handler (used when the chain raises)
# -----------------------------------------------------------------------------


class FallbackFetchDataHandler(
    cqrs.RequestHandler[FetchDataCommand, FetchDataResult],
):
    @property
    def events(self) -> list[cqrs.Event]:
        return []

    async def handle(self, request: FetchDataCommand) -> FetchDataResult:
        logger.info("Fallback handler: returning cached/default for source=%s", request.source)
        HANDLER_SOURCE.append("fallback")
        return FetchDataResult(
            data="cached_or_default",
            source="fallback",
        )


# -----------------------------------------------------------------------------
# Mappers and bootstrap
# -----------------------------------------------------------------------------


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(
        FetchDataCommand,
        cqrs.RequestHandlerFallback(
            primary=CORChainWrapperHandler,
            fallback=FallbackFetchDataHandler,
            failure_exceptions=(ConnectionError, TimeoutError),
        ),
    )


async def main() -> None:
    HANDLER_SOURCE.clear()

    # Build COR chain and inject the chain entry so CORChainWrapperHandler gets it
    source_a = SourceAHandler()
    source_b = SourceBHandler()
    default = DefaultChainHandler()
    build_chain([source_a, source_b, default])

    di_container = di.Container()
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: source_a, scope="request"),
            SourceAHandler,
        ),
    )

    mediator = bootstrap.bootstrap(
        di_container=di_container,
        commands_mapper=commands_mapper,
    )

    print("\n" + "=" * 60)
    print("COR REQUEST HANDLER FALLBACK EXAMPLE")
    print("=" * 60)

    # Case 1: chain handles (source=a)
    print("\n1. Send FetchDataCommand(source='a') — chain handles")
    result1: FetchDataResult = await mediator.send(FetchDataCommand(source="a"))
    print(f"   Result: data={result1.data}, source={result1.source}")
    assert result1.source == "chain" and result1.data == "data_from_a"

    # Case 2: chain handles (source=b)
    print("\n2. Send FetchDataCommand(source='b') — chain handles")
    result2: FetchDataResult = await mediator.send(FetchDataCommand(source="b"))
    print(f"   Result: data={result2.data}, source={result2.source}")
    assert result2.source == "chain" and result2.data == "data_from_b"

    # Case 3: chain raises (source=error) -> fallback runs
    print("\n3. Send FetchDataCommand(source='error') — chain raises, fallback runs")
    result3: FetchDataResult = await mediator.send(FetchDataCommand(source="error"))
    print(f"   Result: data={result3.data}, source={result3.source}")
    assert result3.source == "fallback" and result3.data == "cached_or_default"

    print("\n   Handlers that ran (in order): " + str(HANDLER_SOURCE))
    assert "chain" in HANDLER_SOURCE and "fallback" in HANDLER_SOURCE
    print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
