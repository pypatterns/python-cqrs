"""
Example: Request Handler Fallback with Optional Circuit Breaker

This example demonstrates the RequestHandlerFallback pattern for command/query
handlers. When the primary request handler fails (or the circuit breaker is
open), the fallback handler is invoked. This is useful for resilient reads
or writes when the primary path (e.g. database or external API) is unavailable.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example (without circuit breaker):
   python examples/events/request_fallback.py

With circuit breaker (optional dependency):
   pip install aiobreaker
   python examples/events/request_fallback.py

The example will:
- Send a command that is handled by a primary handler (simulated to fail)
- Fallback handler runs and returns a valid response
- With circuit breaker: after N failures the circuit opens and requests are
  dispatched to fallback without calling the primary handler

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. RequestHandlerFallback Registration:
   - Bind request type to RequestHandlerFallback(primary, fallback, ...)
   - Optional failure_exceptions to trigger fallback only for specific errors
   - Optional circuit_breaker (e.g. AioBreakerAdapter) per domain

2. Primary and Fallback Handlers:
   - Both implement RequestHandler[Request, Response]
   - Primary can raise; fallback provides alternative implementation (e.g. cache)

3. Flow:
   - mediator.send(request) dispatches to primary handler
   - On primary exception (or circuit open): fallback handler is invoked
   - Response and events from the handler that ran are returned

4. Circuit Breaker (optional):
   - Use one AioBreakerAdapter instance per domain (e.g. commands)
   - After fail_max failures, circuit opens; primary is not called, fallback runs

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - di (dependency injection)

Optional for circuit breaker:
   pip install aiobreaker
   or: pip install python-cqrs[aiobreaker]

================================================================================
"""

import asyncio
import logging
import di

import cqrs
from cqrs.adapters.circuit_breaker import AioBreakerAdapter
from cqrs.bootstrap import requests as bootstrap

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HANDLER_USED: list[str] = []


# -----------------------------------------------------------------------------
# Command and response
# -----------------------------------------------------------------------------


class GetUserProfileCommand(cqrs.Request):
    user_id: str


class UserProfileResult(cqrs.Response):
    user_id: str
    name: str
    source: str  # "primary" or "fallback"


# -----------------------------------------------------------------------------
# Primary handler (simulates failure – e.g. database unavailable)
# -----------------------------------------------------------------------------


class PrimaryGetUserProfileHandler(
    cqrs.RequestHandler[GetUserProfileCommand, UserProfileResult],
):
    @property
    def events(self) -> list[cqrs.Event]:
        return []

    async def handle(
        self,
        request: GetUserProfileCommand,
    ) -> UserProfileResult:
        logger.info("Primary handler: fetching profile for user %s", request.user_id)
        HANDLER_USED.append("primary")
        raise ConnectionError("Database unavailable")


# -----------------------------------------------------------------------------
# Fallback handler (e.g. return cached or default data)
# -----------------------------------------------------------------------------


class FallbackGetUserProfileHandler(
    cqrs.RequestHandler[GetUserProfileCommand, UserProfileResult],
):
    @property
    def events(self) -> list[cqrs.Event]:
        return []

    async def handle(
        self,
        request: GetUserProfileCommand,
    ) -> UserProfileResult:
        logger.info(
            "Fallback handler: returning cached/default profile for user %s",
            request.user_id,
        )
        HANDLER_USED.append("fallback")
        return UserProfileResult(
            user_id=request.user_id,
            name="Unknown User",
            source="fallback",
        )


# -----------------------------------------------------------------------------
# Mappers and bootstrap
# -----------------------------------------------------------------------------


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    # Without circuit breaker: fallback on any exception (or restrict with failure_exceptions)
    mapper.bind(
        GetUserProfileCommand,
        cqrs.RequestHandlerFallback(
            primary=PrimaryGetUserProfileHandler,
            fallback=FallbackGetUserProfileHandler,
            failure_exceptions=(ConnectionError, TimeoutError),
        ),
    )


def commands_mapper_with_circuit_breaker(mapper: cqrs.RequestMap) -> None:
    try:
        request_cb = AioBreakerAdapter(fail_max=2, timeout_duration=60)
    except ImportError:
        commands_mapper(mapper)
        return
    mapper.bind(
        GetUserProfileCommand,
        cqrs.RequestHandlerFallback(
            primary=PrimaryGetUserProfileHandler,
            fallback=FallbackGetUserProfileHandler,
            failure_exceptions=(ConnectionError, TimeoutError),
            circuit_breaker=request_cb,
        ),
    )


async def main() -> None:
    HANDLER_USED.clear()

    use_circuit_breaker = False
    try:
        import aiobreaker  # noqa: F401

        use_circuit_breaker = True
    except ImportError:
        pass

    commands_mapper_fn = commands_mapper_with_circuit_breaker if use_circuit_breaker else commands_mapper

    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=commands_mapper_fn,
    )

    print("\n" + "=" * 60)
    print("REQUEST HANDLER FALLBACK EXAMPLE")
    print("=" * 60)
    print("\nSending GetUserProfileCommand (primary will fail)...\n")

    result: UserProfileResult = await mediator.send(
        GetUserProfileCommand(user_id="user_42"),
    )

    print("\nResult:")
    print(f"  Handlers that ran (in order): {HANDLER_USED}")
    print(f"  Response: user_id={result.user_id}, name={result.name}, source={result.source}")
    assert result.source == "fallback"
    assert "primary" in HANDLER_USED and "fallback" in HANDLER_USED
    assert HANDLER_USED[-1] == "fallback"
    print("  ✓ Primary ran and failed; fallback ran and returned response.")
    print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
