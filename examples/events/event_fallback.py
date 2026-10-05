"""
Example: Event Handler Fallback with Optional Circuit Breaker

This example demonstrates the EventHandlerFallback pattern for domain event
handlers. When the primary event handler fails (or the circuit breaker is open),
the fallback handler is invoked. This is useful for resilient side effects
such as sending notifications or updating read models when the primary path
(e.g. external API) is unavailable.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example (without circuit breaker):
   python examples/events/event_fallback.py

With circuit breaker (optional dependency):
   pip install aiobreaker
   python examples/events/event_fallback.py

The example will:
- Execute a command that emits a domain event
- Primary event handler fails (simulated external service failure)
- Fallback event handler runs and completes successfully
- With circuit breaker: after N failures the circuit opens and fallback is
  used without calling the primary handler

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. EventHandlerFallback Registration:
   - Bind event type to EventHandlerFallback(primary, fallback, ...)
   - Optional failure_exceptions to trigger fallback only for specific errors
   - Optional circuit_breaker (e.g. AioBreakerAdapter) per domain

2. Primary and Fallback Handlers:
   - Primary handler implements EventHandler[EventType]; can raise
   - Fallback handler implements same event type; runs when primary fails

3. Flow:
   - Command handler emits domain event
   - EventEmitter runs handlers; for EventHandlerFallback runs primary first
   - On primary exception (or circuit open): fallback handler is invoked
   - Events from the handler that actually ran are collected and returned

4. Circuit Breaker (optional):
   - Use one AioBreakerAdapter instance per domain (e.g. events)
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

# Track which handler ran for demo output
EVENTS_HANDLED_BY: list[str] = []


# -----------------------------------------------------------------------------
# Command and domain event
# -----------------------------------------------------------------------------


class SendNotificationCommand(cqrs.Request):
    user_id: str
    message: str


class NotificationSent(cqrs.DomainEvent, frozen=True):
    user_id: str
    message: str


# -----------------------------------------------------------------------------
# Command handler (emits domain event)
# -----------------------------------------------------------------------------


class SendNotificationCommandHandler(cqrs.RequestHandler[SendNotificationCommand, None]):
    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    async def handle(self, request: SendNotificationCommand) -> None:
        self._events.append(
            NotificationSent(user_id=request.user_id, message=request.message),
        )
        logger.info("Command: emitted NotificationSent for user %s", request.user_id)


# -----------------------------------------------------------------------------
# Primary event handler (simulates failure – e.g. external notification API down)
# -----------------------------------------------------------------------------


class PrimaryNotificationSentHandler(cqrs.EventHandler[NotificationSent]):
    async def handle(self, event: NotificationSent) -> None:
        logger.info(
            "Primary handler: would send notification to user %s: %s",
            event.user_id,
            event.message,
        )
        EVENTS_HANDLED_BY.append("primary")
        raise RuntimeError("External notification service unavailable")


# -----------------------------------------------------------------------------
# Fallback event handler (e.g. write to local queue or log)
# -----------------------------------------------------------------------------


class FallbackNotificationSentHandler(cqrs.EventHandler[NotificationSent]):
    async def handle(self, event: NotificationSent) -> None:
        logger.info(
            "Fallback handler: enqueue notification for user %s (primary failed): %s",
            event.user_id,
            event.message,
        )
        EVENTS_HANDLED_BY.append("fallback")


# -----------------------------------------------------------------------------
# Mappers and bootstrap
# -----------------------------------------------------------------------------


def command_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(SendNotificationCommand, SendNotificationCommandHandler)


def events_mapper(mapper: cqrs.EventMap) -> None:
    # Without circuit breaker: any exception from primary triggers fallback
    mapper.bind(
        NotificationSent,
        cqrs.EventHandlerFallback(
            primary=PrimaryNotificationSentHandler,
            fallback=FallbackNotificationSentHandler,
        ),
    )


def events_mapper_with_circuit_breaker(mapper: cqrs.EventMap) -> None:
    try:
        event_cb = AioBreakerAdapter(fail_max=2, timeout_duration=60)
    except ImportError:
        # No aiobreaker: use same as without circuit breaker
        events_mapper(mapper)
        return
    mapper.bind(
        NotificationSent,
        cqrs.EventHandlerFallback(
            primary=PrimaryNotificationSentHandler,
            fallback=FallbackNotificationSentHandler,
            circuit_breaker=event_cb,
        ),
    )


async def main() -> None:
    EVENTS_HANDLED_BY.clear()

    use_circuit_breaker = False
    try:
        import aiobreaker  # noqa: F401

        use_circuit_breaker = True
    except ImportError:
        pass

    events_mapper_fn = events_mapper_with_circuit_breaker if use_circuit_breaker else events_mapper

    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=command_mapper,
        domain_events_mapper=events_mapper_fn,
    )

    print("\n" + "=" * 60)
    print("EVENT HANDLER FALLBACK EXAMPLE")
    print("=" * 60)
    print("\nSending command that emits NotificationSent...")
    print("Primary handler will fail; fallback handler will run.\n")

    await mediator.send(
        SendNotificationCommand(user_id="user_1", message="Hello from CQRS"),
    )

    print("\nResult:")
    print(f"  Handlers that ran (in order): {EVENTS_HANDLED_BY}")
    assert "primary" in EVENTS_HANDLED_BY and "fallback" in EVENTS_HANDLED_BY
    assert EVENTS_HANDLED_BY[-1] == "fallback"
    print("  ✓ Primary ran and failed; fallback ran and completed.")
    print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
