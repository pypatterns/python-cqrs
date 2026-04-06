"""Fallback wrapper for event handlers with optional circuit breaker."""

import dataclasses
import typing

from cqrs.circuit_breaker import ICircuitBreaker
from cqrs.events import event_handler
from cqrs.generic_utils import get_generic_args_for_origin

EventHandlerT = typing.Type[event_handler.EventHandler]

_EVENT_HANDLER_ORIGINS: tuple[type, ...] = (event_handler.EventHandler,)


def _event_type_name(t: type) -> str:
    return getattr(t, "__name__", str(t))


@dataclasses.dataclass(frozen=True)
class EventHandlerFallback:
    """
    Fallback wrapper for event handlers.

    When the primary handler fails (or circuit breaker is open), the fallback
    handler is invoked. Use a separate circuit breaker instance per domain
    (e.g. one for events) that uses the same adapter class.

    Attributes:
        primary: The primary event handler class.
        fallback: The fallback handler class to execute if primary fails.
        failure_exceptions: Exception types that trigger fallback; if empty, any exception.
        circuit_breaker: Optional circuit breaker instance (e.g. AioBreakerAdapter).

    Example::
        event_cb = AioBreakerAdapter(fail_max=5, timeout_duration=60)
        event_map.bind(
            OrderCreatedEvent,
            EventHandlerFallback(
                SendEmailHandler,
                SendEmailFallbackHandler,
                circuit_breaker=event_cb,
            ),
        )
    """

    primary: EventHandlerT
    fallback: EventHandlerT
    failure_exceptions: tuple[type[Exception], ...] = ()
    circuit_breaker: typing.Optional[ICircuitBreaker] = None

    def __post_init__(self) -> None:
        if not isinstance(self.primary, type) or not isinstance(self.fallback, type):
            raise TypeError(
                "EventHandlerFallback primary and fallback must be handler classes",
            )
        if not issubclass(self.primary, event_handler.EventHandler):
            raise TypeError(
                f"EventHandlerFallback primary ({self.primary.__name__}) " "must be a subclass of EventHandler",
            )
        if not issubclass(self.fallback, event_handler.EventHandler):
            raise TypeError(
                f"EventHandlerFallback fallback ({self.fallback.__name__}) " "must be a subclass of EventHandler",
            )
        # Validate that primary and fallback handle the same event type
        primary_args = get_generic_args_for_origin(
            self.primary,
            _EVENT_HANDLER_ORIGINS,
            min_args=1,
        )
        fallback_args = get_generic_args_for_origin(
            self.fallback,
            _EVENT_HANDLER_ORIGINS,
            min_args=1,
        )
        if primary_args is not None and fallback_args is not None:
            # Reject TypeVar (unparameterized) so we only allow concrete types
            if any(isinstance(a, typing.TypeVar) for a in primary_args + fallback_args):
                raise TypeError(
                    "EventHandlerFallback primary and fallback must be parameterized with a concrete event type "
                    "(e.g. EventHandler[MyEvent])",
                )
            if primary_args[0] != fallback_args[0]:
                raise TypeError(
                    "EventHandlerFallback primary and fallback must handle the same event type: "
                    f"primary {self.primary.__name__} handles {_event_type_name(primary_args[0])}, "
                    f"fallback {self.fallback.__name__} handles {_event_type_name(fallback_args[0])}",
                )
        elif primary_args is None or fallback_args is None:
            raise TypeError(
                "EventHandlerFallback primary and fallback must be parameterized with a concrete event type "
                "(e.g. EventHandler[MyEvent])",
            )
