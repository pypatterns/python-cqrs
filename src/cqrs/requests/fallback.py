"""Fallback wrapper for request handlers with optional circuit breaker."""

import dataclasses
import typing

from cqrs.circuit_breaker import ICircuitBreaker
from cqrs.generic_utils import get_generic_args_for_origin
from cqrs.requests.request_handler import RequestHandler, StreamingRequestHandler

RequestHandlerT = typing.Union[type[RequestHandler], type[StreamingRequestHandler]]

_REQUEST_HANDLER_ORIGINS: tuple[type, ...] = (RequestHandler, StreamingRequestHandler)


def _type_name(t: type) -> str:
    return getattr(t, "__name__", str(t))


@dataclasses.dataclass(frozen=True)
class RequestHandlerFallback:
    """
    Fallback wrapper for request handlers.

    When the primary handler fails (or circuit breaker is open), the fallback
    handler is invoked. Use a separate circuit breaker instance per domain
    (e.g. one for requests) that uses the same adapter class.

    Attributes:
        primary: The primary request handler class.
        fallback: The fallback handler class to execute if primary fails.
        failure_exceptions: Exception types that trigger fallback; if empty, any exception.
        circuit_breaker: Optional circuit breaker instance (e.g. AioBreakerAdapter).

    Example::
        request_cb = AioBreakerAdapter(fail_max=5, timeout_duration=60)
        request_map.bind(
            MyCommand,
            RequestHandlerFallback(
                MyCommandHandler,
                MyCommandHandlerFallback,
                failure_exceptions=(ConnectionError, TimeoutError),
                circuit_breaker=request_cb,
            ),
        )
    """

    primary: RequestHandlerT
    fallback: RequestHandlerT
    failure_exceptions: tuple[type[Exception], ...] = ()
    circuit_breaker: typing.Optional[ICircuitBreaker] = None

    def __post_init__(self) -> None:
        if not isinstance(self.primary, type) or not isinstance(self.fallback, type):
            raise TypeError(
                "RequestHandlerFallback primary and fallback must be handler classes",
            )
        primary_streaming = issubclass(self.primary, StreamingRequestHandler)
        fallback_streaming = issubclass(self.fallback, StreamingRequestHandler)
        if primary_streaming != fallback_streaming:
            raise TypeError(
                "RequestHandlerFallback primary and fallback must be the same handler base type: "
                "both RequestHandler or both StreamingRequestHandler",
            )
        # Validate that primary and fallback handle the same request and response types
        primary_args = get_generic_args_for_origin(
            self.primary,
            _REQUEST_HANDLER_ORIGINS,
            min_args=2,
        )
        fallback_args = get_generic_args_for_origin(
            self.fallback,
            _REQUEST_HANDLER_ORIGINS,
            min_args=2,
        )
        if primary_args is not None and fallback_args is not None:
            # Reject TypeVar (unparameterized) so we only allow concrete types
            if any(isinstance(a, typing.TypeVar) for a in primary_args + fallback_args):
                raise TypeError(
                    "RequestHandlerFallback primary and fallback must be parameterized with concrete types "
                    "(e.g. RequestHandler[MyCommand, MyResult] or StreamingRequestHandler[MyCommand, MyResult])",
                )
            if primary_args[0] != fallback_args[0]:
                raise TypeError(
                    "RequestHandlerFallback primary and fallback must handle the same request type: "
                    f"primary {self.primary.__name__} handles {_type_name(primary_args[0])}, "
                    f"fallback {self.fallback.__name__} handles {_type_name(fallback_args[0])}",
                )
            if primary_args[1] != fallback_args[1]:
                raise TypeError(
                    "RequestHandlerFallback primary and fallback must have the same response type: "
                    f"primary {self.primary.__name__} returns {_type_name(primary_args[1])}, "
                    f"fallback {self.fallback.__name__} returns {_type_name(fallback_args[1])}",
                )
        elif primary_args is None or fallback_args is None:
            raise TypeError(
                "RequestHandlerFallback primary and fallback must be parameterized with concrete types "
                "(e.g. RequestHandler[MyCommand, MyResult] or StreamingRequestHandler[MyCommand, MyResult])",
            )
