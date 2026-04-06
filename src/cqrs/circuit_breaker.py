"""Unified circuit breaker protocol for Saga, Request and Event fallbacks."""

import typing


class ICircuitBreaker(typing.Protocol):
    """
    Unified interface for circuit breaker implementations.

    Used by Saga step fallbacks, Request handler fallbacks and Event handler
    fallbacks. The same adapter class works for all with identifier-based
    namespacing.

    Note:
        Implementors should use a dedicated adapter instance per domain (events,
        requests, saga) to keep circuit breaker state isolated.
    """

    async def call(
        self,
        identifier: typing.Union[type, str],
        func: typing.Callable[..., typing.Awaitable[typing.Any]],
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> typing.Any:
        """
        Execute the function with circuit breaker protection.

        Args:
            identifier: Handler/step type or string used as circuit breaker namespace.
            func: The async function to execute.
            *args: Positional arguments to pass to func.
            **kwargs: Keyword arguments to pass to func.

        Returns:
            The result of func execution.

        Raises:
            CircuitBreakerError: If the circuit breaker is open.
            Exception: Any exception raised by func (if circuit is closed).
        """
        ...

    def is_circuit_breaker_error(self, exc: Exception) -> bool:
        """
        Check if the given exception is a circuit breaker error.

        Args:
            exc: The exception to check.

        Returns:
            True if the exception is a circuit breaker error, False otherwise.
        """
        ...


def should_use_fallback(
    primary_error: Exception,
    circuit_breaker: typing.Optional[ICircuitBreaker],
    failure_exceptions: tuple[type[Exception], ...],
) -> bool:
    """
    Determine whether to invoke the fallback after primary handler failure.

    Returns True if the circuit breaker reports a breaker error, or the
    exception matches failure_exceptions, or failure_exceptions is empty
    (any exception triggers fallback).
    """
    if circuit_breaker is not None and circuit_breaker.is_circuit_breaker_error(
        primary_error,
    ):
        return True
    if failure_exceptions and isinstance(primary_error, failure_exceptions):
        return True
    if not failure_exceptions:
        return True
    return False
