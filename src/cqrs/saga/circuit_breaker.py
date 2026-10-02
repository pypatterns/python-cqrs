"""Circuit breaker protocol for Saga steps."""

import typing

from cqrs.handlers.saga import SagaStepHandler


class ISagaStepCircuitBreaker(typing.Protocol):
    """
    Interface for circuit breaker implementations.

    Circuit breakers protect saga steps from cascading failures by opening
    the circuit when a service is unhealthy, allowing the system to fail fast
    and switch to fallback logic.
    """

    async def call(
        self,
        step_type: type[SagaStepHandler],
        func: typing.Callable[..., typing.Awaitable[typing.Any]],
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> typing.Any:
        """
        Execute the function with circuit breaker protection.

        Args:
            step_type: The step handler class type. MUST be used to determine
                      the namespace/identity of the breaker.
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

        This method allows implementations to determine if an exception
        represents a circuit breaker being open, without exposing concrete
        implementation details.

        Args:
            exc: The exception to check.

        Returns:
            True if the exception is a circuit breaker error, False otherwise.
        """
        ...
