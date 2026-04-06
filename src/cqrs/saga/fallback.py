"""Fallback wrapper for Saga steps to handle failures gracefully."""

import dataclasses
import typing

from cqrs.circuit_breaker import ICircuitBreaker
from cqrs.saga.step import SagaStepHandler


@dataclasses.dataclass(frozen=True)
class Fallback:
    """
    Fallback wrapper for Saga steps.

    Allows defining an alternative step (fallback) to be executed when the primary
    step fails or when a circuit breaker is open.

    Attributes:
        step: The primary step handler class to execute.
        fallback: The fallback step handler class to execute if primary fails.
        failure_exceptions: Tuple of exception types that trigger fallback immediately.
        circuit_breaker: Optional circuit breaker instance specific to this step.

    Example::
        Fallback(
            step=ReserveInventoryStep,
            fallback=ReserveInventoryFallbackStep,
            failure_exceptions=(ConnectionError, TimeoutError),
            circuit_breaker=cb_adapter
        )
    """

    step: type[SagaStepHandler]
    fallback: type[SagaStepHandler]
    failure_exceptions: tuple[type[Exception], ...] = ()
    circuit_breaker: typing.Optional[ICircuitBreaker] = None
