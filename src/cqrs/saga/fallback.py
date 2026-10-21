"""
Fallback wrapper for saga steps.

[AI Instruction]
Import ``Fallback`` from this module. It is not an attribute of ``cqrs``.
Place it in ``Saga.steps`` in place of a single step class.
"""

import dataclasses

from cqrs.circuit_breaker import ICircuitBreaker
from cqrs.handlers.saga import SagaStepHandler


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

    [AI Instruction]
    Put this in ``Saga.steps`` when the primary step may fail and another
    step can still complete the same work. ``step`` and ``fallback`` are
    ``SagaStepHandler`` classes for the same context. An empty
    ``failure_exceptions`` means any error switches to the fallback. Use one
    circuit breaker instance per step so its open state does not trip
    unrelated steps.
    """

    step: type[SagaStepHandler]
    fallback: type[SagaStepHandler]
    failure_exceptions: tuple[type[Exception], ...] = ()
    circuit_breaker: ICircuitBreaker | None = None
