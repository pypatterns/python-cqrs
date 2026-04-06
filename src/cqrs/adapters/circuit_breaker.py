"""Circuit breaker adapter for aiobreaker integration."""

import logging
import typing
from datetime import timedelta
from typing import TYPE_CHECKING, Callable

from cqrs.circuit_breaker import ICircuitBreaker
from cqrs.saga.circuit_breaker import ISagaStepCircuitBreaker

logger = logging.getLogger("cqrs.adapters.circuit_breaker")

if TYPE_CHECKING:
    from aiobreaker.storage.base import CircuitBreakerStorage as _CircuitBreakerStorage
else:
    _CircuitBreakerStorage = typing.Any

try:
    import aiobreaker
    from aiobreaker import CircuitBreaker, CircuitBreakerError
    from aiobreaker.storage.base import CircuitBreakerStorage

    try:
        from aiobreaker import CircuitBreakerListener
    except ImportError:
        # Fallback if CircuitBreakerListener is not available
        CircuitBreakerListener = object  # type: ignore[assignment, misc]
except ImportError:
    aiobreaker = None  # type: ignore[assignment, misc]
    CircuitBreaker = None  # type: ignore[assignment, misc]
    CircuitBreakerError = None  # type: ignore[assignment, misc]
    CircuitBreakerListener = object  # type: ignore[assignment, misc]
    CircuitBreakerStorage = typing.Any  # type: ignore[assignment, misc]

    logger.warning(
        "aiobreaker is not installed. Install it with: pip install aiobreaker",
    )


class CriticalLogListener(CircuitBreakerListener if aiobreaker else object):  # type: ignore[misc]
    """
    Listener for circuit breaker state changes with critical logging.

    Implements CircuitBreakerListener interface methods.
    """

    def __init__(self, breaker_name: str):
        self._breaker_name = breaker_name

    def before_call(
        self,
        breaker: typing.Any,
        func: typing.Any,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> None:
        """Called before the circuit breaker calls the function."""
        pass

    def failure(self, breaker: typing.Any, exception: Exception) -> None:
        """Called when the function fails."""
        pass

    def success(self, breaker: typing.Any) -> None:
        """Called when the function succeeds."""
        pass

    def state_change(
        self,
        breaker: typing.Any,
        old: typing.Any,
        new: typing.Any,
    ) -> None:
        """Log circuit breaker state changes at critical level."""
        logger.critical(
            f"{self._breaker_name} circuit breaker state changed: {old.state} -> {new.state}",
        )


StorageFactory = Callable[[str], _CircuitBreakerStorage]


def default_memory_storage_factory(name: str) -> _CircuitBreakerStorage:
    """
    Default factory returning Memory Storage.

    Args:
        name: Name of the circuit breaker (namespace).

    Returns:
        CircuitMemoryStorage instance.
    """
    if aiobreaker is None:
        raise ImportError(
            "aiobreaker is not installed. Install it with: pip install aiobreaker",
        )

    try:
        from aiobreaker.storage.memory import CircuitMemoryStorage
    except ImportError:
        raise ImportError(
            "Memory storage requires aiobreaker. Make sure aiobreaker is installed.",
        )

    return CircuitMemoryStorage(state=aiobreaker.CircuitBreakerState.CLOSED)


def _identifier_to_name(identifier: typing.Union[type, str]) -> str:
    """Build circuit breaker namespace from type or string."""
    if isinstance(identifier, str):
        return identifier
    module = getattr(identifier, "__module__", "")
    name = getattr(identifier, "__name__", str(identifier))
    return f"{module}.{name}" if module else name


class AioBreakerAdapter(ICircuitBreaker, ISagaStepCircuitBreaker):
    """
    Unified adapter for aiobreaker circuit breaker.

    Implements ICircuitBreaker (and ISagaStepCircuitBreaker for backward
    compatibility). Each fallback type (Saga, Request, Event) typically
    uses its own adapter instance; identifier can be a step/handler type
    or a string for namespace.

    Attributes:
        fail_max: Maximum number of failures before opening the circuit.
        timeout_duration: Time to wait before attempting to reset the circuit (in seconds).
        exclude: List of exception types that should NOT open the circuit
                (business exceptions).
        storage_factory: Factory function to create circuit breaker storage.
                        Defaults to in-memory storage.

    Example::
        # One adapter per fallback domain (each has its own breaker namespaces)
        saga_cb = AioBreakerAdapter(fail_max=3, timeout_duration=60)
        request_cb = AioBreakerAdapter(fail_max=5, timeout_duration=30)
        event_cb = AioBreakerAdapter(fail_max=5, timeout_duration=30)
    """

    def __init__(
        self,
        fail_max: int = 5,
        timeout_duration: int = 60,
        exclude: typing.Optional[list[type[Exception]]] = None,
        storage_factory: typing.Optional[StorageFactory] = None,
    ) -> None:
        if CircuitBreaker is None:
            raise ImportError(
                "aiobreaker is not installed. Install it with: pip install aiobreaker",
            )

        self._fail_max = fail_max
        self._timeout_duration = timeout_duration
        self._exclude = exclude or []
        self._storage_factory = storage_factory or default_memory_storage_factory

        # Dictionary to store circuit breakers per identifier (type or str)
        self._breakers: dict[str, typing.Any] = {}  # type: ignore[type-arg]

    def _get_breaker(self, identifier: typing.Union[type, str]) -> typing.Any:  # type: ignore[return-type]
        """
        Get or create circuit breaker for an identifier (type or string).

        Args:
            identifier: Step/handler type or string for namespace.

        Returns:
            CircuitBreaker instance for this identifier.
        """
        name = _identifier_to_name(identifier)
        if name not in self._breakers:
            self._breakers[name] = self._create_breaker(name)
        return self._breakers[name]

    def _create_breaker(self, name: str) -> typing.Any:  # type: ignore[return-type]
        """
        Create circuit breaker for a step.

        For each step, creates a breaker with storage from factory.
        Each step gets its own isolated circuit breaker instance.

        Args:
            name: Name for the circuit breaker (step's fully qualified name).

        Returns:
            CircuitBreaker instance.
        """
        if CircuitBreaker is None:
            raise ImportError(
                "aiobreaker is not installed. Install it with: pip install aiobreaker",
            )

        # Create storage using factory
        storage = self._storage_factory(name)

        listeners: list[typing.Any] = [CriticalLogListener(name)]

        return CircuitBreaker(
            # When CircuitBreaker is not None, CircuitBreakerStorage is the actual type
            state_storage=storage,  # type: ignore[arg-type]
            name=name,
            fail_max=self._fail_max,
            timeout_duration=timedelta(seconds=self._timeout_duration),
            exclude=self._exclude,
            listeners=listeners,
        )

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
            identifier: Step/handler type or string for breaker namespace.
            func: The async function to execute.
            *args: Positional arguments to pass to func.
            **kwargs: Keyword arguments to pass to func.

        Returns:
            The result of func execution.

        Raises:
            CircuitBreakerError: If the circuit breaker is open.
            Exception: Any exception raised by func (if circuit is closed).
        """
        breaker = self._get_breaker(identifier)
        return await breaker.call_async(func, *args, **kwargs)

    def is_circuit_breaker_error(self, exc: Exception) -> bool:
        """
        Check if the given exception is a circuit breaker error.

        Args:
            exc: The exception to check.

        Returns:
            True if the exception is a CircuitBreakerError, False otherwise.
        """
        if CircuitBreakerError is None:
            return False
        return isinstance(exc, CircuitBreakerError)
