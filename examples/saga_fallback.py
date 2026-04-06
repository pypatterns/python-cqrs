"""
Example: Saga Fallback Pattern with Circuit Breaker

This example demonstrates how to configure a saga with fallback steps and
circuit breaker protection. The fallback pattern allows defining alternative
steps to execute when primary steps fail, while circuit breakers prevent
cascading failures by opening the circuit when a service becomes unhealthy.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Prerequisites:
   pip install aiobreaker  # Required for circuit breaker functionality

Run the example:
   python examples/saga_fallback.py

The example will demonstrate:
- Primary step that always fails
- Fallback step execution when primary fails
- Circuit breaker opening after repeated failures
- Primary step NOT executing when circuit breaker is open (fail fast)

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Fallback Pattern Configuration:
   - Define primary step handler that fails
   - Define fallback step handler as alternative
   - Wrap step with Fallback() to enable fallback behavior
   - Automatic context snapshot/restore before fallback execution

2. Circuit Breaker Integration:
   - Use AioBreakerAdapter to protect steps from cascading failures
   - Configure fail_max (failure threshold) and timeout_duration
   - Circuit breaker opens after threshold failures

3. Fallback Execution Flow:
   - Primary step executes first and fails
   - Fallback step executes automatically
   - Context is restored from snapshot before fallback execution

4. Circuit Breaker Protection:
   - After threshold failures, circuit breaker opens
   - When circuit is OPEN, PrimaryStep is NOT executed (fail fast)
   - Fallback is NOT triggered for CircuitBreakerError
   - This prevents unnecessary load on failing services

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - aiobreaker (for circuit breaker functionality)
   - pydantic (for context models)
   - di (for dependency injection)

Install circuit breaker dependency:
   pip install aiobreaker

Or install with optional dependencies:
   pip install python-cqrs[aiobreaker]

================================================================================
"""

import asyncio
import dataclasses
import logging
import typing
import uuid

import di
from di import dependent

import cqrs
from cqrs.adapters.circuit_breaker import AioBreakerAdapter
from cqrs.events.event import Event
from cqrs.response import Response
from cqrs.saga import bootstrap
from cqrs.saga.fallback import Fallback
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.memory import MemorySagaStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# Domain Models
# ============================================================================


@dataclasses.dataclass
class OrderContext(SagaContext):
    """Shared context passed between all saga steps."""

    order_id: str
    user_id: str
    amount: float

    # This field is populated by step during execution
    reservation_id: typing.Optional[str] = None


# ============================================================================
# Step Responses
# ============================================================================


class ReserveInventoryResponse(Response):
    """Response from inventory reservation step."""

    reservation_id: str
    source: str  # "primary" or "fallback"


# ============================================================================
# Saga Step Handlers
# ============================================================================


class PrimaryStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    """Primary step that always fails."""

    def __init__(self) -> None:
        self._events: list[Event] = []
        self._call_count = 0  # Track how many times act() was called

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    @property
    def call_count(self) -> int:
        """Get number of times act() was called."""
        return self._call_count

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        """
        Simulate a failing primary reservation step for the saga.

        This action always raises a RuntimeError to emulate an unavailable downstream service and trigger fallback or compensation behavior.

        Parameters:
            context (OrderContext): Shared saga context containing order details (e.g., order_id, user_id, amount, reservation_id).

        Raises:
            RuntimeError: Indicates the primary step failed (service unavailable).
        """
        self._call_count += 1
        logger.info(
            f"  [PrimaryStep] Executing act() for order {context.order_id} " f"(call #{self._call_count})...",
        )
        raise RuntimeError("Primary step failed - service unavailable")

    async def compensate(self, context: OrderContext) -> None:
        """Compensation for primary step."""
        logger.info(f"  Compensating primary step for order {context.order_id}")


class FallbackStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    """Fallback step that succeeds."""

    def __init__(self) -> None:
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        """Fallback step that succeeds."""
        logger.info(f"  Executing fallback step for order {context.order_id}...")
        reservation_id = f"fallback_reservation_{context.order_id}"

        # Update context
        context.reservation_id = reservation_id

        response = ReserveInventoryResponse(
            reservation_id=reservation_id,
            source="fallback",
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        """Compensation for fallback step."""
        logger.info(f"  Compensating fallback step for order {context.order_id}")


# ============================================================================
# Saga Class Definition with Fallback
# ============================================================================


class OrderSagaWithFallback(Saga[OrderContext]):
    """Order processing saga with fallback step and circuit breaker protection."""

    steps = [
        Fallback(
            step=PrimaryStep,
            fallback=FallbackStep,
            circuit_breaker=AioBreakerAdapter(
                fail_max=2,  # Circuit opens after 2 failures
                timeout_duration=60,  # Wait 60 seconds before retry
            ),
        ),
    ]


# ============================================================================
# Main Example
# ============================================================================


async def run_saga(
    mediator: cqrs.SagaMediator,
    storage: MemorySagaStorage,
    order_id: str,
    primary_step: PrimaryStep,
) -> None:
    """Run a single saga execution."""
    saga_id = uuid.uuid4()
    context = OrderContext(
        order_id=order_id,
        user_id="user_123",
        amount=100.0,
    )

    print(f"\nProcessing order {order_id} (saga_id: {saga_id})...")
    initial_call_count = primary_step.call_count

    try:
        step_results = []
        async for step_result in mediator.stream(context, saga_id=saga_id):
            step_results.append(step_result)
            step_name = step_result.step_type.__name__
            if hasattr(step_result.response, "source"):
                source = getattr(step_result.response, "source", "N/A")
                print(f"  ✓ Step completed: {step_name} (source: {source})")
            else:
                print(f"  ✓ Step completed: {step_name}")

        print("  ✓ Saga completed successfully")
        print(
            f"  - PrimaryStep.act() was called: {primary_step.call_count - initial_call_count} time(s)",
        )

    except Exception as e:
        # Check if it's a CircuitBreakerError
        try:
            from aiobreaker import CircuitBreakerError

            is_circuit_breaker_error = isinstance(e, CircuitBreakerError)
        except ImportError:
            # Fallback: check by error message/type name
            is_circuit_breaker_error = "CircuitBreakerError" in str(type(e).__name__)

        if is_circuit_breaker_error:
            print("  ✗ CircuitBreakerError: Circuit is OPEN")
            print(
                f"  - PrimaryStep.act() was called: {primary_step.call_count - initial_call_count} time(s)",
            )
            print("  - PrimaryStep was NOT executed (fail fast)")
            print("  - Fallback was NOT triggered")
        else:
            print(f"  ✗ Saga failed: {e}")
            print(
                f"  - PrimaryStep.act() was called: {primary_step.call_count - initial_call_count} time(s)",
            )


async def main() -> None:
    """
    Run an interactive demonstration of the saga fallback pattern with a circuit breaker.

    Executes three scenarios that show a failing primary step with an automatic fallback, the circuit breaker opening after a configurable number of failures, and fail-fast behavior when the circuit is open. Also conditionally demonstrates configuring a Redis-backed circuit breaker storage, prints per-scenario results and a summary, and informs about missing optional dependencies.
    """
    print("\n" + "=" * 80)
    print("SAGA FALLBACK PATTERN WITH CIRCUIT BREAKER EXAMPLE")
    print("=" * 80)
    print("\nThis example demonstrates:")
    print("  • Primary step that always fails")
    print("  • Automatic fallback step execution")
    print("  • Circuit breaker opening after 2 failures")
    print("  • Primary step NOT executing when circuit breaker is open")

    # Setup DI container
    di_container = di.Container()

    # Register step handlers (create instances to track call count)
    primary_step = PrimaryStep()
    fallback_step = FallbackStep()

    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: primary_step, scope="request"),
            PrimaryStep,
        ),
    )
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: fallback_step, scope="request"),
            FallbackStep,
        ),
    )

    # Create saga storage (supports create_run(): one session per saga, checkpoint commits)
    storage = MemorySagaStorage()
    di_container.bind(
        di.bind_by_type(
            dependent.Dependent(lambda: storage, scope="request"),
            MemorySagaStorage,
        ),
    )

    # Define saga mapper
    def saga_mapper(mapper: cqrs.SagaMap) -> None:
        mapper.bind(OrderContext, OrderSagaWithFallback)

    # Create saga mediator using bootstrap
    mediator = bootstrap.bootstrap(
        di_container=di_container,
        sagas_mapper=saga_mapper,
        saga_storage=storage,
    )

    print("\n" + "=" * 80)
    print("SCENARIO 1: First execution (PrimaryStep fails, FallbackStep succeeds)")
    print("=" * 80)
    await run_saga(mediator, storage, "order_001", primary_step)

    print("\n" + "=" * 80)
    print(
        "SCENARIO 2: Second execution (PrimaryStep fails again, FallbackStep succeeds)",
    )
    print("=" * 80)
    print("  (After 2 failures, circuit breaker will open)")
    await run_saga(mediator, storage, "order_002", primary_step)

    print("\n" + "=" * 80)
    print("SCENARIO 3: Third execution (Circuit breaker is OPEN)")
    print("=" * 80)
    print("  (PrimaryStep will NOT be executed - fail fast)")
    await run_saga(mediator, storage, "order_003", primary_step)

    # ------------------------------------------------------------------------
    # DEMO: Redis Storage Configuration (Optional)
    # ------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("DEMO: Configuring Circuit Breaker with Redis Storage")
    print("=" * 80)

    try:
        import redis
        from aiobreaker.storage.redis import CircuitRedisStorage
        from aiobreaker import CircuitBreakerState

        # Factory function to create Redis storage
        def redis_storage_factory(name: str):
            # Note: decode_responses=False is important for aiobreaker compatibility
            client = redis.from_url(
                "redis://localhost:6379",
                encoding="utf-8",
                decode_responses=False,
            )
            return CircuitRedisStorage(
                state=CircuitBreakerState.CLOSED,
                redis_object=client,
                namespace=name,
            )

        # Example configuration with Redis storage
        class OrderSagaWithRedisBreaker(Saga[OrderContext]):
            steps = [
                Fallback(
                    step=PrimaryStep,
                    fallback=FallbackStep,
                    circuit_breaker=AioBreakerAdapter(
                        fail_max=2,
                        timeout_duration=60,
                        storage_factory=redis_storage_factory,
                    ),
                ),
            ]

        print("✓ Successfully defined Saga with Redis-backed Circuit Breaker")
        print("  (Use this pattern to share circuit state across multiple instances)")

    except ImportError:
        print("ℹ️ Redis dependencies not installed. Skipping Redis demo.")
        print("  To use Redis storage, install: pip install redis")

    print("\n" + "=" * 80)
    print("✅ EXAMPLE COMPLETED")
    print("=" * 80)
    print("\nSummary:")
    print(f"  • Total PrimaryStep.act() calls: {primary_step.call_count}")
    print(
        "  • First 2 executions: PrimaryStep executed and failed, FallbackStep succeeded",
    )
    print("  • Third execution: PrimaryStep NOT executed (circuit breaker OPEN)")
    print("  • Circuit breaker prevents unnecessary load on failing service")
    print("\nKey Takeaways:")
    print("  • Circuit breaker opens after threshold failures")
    print("  • When circuit is OPEN, PrimaryStep is NOT executed (fail fast)")
    print("  • Fallback is NOT triggered for CircuitBreakerError")
    print("  • This prevents cascading failures and unnecessary load")
    print("\n" + "=" * 80 + "\n")

    try:
        import aiobreaker  # noqa: F401
    except ImportError:
        print("\n" + "=" * 80)
        print("❌ MISSING DEPENDENCY")
        print("=" * 80)
        print("\naiobreaker is required for circuit breaker functionality.")
        print("\nInstall it with:")
        print("  pip install aiobreaker")
        print("\nOr install with optional dependencies:")
        print("  pip install python-cqrs[aiobreaker]")
        print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
