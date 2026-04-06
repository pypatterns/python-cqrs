"""Execution components for Saga transactions."""

import copy
import dataclasses
import logging
import typing

from cqrs.container.protocol import Container
from cqrs.saga.fallback import Fallback
from cqrs.saga.models import ContextT, SagaContext
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.enums import SagaStepStatus
from cqrs.saga.storage.protocol import ISagaStorage, SagaStorageRun

logger = logging.getLogger("cqrs.saga")


class SagaStateManager:
    """Manages saga state in storage."""

    def __init__(
        self,
        saga_id: typing.Any,
        storage: typing.Union[ISagaStorage, SagaStorageRun],
    ) -> None:
        """
        Create a SagaStateManager bound to a specific saga identifier and storage backend.

        Parameters:
            saga_id: Identifier for the saga instance.
            storage: Storage backend implementing ISagaStorage or SagaStorageRun used to persist saga state and history.
        """
        self._saga_id = saga_id
        self._storage = storage

    async def create_saga(
        self,
        saga_name: str,
        context: SagaContext,
    ) -> None:
        """Create a new saga in storage."""
        await self._storage.create_saga(
            self._saga_id,
            saga_name,
            context.to_dict(),
        )

    async def update_status(self, status: typing.Any) -> None:
        """Update saga status."""
        await self._storage.update_status(self._saga_id, status)

    async def update_context(self, context: SagaContext) -> None:
        """Update saga context."""
        await self._storage.update_context(
            self._saga_id,
            context.to_dict(),
        )

    async def log_step(
        self,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        error: typing.Optional[str] = None,
    ) -> None:
        """Log step execution."""
        await self._storage.log_step(
            self._saga_id,
            step_name,
            action,
            status,
            details=error,
        )


class SagaRecoveryManager:
    """Manages saga recovery from storage."""

    def __init__(
        self,
        saga_id: typing.Any,
        storage: typing.Union[ISagaStorage, SagaStorageRun],
        container: Container,
        saga_steps: list[typing.Union[type[SagaStepHandler], Fallback]],
    ) -> None:
        """
        Construct a SagaRecoveryManager that holds the identifiers, storage, DI container, and configured saga steps required to reconstruct a saga's execution state.

        Parameters:
            saga_id: Identifier for the saga instance (e.g., UUID or other unique value).
            storage: Persistence backend implementing saga history operations (ISagaStorage or SagaStorageRun).
            container: Dependency injection container used to resolve step handler instances.
            saga_steps: Ordered list of saga step types or Fallback wrappers that define the saga's execution sequence.
        """
        self._saga_id = saga_id
        self._storage = storage
        self._container = container
        self._saga_steps = saga_steps

    async def load_completed_step_names(self) -> set[str]:
        """
        Return the names of saga steps that completed their primary ("act") action.

        Returns:
            set[str]: Step names recorded with status `SagaStepStatus.COMPLETED` and action `"act"`.
        """
        history = await self._storage.get_step_history(self._saga_id)
        return {e.step_name for e in history if e.status == SagaStepStatus.COMPLETED and e.action == "act"}

    async def reconstruct_completed_steps(
        self,
        completed_step_names: set[str],
    ) -> list[SagaStepHandler[SagaContext, typing.Any]]:
        """
        Reconstructs and returns the resolved step handler instances corresponding to the completed steps, preserving saga execution order.

        Parameters:
            completed_step_names (set[str]): Names of steps that completed the "act" action.

        Returns:
            list[SagaStepHandler[SagaContext, typing.Any]]: Resolved step handler instances in execution order. For Fallback wrappers, the primary handler is chosen if its name appears in completed_step_names; otherwise the fallback handler is chosen when present.
        """
        completed_steps: list[SagaStepHandler[SagaContext, typing.Any]] = []

        for step_item in self._saga_steps:
            # Handle Fallback wrapper
            if isinstance(step_item, Fallback):
                # Check both primary and fallback step names
                primary_name = step_item.step.__name__
                fallback_name = step_item.fallback.__name__
                if primary_name in completed_step_names:
                    step = await self._container.resolve(step_item.step)
                    completed_steps.append(step)
                elif fallback_name in completed_step_names:
                    step = await self._container.resolve(step_item.fallback)
                    completed_steps.append(step)
            else:
                # Regular step
                step_name = step_item.__name__
                if step_name in completed_step_names:
                    step = await self._container.resolve(step_item)
                    completed_steps.append(step)

        return completed_steps


class SagaStepExecutor(typing.Generic[ContextT]):
    """Executes regular saga steps."""

    def __init__(
        self,
        context: ContextT,
        container: Container,
        state_manager: SagaStateManager,
    ) -> None:
        """
        Initialize step executor.

        Args:
            context: Saga context
            container: DI container for resolving step handlers
            state_manager: State manager for logging and updates
        """
        self._context = context
        self._container = container
        self._state_manager = state_manager

    async def execute_step(
        self,
        step_type: type[SagaStepHandler],
        step_name: str,
    ) -> SagaStepResult[ContextT, typing.Any]:
        """
        Execute a regular saga step.

        Args:
            step_type: Type of the step handler
            step_name: Name of the step (for logging)

        Returns:
            Result of step execution
        """
        # Resolve step handler from DI container
        step = await self._container.resolve(step_type)

        # Log step start
        await self._state_manager.log_step(
            step_name,
            "act",
            SagaStepStatus.STARTED,
        )

        # Execute step
        step_result = await step.act(self._context)

        # Update context and log completion
        await self._state_manager.update_context(self._context)
        await self._state_manager.log_step(
            step_name,
            "act",
            SagaStepStatus.COMPLETED,
        )

        return step_result


class FallbackStepExecutor(typing.Generic[ContextT]):
    """Executes Fallback wrapper steps with context snapshot/restore."""

    def __init__(
        self,
        context: ContextT,
        container: Container,
        state_manager: SagaStateManager,
    ) -> None:
        """
        Initialize fallback step executor.

        Args:
            context: Saga context
            container: DI container for resolving step handlers
            state_manager: State manager for logging and updates
        """
        self._context = context
        self._container = container
        self._state_manager = state_manager

    async def execute_fallback_step(
        self,
        fallback_wrapper: Fallback,
        completed_step_names: set[str],
    ) -> typing.Optional[
        tuple[
            SagaStepResult[ContextT, typing.Any],
            SagaStepHandler,
        ]
    ]:
        """
        Execute a Fallback step with context snapshot/restore mechanism.

        Args:
            fallback_wrapper: The Fallback instance containing step and fallback
            completed_step_names: Set of completed step names for idempotency check

        Returns:
            Step result if executed, None if skipped (already completed)

        Raises:
            Exception: If both primary and fallback steps fail
        """
        primary_step_name = fallback_wrapper.step.__name__
        fallback_step_name = fallback_wrapper.fallback.__name__

        # Idempotency: Check if either primary or fallback is already completed
        if primary_step_name in completed_step_names:
            logger.debug(
                f"Skipping already completed Fallback primary step: {primary_step_name}",
            )
            return None

        if fallback_step_name in completed_step_names:
            logger.debug(
                f"Skipping already completed Fallback fallback step: {fallback_step_name}",
            )
            return None

        # Resolve step handlers
        primary_step = await self._container.resolve(fallback_wrapper.step)
        fallback_step = await self._container.resolve(fallback_wrapper.fallback)

        # Create context snapshot before executing primary step
        context_snapshot = copy.deepcopy(self._context.to_dict())

        # Try to execute primary step
        try:
            await self._state_manager.log_step(
                primary_step_name,
                "act",
                SagaStepStatus.STARTED,
            )

            # Execute primary step with circuit breaker if present
            if fallback_wrapper.circuit_breaker is not None:
                step_result = await fallback_wrapper.circuit_breaker.call(
                    fallback_wrapper.step,
                    primary_step.act,
                    self._context,
                )
            else:
                step_result = await primary_step.act(self._context)

            # Primary step succeeded
            await self._state_manager.update_context(self._context)
            await self._state_manager.log_step(
                primary_step_name,
                "act",
                SagaStepStatus.COMPLETED,
            )
            return step_result, primary_step

        except Exception as primary_error:
            should_fallback = False

            # 1. Check Circuit Breaker
            if (
                fallback_wrapper.circuit_breaker is not None
                and fallback_wrapper.circuit_breaker.is_circuit_breaker_error(
                    primary_error,
                )
            ):
                logger.warning(
                    f"Circuit breaker open for step '{primary_step_name}'. "
                    f"Switching to fallback '{fallback_step_name}'.",
                )
                should_fallback = True

            # 2. Check failure_exceptions if defined
            elif fallback_wrapper.failure_exceptions:
                if isinstance(primary_error, fallback_wrapper.failure_exceptions):
                    should_fallback = True

            # 3. If no specific exceptions defined, catch all
            else:
                should_fallback = True

            if should_fallback:
                # Log warning but DO NOT log FAILED status for primary step
                logger.warning(
                    f"Primary step '{primary_step_name}' failed: {primary_error}. "
                    f"Switching to fallback '{fallback_step_name}'.",
                )

                # Restore context from snapshot
                restored_context = self._context.__class__.from_dict(context_snapshot)
                # Copy all fields from restored context to the existing one
                for field in dataclasses.fields(self._context):
                    setattr(
                        self._context,
                        field.name,
                        getattr(restored_context, field.name),
                    )

                # Execute fallback step
                try:
                    await self._state_manager.log_step(
                        fallback_step_name,
                        "act",
                        SagaStepStatus.STARTED,
                    )

                    step_result = await fallback_step.act(self._context)

                    # Fallback succeeded
                    await self._state_manager.update_context(self._context)
                    await self._state_manager.log_step(
                        fallback_step_name,
                        "act",
                        SagaStepStatus.COMPLETED,
                    )
                    return step_result, fallback_step

                except Exception as fallback_error:
                    # Fallback also failed - saga fails
                    await self._state_manager.log_step(
                        fallback_step_name,
                        "act",
                        SagaStepStatus.FAILED,
                        str(fallback_error),
                    )
                    raise fallback_error
            else:
                # Should not fallback, re-raise original error
                raise primary_error
