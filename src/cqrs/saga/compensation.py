"""Compensation components for Saga transactions."""

import asyncio
import contextlib
import logging
import typing

from cqrs.container.protocol import Container
from cqrs.container.scope import ScopeStrategy, handler_scope
from cqrs.saga.execution import SagaStepRef
from cqrs.saga.models import ContextT
from cqrs.saga.step import SagaStepHandler
from cqrs.saga.storage.enums import SagaStepStatus, SagaStatus
from cqrs.saga.storage.protocol import ISagaStorage, SagaStorageRun

logger = logging.getLogger("cqrs.saga")

CompletedStep: typing.TypeAlias = SagaStepHandler[typing.Any, typing.Any] | SagaStepRef


def _step_type(step: CompletedStep) -> type[SagaStepHandler]:
    if isinstance(step, SagaStepRef):
        return step.step_type
    return type(step)


class SagaCompensator(typing.Generic[ContextT]):
    """Handles compensation of saga steps with retry mechanism."""

    def __init__(
        self,
        saga_id: typing.Any,
        context: ContextT,
        storage: ISagaStorage | SagaStorageRun,
        retry_count: int = 3,
        retry_delay: float = 1.0,
        retry_backoff: float = 2.0,
        on_after_compensate_step: typing.Callable[[], typing.Awaitable[None]] | None = None,
        container: Container | None = None,
        scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    ) -> None:
        """
        Create a SagaCompensator configured to perform compensation of completed saga steps with retry and optional post-step callback.

        Parameters:
            saga_id: Identifier of the saga.
            context: Saga execution context passed to step compensation handlers.
            storage: Storage or run object implementing saga persistence operations.
            retry_count: Maximum number of attempts per step before giving up.
            retry_delay: Initial delay in seconds before the first retry.
            retry_backoff: Multiplier applied to the delay for each successive retry (exponential backoff).
            on_after_compensate_step: Optional async callback invoked after each step is successfully compensated.
            container: DI container used to re-resolve steps under ScopeStrategy.HANDLER.
            scope_strategy: DI scope boundary. Under HANDLER each step is compensated
                in a fresh scope with a freshly resolved step instance.
        """
        self._saga_id = saga_id
        self._context = context
        self._storage = storage
        self._retry_count = retry_count
        self._retry_delay = retry_delay
        self._retry_backoff = retry_backoff
        self._on_after_compensate_step = on_after_compensate_step
        self._container = container
        self._scope_strategy = scope_strategy

    async def compensate_steps(
        self,
        completed_steps: list[CompletedStep],
    ) -> None:
        """
        Compensates completed saga steps in reverse order, applying retry logic and recording step statuses.

        Compensates each handler from last to first, skipping steps already recorded as compensated in the saga history. Updates the saga status to COMPENSATING at the start and logs per-step statuses (STARTED, COMPLETED, FAILED) in storage. After a step completes, the optional on_after_compensate_step callback (if provided) is awaited. If any step fails after all retry attempts, the saga is marked as FAILED. If no completed steps are provided, no compensation is attempted and the saga is marked as FAILED.

        Parameters:
            completed_steps: Handlers or skip/recovery markers corresponding to
                steps that completed during the saga; these will be compensated
                in reverse order.

        Returns:
            None
        """
        await self._storage.update_status(self._saga_id, SagaStatus.COMPENSATING)

        if not completed_steps:
            logger.info(
                f"Saga {self._saga_id}: completed_steps is empty, "
                "skipping compensation (no step.compensate() will be called).",
            )
            await self._storage.update_status(self._saga_id, SagaStatus.FAILED)
            return

        # Load history to skip already compensated steps
        history = await self._storage.get_step_history(self._saga_id)
        compensated_steps = {
            e.step_name for e in history if e.status == SagaStepStatus.COMPLETED and e.action == "compensate"
        }

        compensation_errors: list[tuple[str, Exception]] = []

        for completed_step in reversed(completed_steps):
            # Taken before a possible re-resolve so history keeps the original name.
            step_name = _step_type(completed_step).__name__

            if step_name in compensated_steps:
                logger.debug(f"Skipping already compensated step: {step_name}")
                continue

            try:
                await self._storage.log_step(
                    self._saga_id,
                    step_name,
                    "compensate",
                    SagaStepStatus.STARTED,
                )

                # Compensation must run inside a live scope: under HANDLER the scope
                # that served ``act`` is already closed, so the step is resolved again
                # here with fresh scoped dependencies. Under SEND the ambient saga
                # scope is reused and the very same instance keeps running. Each
                # retry attempt opens its own HANDLER scope (see retry helper).
                await self._compensate_step_with_retry(completed_step, step_name)

                await self._storage.update_context(
                    self._saga_id,
                    self._context.to_dict(),
                )
                await self._storage.log_step(
                    self._saga_id,
                    step_name,
                    "compensate",
                    SagaStepStatus.COMPLETED,
                )
            except Exception as compensation_error:
                await self._storage.log_step(
                    self._saga_id,
                    step_name,
                    "compensate",
                    SagaStepStatus.FAILED,
                    str(compensation_error),
                )
                compensation_errors.append((step_name, compensation_error))
                continue

            # Callback only after successful step compensation; failures are not treated as step failure
            if self._on_after_compensate_step is not None:
                try:
                    await self._on_after_compensate_step()
                except Exception as callback_error:
                    logger.error(
                        "on_after_compensate_step failed (e.g. run.commit): %s",
                        callback_error,
                        exc_info=callback_error,
                    )
                    raise

        # If compensation failed after all retries
        if compensation_errors:
            for step_name, comp_error in compensation_errors:
                logger.error(
                    f"Compensation failed for step '{step_name}' after {self._retry_count} attempts. "
                    f"Error: {type(comp_error).__name__}: {comp_error}",
                    exc_info=comp_error,
                )
            # Mark as failed eventually
            await self._storage.update_status(self._saga_id, SagaStatus.FAILED)
        else:
            # All compensations completed or were skipped — mark saga as FAILED because the original forward transaction failed
            await self._storage.update_status(self._saga_id, SagaStatus.FAILED)

    @contextlib.asynccontextmanager
    async def _compensation_scope(self) -> typing.AsyncIterator[None]:
        """Open a per-step scope under HANDLER; a no-op for SEND and NONE."""
        if self._container is None:
            yield
            return
        async with handler_scope(self._container, self._scope_strategy):
            yield

    async def _resolve_in_scope(
        self,
        step: CompletedStep,
    ) -> SagaStepHandler[ContextT, typing.Any]:
        """
        Return the step instance to compensate with.

        Under HANDLER the instance that ran ``act`` holds dependencies from a
        closed scope, so a fresh instance is resolved inside the current scope.
        Skip/recovery markers (``SagaStepRef``) are always resolved because they
        are not live handler instances. ``compensate`` only receives the saga
        context, so it must not rely on state stored on the instance during ``act``.
        """
        if self._container is None:
            if isinstance(step, SagaStepRef):
                raise RuntimeError(
                    "Cannot compensate a skipped saga step without a DI container",
                )
            return step
        if self._scope_strategy == ScopeStrategy.HANDLER or isinstance(step, SagaStepRef):
            return await self._container.resolve(_step_type(step))
        return step

    async def _compensate_step_with_retry(
        self,
        completed_step: CompletedStep,
        step_name: str,
    ) -> None:
        """
        Compensate a single step with retry mechanism and exponential backoff.

        Each attempt re-enters a compensation scope and re-resolves the step so
        a failed HANDLER attempt does not reuse a closed UoW.

        Args:
            completed_step: The executed handler or a skip/recovery marker
            step_name: Name of the step as recorded in the saga history

        Raises:
            Exception: If compensation fails after all retry attempts
        """
        last_exception: Exception | None = None
        for attempt in range(1, self._retry_count + 1):
            try:
                async with self._compensation_scope():
                    step = await self._resolve_in_scope(completed_step)
                    await step.compensate(self._context)
                logger.debug(
                    f"Successfully compensated step '{step_name}' on attempt {attempt}",
                )
                return
            except Exception as e:
                last_exception = e
                if attempt < self._retry_count:
                    # Calculate exponential backoff delay
                    delay = self._retry_delay * (self._retry_backoff ** (attempt - 1))
                    logger.warning(
                        f"Compensation attempt {attempt}/{self._retry_count} failed for step '{step_name}': {e}. "
                        f"Retrying in {delay:.2f}s...",
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"Compensation failed for step '{step_name}' after {self._retry_count} attempts: {e}",
                    )

        # If we get here, all retries failed
        if last_exception:
            raise last_exception
