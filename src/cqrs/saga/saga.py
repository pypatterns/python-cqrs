import dataclasses
import logging
import types
import typing
import uuid

from cqrs.container.protocol import Container
from cqrs.saga.compensation import SagaCompensator
from cqrs.saga.execution import (
    FallbackStepExecutor,
    SagaRecoveryManager,
    SagaStateManager,
    SagaStepExecutor,
)
from cqrs.saga.fallback import Fallback
from cqrs.saga.models import ContextT
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.protocol import ISagaStorage, SagaStorageRun
from cqrs.saga.validation import (
    SagaContextTypeExtractor,
    SagaStepValidator,
)

logger = logging.getLogger("cqrs.saga")


class SagaTransaction(typing.Generic[ContextT]):
    """
    Context manager for saga transaction execution.

    Provides an async iterable interface for executing saga steps with automatic
    compensation on failure. Each iteration yields the result of a step's act method.

    Recovery Strategy: Strict Backward Recovery
    --------------------------------------------
    This implementation follows the "Strict Backward Recovery" pattern, which ensures
    data consistency in distributed systems. The key principle is:

    **Point of No Return**: Once a saga enters COMPENSATING or FAILED status,
    forward execution is permanently disabled. Only compensation can proceed.

    Why this matters:
    - Prevents "zombie states" where a saga is partially compensated and partially executed
    - Ensures eventual consistency: if we started rolling back, we must finish rolling back
    - Avoids scenarios where compensation actions (e.g., refund) conflict with new execution
      attempts (e.g., retry of failed step)

    Example scenario:
        Step A (Reserve inventory) - Success
        Step B (Charge payment) - Success
        Step C (Ship order) - Fails
        Saga enters COMPENSATING status
        Compensation C - Success (nothing to undo)
        System crashes during compensation B

        On recovery:
        - Status is COMPENSATING
        - We MUST complete compensation (refund payment, release inventory)
        - We MUST NOT retry Step C, even if network is now available
        - This prevents: client gets free product (refunded + shipped)

    Usage::

        async with saga.transaction(context=OrderContext(order_id="123")) as transaction:
            async for step_result in transaction:
                # Process step_result
                pass
    """

    def __init__(
        self,
        saga: "Saga[ContextT]",
        context: ContextT,
        container: Container,
        storage: ISagaStorage,
        saga_id: typing.Optional[uuid.UUID] = None,
        compensation_retry_count: int = 3,
        compensation_retry_delay: float = 1.0,
        compensation_retry_backoff: float = 2.0,
    ):
        self._saga = saga
        self._context = context
        self._container = container
        self._storage = storage
        self._completed_steps: list[SagaStepHandler[ContextT, typing.Any]] = []
        self._error: typing.Optional[BaseException] = None
        self._compensated: bool = False
        self._comp_retry_count = compensation_retry_count
        self._comp_retry_delay = compensation_retry_delay
        self._comp_retry_backoff = compensation_retry_backoff

        self._saga_id = saga_id or uuid.uuid4()
        self._is_new_saga = saga_id is None

        # Initialize components
        self._state_manager = SagaStateManager(self._saga_id, storage)
        self._recovery_manager = SagaRecoveryManager(
            self._saga_id,
            storage,
            container,
            saga.steps,
        )
        self._step_executor: SagaStepExecutor[ContextT] = SagaStepExecutor[ContextT](
            context,
            container,
            self._state_manager,
        )
        self._fallback_executor: FallbackStepExecutor[ContextT] = FallbackStepExecutor[ContextT](
            context,
            container,
            self._state_manager,
        )
        self._compensator: SagaCompensator[ContextT] = SagaCompensator[ContextT](
            self._saga_id,
            context,
            storage,
            self._comp_retry_count,
            self._comp_retry_delay,
            self._comp_retry_backoff,
        )

    @property
    def saga_id(self) -> uuid.UUID:
        return self._saga_id

    @property
    def completed_steps(self) -> list[SagaStepHandler[ContextT, typing.Any]]:
        """
        Get list of completed step handlers.

        Returns:
            List of step handlers that have been executed successfully.
            Can be used to collect events from steps.
        """
        return self._completed_steps

    async def __aenter__(self) -> "SagaTransaction[ContextT]":
        return self

    async def __aexit__(
        self,
        exc_type: typing.Optional[type[BaseException]],
        exc_val: typing.Optional[BaseException],
        exc_tb: typing.Optional[types.TracebackType],
    ) -> bool:
        # If an exception occurred, compensate all completed steps.
        # Do not compensate on GeneratorExit: consumer stopped iteration intentionally
        # (e.g. to resume later), which is not a failure.
        if exc_val is not None and exc_type is not GeneratorExit and not self._compensated:
            self._error = exc_val
            await self._compensate()
        return False  # Don't suppress the exception

    async def __aiter__(
        self,
    ) -> typing.AsyncIterator[SagaStepResult[ContextT, typing.Any]]:
        """
        Execute saga steps sequentially and yield each step result.

        Implements the Strict Backward Recovery strategy: if the saga is in COMPENSATING or FAILED status, forward execution is never resumed. When the underlying storage provides create_run(), execution is performed within a per-saga run with checkpoint commits; otherwise the legacy run-less path is used.
        Returns:
                AsyncIterator[SagaStepResult[ContextT, typing.Any]]: An async iterator that yields the result for each executed saga step in order.
        """
        try:
            run_cm = self._storage.create_run()
        except NotImplementedError:
            run_cm = None

        if run_cm is not None:
            async with run_cm as run:
                async for step_result in self._execute(run):
                    yield step_result
        else:
            async for step_result in self._execute(None):
                yield step_result

    def _build_run_scoped_components(
        self,
        run: SagaStorageRun,
    ) -> tuple[
        SagaStateManager,
        SagaRecoveryManager,
        SagaStepExecutor[ContextT],
        FallbackStepExecutor[ContextT],
        SagaCompensator[ContextT],
    ]:
        """Build state manager, recovery manager, executors, and compensator for a storage run (checkpoint commits)."""
        state_manager = SagaStateManager(self._saga_id, run)
        recovery_manager = SagaRecoveryManager(
            self._saga_id,
            run,
            self._container,
            self._saga.steps,
        )
        step_executor = SagaStepExecutor(
            self._context,
            self._container,
            state_manager,
        )
        fallback_executor = FallbackStepExecutor(
            self._context,
            self._container,
            state_manager,
        )
        compensator = SagaCompensator(
            self._saga_id,
            self._context,
            run,
            self._comp_retry_count,
            self._comp_retry_delay,
            self._comp_retry_backoff,
            on_after_compensate_step=run.commit,
        )
        return (
            state_manager,
            recovery_manager,
            step_executor,
            fallback_executor,
            compensator,
        )

    async def _execute(
        self,
        run: typing.Optional[SagaStorageRun],
    ) -> typing.AsyncIterator[SagaStepResult[ContextT, typing.Any]]:
        """
        Execute the saga's configured steps, using the provided storage run for checkpointed operations when available, and perform recovery and compensation as required.

        Parameters:
            run (typing.Optional[SagaStorageRun ]): Optional per-saga storage run. When provided, the run is used for loading saga state, creating run-scoped managers/executors, and committing at checkpoint boundaries. When None, the transaction's internal managers and executors are used.

        Returns:
            Async iterator that yields SagaStepResult values for each step that completes; each yielded result will include the transaction's saga_id.

        Raises:
            RuntimeError: If the saga was recovered in COMPENSATING or FAILED state and compensation was completed, forward execution is not allowed.
        """
        if run is not None:
            (
                state_manager,
                recovery_manager,
                step_executor,
                fallback_executor,
                compensator,
            ) = self._build_run_scoped_components(run)
        else:
            state_manager = self._state_manager
            recovery_manager = self._recovery_manager
            step_executor = self._step_executor
            fallback_executor = self._fallback_executor
            compensator = self._compensator

        completed_step_names: set[str] = set()

        if self._is_new_saga:
            await state_manager.create_saga(
                self._saga.__class__.__name__,
                self._context,
            )
            if run is not None:
                await run.commit()
            await state_manager.update_status(SagaStatus.RUNNING)
            if run is not None:
                await run.commit()
        else:
            try:
                if run is not None:
                    status, _, _ = await run.load_saga_state(
                        self._saga_id,
                        read_for_update=True,
                    )
                else:
                    status, _, _ = await self._storage.load_saga_state(
                        self._saga_id,
                        read_for_update=True,
                    )

                if status == SagaStatus.COMPLETED:
                    logger.info(
                        f"Saga {self._saga_id} is already {status}. Skipping execution.",
                    )
                    return

                if status in (SagaStatus.COMPENSATING, SagaStatus.FAILED):
                    logger.warning(
                        f"Saga {self._saga_id} is in {status} state. " "Resuming compensation immediately.",
                    )
                    completed_act_steps = await recovery_manager.load_completed_step_names()
                    reconstructed_steps = await recovery_manager.reconstruct_completed_steps(
                        completed_act_steps,
                    )
                    self._completed_steps = [
                        typing.cast(SagaStepHandler[ContextT, typing.Any], step) for step in reconstructed_steps
                    ]
                    if not self._completed_steps:
                        logger.warning(
                            f"Saga {self._saga_id}: no completed steps to compensate "
                            "(saga failed before any step finished 'act', or step names in "
                            "storage do not match saga step class names). "
                            "Marking as FAILED without calling compensate().",
                        )
                    await compensator.compensate_steps(self._completed_steps)
                    if run is not None:
                        await run.commit()
                    raise RuntimeError(
                        f"Saga {self._saga_id} was recovered in {status} state "
                        "and compensation was completed. Forward execution is not allowed.",
                    )

                completed_step_names = await recovery_manager.load_completed_step_names()
            except ValueError:
                if run is not None:
                    await run.rollback()
                await state_manager.create_saga(
                    self._saga.__class__.__name__,
                    self._context,
                )
                if run is not None:
                    await run.commit()
                await state_manager.update_status(SagaStatus.RUNNING)
                if run is not None:
                    await run.commit()

        step_name = "unknown_step"
        try:
            for step_item in self._saga.steps:
                if isinstance(step_item, Fallback):
                    fallback_result = await fallback_executor.execute_fallback_step(
                        step_item,
                        completed_step_names,
                    )
                    if fallback_result is not None:
                        step_result, executed_step = fallback_result
                        self._completed_steps.append(executed_step)
                        if run is not None:
                            await run.commit()
                        yield dataclasses.replace(
                            step_result,
                            saga_id=self._saga_id,
                        )
                    else:
                        primary_name = step_item.step.__name__
                        fallback_name = step_item.fallback.__name__
                        if primary_name in completed_step_names:
                            step = await self._container.resolve(step_item.step)
                            self._completed_steps.append(step)
                        elif fallback_name in completed_step_names:
                            step = await self._container.resolve(step_item.fallback)
                            self._completed_steps.append(step)
                    continue

                step_type = step_item
                step_name = step_type.__name__

                if step_name in completed_step_names:
                    step = await self._container.resolve(step_type)
                    self._completed_steps.append(step)
                    logger.debug(f"Skipping already completed step: {step_name}")
                    continue

                step_result = await step_executor.execute_step(
                    step_type,
                    step_name,
                )
                step = await self._container.resolve(step_type)
                self._completed_steps.append(step)
                if run is not None:
                    await run.commit()
                yield dataclasses.replace(
                    step_result,
                    saga_id=self._saga_id,
                )

            await state_manager.update_context(self._context)
            await state_manager.update_status(SagaStatus.COMPLETED)
            if run is not None:
                await run.commit()

        except Exception as e:
            await state_manager.log_step(
                step_name,
                "act",
                SagaStepStatus.FAILED,
                str(e),
            )
            if run is not None:
                await run.commit()
            self._error = e
            self._compensated = True
            await compensator.compensate_steps(self._completed_steps)
            if run is not None:
                await run.commit()
            raise

    async def _compensate(self) -> None:
        """
        Mark the transaction as compensated and run compensation for all completed steps in reverse order.

        Sets an internal flag to prevent repeated compensation and delegates to the compensator which applies the configured retry behavior.
        """
        # Prevent double compensation
        if self._compensated:
            return

        self._compensated = True
        await self._compensator.compensate_steps(self._completed_steps)


class Saga(typing.Generic[ContextT]):
    """
    Saga class that defines steps and context type.

    Saga is a simple class that serves as a container for:
    - List of step handler types to execute in order
    - Context type (via Generic parameter)

    Saga does not depend on DI container or storage. All work with dependencies
    and storage is handled by SagaTransaction class.

    Steps must be defined as a class attribute. All steps must handle the same
    context type as specified in the Generic parameter.

    Usage::

        class OrderSaga(Saga[OrderContext]):
            steps = [
                ReserveInventoryStep,
                ProcessPaymentStep,
                ShipOrderStep,
            ]

        saga = OrderSaga()

    Note:
        Steps are validated at class creation time via __init_subclass__ to ensure
        they handle the correct context type.
    """

    steps: typing.ClassVar[list[typing.Union[type[SagaStepHandler], Fallback]]] = []

    def __init_subclass__(cls, **kwargs: typing.Any) -> None:
        """Validate steps when subclass is created."""
        super().__init_subclass__(**kwargs)
        cls._validate_steps()

    @classmethod
    def _validate_steps(cls) -> None:
        """
        Validate saga steps.

        Ensures that:
        1. Steps is a list
        2. All steps are valid step handler types or Fallback instances
        3. All steps handle the correct context type
        """
        # Extract context type from Generic parameter
        context_type = SagaContextTypeExtractor.extract_from_class(cls, Saga)

        # Create validator and validate steps
        validator = SagaStepValidator(
            saga_name=cls.__name__,
            context_type=context_type,
        )
        validator.validate_steps(cls.steps)

    @property
    def steps_count(self) -> int:
        """
        Return the number of steps in the saga.

        Returns:
            The total number of steps configured for this saga.
        """
        return len(self.steps)

    def transaction(
        self,
        context: ContextT,
        container: Container,
        storage: ISagaStorage,
        saga_id: typing.Optional[uuid.UUID] = None,
        compensation_retry_count: int = 3,
        compensation_retry_delay: float = 1.0,
        compensation_retry_backoff: float = 2.0,
    ) -> SagaTransaction[ContextT]:
        """
        Create a transaction context manager for saga execution.

        The transaction provides an async iterable interface where each iteration
        yields the result of a step's act method. If any step fails or an exception
        occurs, all completed steps are automatically compensated.

        Args:
            context: The saga context object that contains shared state
                    across all steps in the saga.
            container: DI container for resolving step handlers
            storage: Saga storage implementation
            saga_id: Optional UUID for the saga. If provided, it can be used
                     for recovery or ensuring idempotency.
            compensation_retry_count: Number of retry attempts for compensation (default: 3)
            compensation_retry_delay: Initial delay between retries in seconds (default: 1.0)
            compensation_retry_backoff: Backoff multiplier for exponential delay (default: 2.0)

        Returns:
            A SagaTransaction context manager that can be used in an async with statement.

        Usage::

            async with saga.transaction(
                context=OrderContext(order_id="123"),
                container=container,
                storage=storage,
            ) as transaction:
                async for step_result in transaction:
                    # Process step_result
                    pass
        """
        return SagaTransaction(
            saga=self,
            context=context,
            container=container,
            storage=storage,
            saga_id=saga_id,
            compensation_retry_count=compensation_retry_count,
            compensation_retry_delay=compensation_retry_delay,
            compensation_retry_backoff=compensation_retry_backoff,
        )
