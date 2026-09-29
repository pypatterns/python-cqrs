import contextlib
import logging
import typing
import uuid

from cqrs.container.protocol import Container
from cqrs.container.scope import ScopeStrategy, enter_scope
from cqrs.saga.models import ContextT
from cqrs.saga.saga import Saga
from cqrs.saga.storage.enums import SagaStatus
from cqrs.saga.storage.protocol import ISagaStorage

logger = logging.getLogger(__name__)


async def recover_saga(
    saga: Saga[ContextT],
    saga_id: uuid.UUID,
    context_builder: typing.Union[
        typing.Type[ContextT],
        typing.Callable[[dict[str, typing.Any]], ContextT],
    ],
    container: Container,
    storage: ISagaStorage,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> None:
    """
    Recover and complete a potentially interrupted saga.

    This function attempts to resume a saga from where it left off.
    It relies on the storage to load the last known state and execution history.
    Already completed steps will be skipped.
    If the saga was in a compensating state, compensation will resume.

    On recovery failure (exception during resume), the storage's
    increment_recovery_attempts is called automatically so the saga can be
    retried or excluded by get_sagas_for_recovery(max_recovery_attempts=...).
    Callers do not need to call increment_recovery_attempts themselves.

    Args:
        saga: The saga orchestrator instance.
        saga_id: The ID of the saga to recover.
        context_builder: A class type or factory function to reconstruct the context.
                        If a class is provided, it will be instantiated with **data
                        (assuming the constructor accepts kwargs).
                        If a function is provided, it will be called with the data dict.
                        Examples:
                            - MyContextClass.from_dict (if implements from_dict interface)
                            - lambda d: MyDataClass(**d)
                            - MyClass (if __init__ accepts **kwargs)
        container: DI container for resolving step handlers.
        storage: Saga storage implementation.
        scope_strategy: Same strategy as the original run. Pass ``SEND`` or
            ``HANDLER`` so recovery re-opens the matching DI boundary; with
            ``SEND`` this function opens the scope itself. A plain container
            is enough (no manual ``ScopeAwareContainer``). Do not call
            ``recover_saga`` from inside an already-open ``send()`` SEND scope
            unless you intend to join that UoW.
    """
    # 1. Load state
    try:
        status, context_data, _ = await storage.load_saga_state(saga_id)
    except Exception as e:
        logger.error(f"Failed to load saga {saga_id}: {e}")
        raise

    # Check for terminal states
    # COMPLETED: saga finished successfully, nothing to do
    # FAILED: saga failed and compensation was completed, but we still need to
    #         check if there are any remaining compensation steps to complete
    #         (in case of crash during compensation)
    if status == SagaStatus.COMPLETED:
        logger.info(f"Saga {saga_id} is already completed. Nothing to recover.")
        return

    # 2. Reconstruct context
    try:
        if isinstance(context_builder, type):
            # It's a class, check if it has from_dict method (SagaContext)
            if hasattr(context_builder, "from_dict"):
                context = context_builder.from_dict(context_data)
            else:
                # Fallback: assume constructor accepts kwargs
                context = context_builder(**context_data)
        else:
            # It's a factory function/method
            callable_builder = typing.cast(
                typing.Callable[[dict[str, typing.Any]], ContextT],
                context_builder,
            )
            context = callable_builder(context_data)
    except Exception as e:
        logger.error(f"Failed to reconstruct context for saga {saga_id}: {e}")
        raise

    logger.info(f"Recovering saga {saga_id} (status: {status})...")

    # 3. Resume execution
    # The transaction logic handles skipping completed steps and resuming compensation.
    @contextlib.asynccontextmanager
    async def _recovery_scope() -> typing.AsyncIterator[None]:
        if scope_strategy == ScopeStrategy.SEND:
            async with enter_scope(container):
                yield
            return
        yield

    try:
        async with _recovery_scope():
            async with saga.transaction(
                context=typing.cast(ContextT, context),
                container=container,
                storage=storage,
                saga_id=saga_id,
                scope_strategy=scope_strategy,
            ) as transaction:
                async for step_result in transaction:
                    logger.info(
                        f"Recovered/Executed step: {step_result.step_type.__name__}",
                    )

        logger.info(f"Saga {saga_id} recovery completed successfully.")

    except RuntimeError as e:
        # RuntimeError is raised when saga is recovered in COMPENSATING/FAILED state
        # This is expected behavior - compensation was completed, but we signal
        # that forward execution was not allowed
        error_msg = str(e)
        if "recovered in" in error_msg and "state" in error_msg:
            logger.warning(
                f"Saga {saga_id} recovery completed compensation. " "Forward execution was not allowed.",
            )
            # Re-raise to allow callers to handle this case
            raise
        # For other RuntimeErrors, recovery failed: increment attempts and re-raise
        logger.error(f"Saga {saga_id} recovery ended with error: {e}")
        await storage.increment_recovery_attempts(saga_id, new_status=SagaStatus.FAILED)
        raise
    except Exception as e:
        logger.error(f"Saga {saga_id} recovery ended with error: {e}")
        # Recovery failed: increment attempts so saga can be retried or excluded later
        await storage.increment_recovery_attempts(saga_id, new_status=SagaStatus.FAILED)
        raise
