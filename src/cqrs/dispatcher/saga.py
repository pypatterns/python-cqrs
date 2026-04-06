import logging
import typing
import uuid

from cqrs.container.protocol import Container
from cqrs.dispatcher.exceptions import SagaDoesNotExist
from cqrs.dispatcher.models import SagaDispatchResult
from cqrs.middlewares.base import MiddlewareChain
from cqrs.requests.map import SagaMap
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import SagaTransaction
from cqrs.saga.storage.memory import MemorySagaStorage
from cqrs.saga.storage.protocol import ISagaStorage

logger = logging.getLogger("cqrs")


class SagaDispatcher:
    """
    Dispatcher for saga execution.

    Finds the appropriate saga for a given SagaContext type and executes it.
    Yields result after each step execution.
    """

    def __init__(
        self,
        saga_map: SagaMap,
        container: Container,
        storage: typing.Optional[ISagaStorage] = None,
        middleware_chain: typing.Optional[MiddlewareChain] = None,
        compensation_retry_count: int = 3,
        compensation_retry_delay: float = 1.0,
        compensation_retry_backoff: float = 2.0,
    ) -> None:
        """
        Initialize saga dispatcher.

        Args:
            saga_map: Map of SagaContext types to Saga types
            container: DI container for resolving saga instances
            storage: Saga storage implementation. Defaults to MemorySagaStorage.
            middleware_chain: Optional middleware chain for saga processing
            compensation_retry_count: Number of retry attempts for compensation (default: 3)
            compensation_retry_delay: Initial delay between retries in seconds (default: 1.0)
            compensation_retry_backoff: Backoff multiplier for exponential delay (default: 2.0)
        """
        self._saga_map = saga_map
        self._container = container
        self._storage = storage or MemorySagaStorage()
        self._middleware_chain = middleware_chain or MiddlewareChain()
        self._compensation_retry_count = compensation_retry_count
        self._compensation_retry_delay = compensation_retry_delay
        self._compensation_retry_backoff = compensation_retry_backoff

    def dispatch(
        self,
        context: SagaContext,
        saga_id: typing.Optional[uuid.UUID] = None,
    ) -> typing.AsyncIterator[SagaDispatchResult]:
        """
        Dispatch a saga execution for the given context.

        Called without await; returns an AsyncIterator consumed with async for.
        Yields result after each step execution. After each yield, events are collected
        and included in the dispatch result.

        Args:
            context: The saga context object
            saga_id: Optional UUID for the saga. If provided, can be used
                     for recovery or ensuring idempotency.

        Yields:
            SagaDispatchResult containing step result, events, and saga_id for each step

        Raises:
            SagaDoesNotExist: If no saga is registered for the context type
        """
        return self._dispatch_impl(context, saga_id=saga_id)

    async def _dispatch_impl(
        self,
        context: SagaContext,
        saga_id: typing.Optional[uuid.UUID] = None,
    ) -> typing.AsyncIterator[SagaDispatchResult]:
        # Find saga type by context type
        saga_type = self._saga_map.get(type(context))
        if not saga_type:
            raise SagaDoesNotExist(
                f"Saga not found matching SagaContext type {type(context)}",
            )

        # Resolve saga instance from container
        saga = await self._container.resolve(saga_type)

        # Create saga transaction with container and storage
        transaction = SagaTransaction(
            saga=saga,
            context=context,
            container=self._container,
            storage=self._storage,
            saga_id=saga_id,
            compensation_retry_count=self._compensation_retry_count,
            compensation_retry_delay=self._compensation_retry_delay,
            compensation_retry_backoff=self._compensation_retry_backoff,
        )

        # Execute saga transaction
        async with transaction:
            async for step_result in transaction:
                # Collect events from the current step handler
                step_events = []
                try:
                    # Get the step instance that was just executed
                    # It should be the last one in completed_steps
                    if transaction.completed_steps:
                        step_instance = transaction.completed_steps[-1]
                        if hasattr(step_instance, "events"):
                            step_events_list = step_instance.events
                            if isinstance(step_events_list, list):
                                step_events.extend(step_events_list)
                except Exception as e:
                    logger.warning(
                        f"Failed to collect events from step {step_result.step_type.__name__}: {e}",
                    )

                yield SagaDispatchResult(
                    step_result=step_result,
                    events=step_events,
                    saga_id=str(transaction.saga_id),
                )
