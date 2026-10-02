import typing
import uuid

from cqrs.container.protocol import Container
from cqrs.container.scope import (
    ScopeStrategy,
    enter_scope,
    resolve_concurrent_event_handling,
    wrap_container,
)
from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.events.event_emitter import EventEmitter
from cqrs.events.event_processor import EventProcessor
from cqrs.handlers.saga import SagaStepResult
from cqrs.mapping.events import EventMap
from cqrs.mapping.requests import SagaMap
from cqrs.mediators._common import _warn_on_emitter_strategy_mismatch
from cqrs.middlewares.base import MiddlewareChain
from cqrs.saga.models import SagaContext
from cqrs.saga.storage.memory import MemorySagaStorage
from cqrs.saga.storage.protocol import ISagaStorage


class SagaMediator:
    """
    The saga mediator object.

    Handles saga execution by finding the appropriate saga for a given SagaContext
    and executing it. Events produced by saga steps can be processed and emitted.

    This mediator works with saga transactions that yield results after each step.
    It processes events after each yield, emits events via event emitter, and
    streams results back to the client.

    Usage::

      saga_map = SagaMap()
      saga_map.bind(OrderContext, OrderSaga)
      event_map = EventMap()
      event_map.bind(InventoryReservedEvent, InventoryReservedEventHandler)
      event_emitter = EventEmitter(event_map, container, message_broker)

      mediator = SagaMediator(
        saga_map=saga_map,
        container=container,
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
      )

      # Streams results and publishes events after each step.
      async for result in mediator.execute(order_context):
          print(f"Step completed: {result.step_result.step_type.__name__}")
    """

    def __init__(
        self,
        saga_map: SagaMap,
        container: Container,
        event_emitter: EventEmitter | None = None,
        middleware_chain: MiddlewareChain | None = None,
        event_map: EventMap | None = None,
        max_concurrent_event_handlers: int = 1,
        concurrent_event_handle_enable: bool | None = None,
        storage: ISagaStorage | None = None,
        compensation_retry_count: int = 3,
        compensation_retry_delay: float = 1.0,
        compensation_retry_backoff: float = 2.0,
        scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
        *,
        dispatcher_type: typing.Type[SagaDispatcher] = SagaDispatcher,
    ) -> None:
        concurrent_event_handle_enable = resolve_concurrent_event_handling(
            scope_strategy,
            concurrent_event_handle_enable,
        )
        self._container = wrap_container(container)
        self._scope_strategy = scope_strategy
        _warn_on_emitter_strategy_mismatch(event_emitter, scope_strategy)
        self._event_processor = EventProcessor(
            event_map=event_map or EventMap(),
            event_emitter=event_emitter,
            max_concurrent_event_handlers=max_concurrent_event_handlers,
            concurrent_event_handle_enable=concurrent_event_handle_enable,
        )
        self._dispatcher = dispatcher_type(
            saga_map=saga_map,  # type: ignore
            container=self._container,  # type: ignore
            storage=storage or MemorySagaStorage(),  # type: ignore
            middleware_chain=middleware_chain,  # type: ignore
            compensation_retry_count=compensation_retry_count,  # type: ignore
            compensation_retry_delay=compensation_retry_delay,  # type: ignore
            compensation_retry_backoff=compensation_retry_backoff,  # type: ignore
            scope_strategy=scope_strategy,  # type: ignore
        )

    def execute(
        self,
        context: SagaContext,
        saga_id: uuid.UUID | None = None,
    ) -> typing.AsyncIterator[SagaStepResult]:
        """
        Stream results from saga execution.

        Called without await; returns an AsyncIterator consumed with async for.
        After each step execution:
        1. Events are processed (in parallel with semaphore limit or sequentially
           depending on concurrent_event_handle_enable) via event dispatcher
        2. Events are emitted via the event emitter
        3. The dispatch result is yielded to the client

        The generator continues until all saga steps are completed or an exception occurs.

        Args:
            context: The saga context object
            saga_id: Optional UUID for the saga. If provided, can be used
                     for recovery or ensuring idempotency.

        Yields:
            SagaStepResult

        The consumer must exhaust the iterator or call ``aclose()`` /
        ``async with contextlib.aclosing(...)``. An abandoned SEND stream keeps
        the UoW alive until the generator is garbage-collected.
        """
        return self._execute_impl(context, saga_id=saga_id)

    async def _execute_impl(
        self,
        context: SagaContext,
        saga_id: uuid.UUID | None = None,
    ) -> typing.AsyncIterator[SagaStepResult]:
        if self._scope_strategy == ScopeStrategy.SEND:
            async with enter_scope(self._container):
                async for result in self._iterate_saga(context, saga_id=saga_id):
                    yield result
            return
        async for result in self._iterate_saga(context, saga_id=saga_id):
            yield result

    async def _iterate_saga(
        self,
        context: SagaContext,
        saga_id: uuid.UUID | None = None,
    ) -> typing.AsyncIterator[SagaStepResult]:
        async for dispatch_result in self._dispatcher.dispatch(
            context,
            saga_id=saga_id,
        ):
            await self._event_processor.emit_events(dispatch_result.events)
            yield dispatch_result.step_result
