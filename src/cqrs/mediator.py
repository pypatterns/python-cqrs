import logging
import typing
import uuid

from cqrs.container.protocol import Container
from cqrs.container.scope import (
    ScopeStrategy,
    enter_scope,
    resolve_concurrent_event_handling,
    wrap_container,
)
from cqrs.dispatcher.event import EventDispatcher
from cqrs.dispatcher.request import RequestDispatcher
from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.dispatcher.streaming import StreamingRequestDispatcher
from cqrs.events.event import IEvent
from cqrs.events.event_emitter import EventEmitter
from cqrs.events.event_processor import EventProcessor
from cqrs.events.map import EventMap
from cqrs.middlewares.base import MiddlewareChain
from cqrs.requests.map import RequestMap, SagaMap
from cqrs.requests.request import IRequest
from cqrs.response import IResponse
from cqrs.saga.models import SagaContext
from cqrs.saga.step import SagaStepResult
from cqrs.saga.storage.memory import MemorySagaStorage
from cqrs.saga.storage.protocol import ISagaStorage

logger = logging.getLogger("cqrs")

_ResponseT = typing.TypeVar("_ResponseT", IResponse, None, covariant=True)


def _warn_on_emitter_strategy_mismatch(
    event_emitter: EventEmitter | None,
    scope_strategy: ScopeStrategy,
) -> None:
    """Warn when a hand-built emitter scopes differently than the mediator."""
    if event_emitter is None:
        return
    emitter_strategy = getattr(event_emitter, "_scope_strategy", None)
    if emitter_strategy is None or emitter_strategy == scope_strategy:
        return
    logger.warning(
        "EventEmitter scope_strategy (%s) differs from mediator scope_strategy (%s). "
        "Command and event handlers may resolve different scoped instances "
        "(e.g. separate units of work). Pass the same strategy to both, or build "
        "the mediator via cqrs.bootstrap.",
        getattr(emitter_strategy, "value", emitter_strategy),
        getattr(scope_strategy, "value", scope_strategy),
    )


class RequestMediator:
    """
    The request mediator object.

    Handles requests and processes events. Events can be processed in parallel
    (with semaphore limit) or sequentially depending on concurrent_event_handle_enable.

    Usage::

      message_broker = AMQPMessageBroker(
        dsn=f"amqp://{LOGIN}:{PASSWORD}@{HOSTNAME}/",
        queue_name="user_joined_domain",
        exchange_name="user_joined",
      )
      event_map = EventMap()
      event_map.bind(UserJoinedDomainEvent, UserJoinedDomainEventHandler)
      request_map = RequestMap()
      request_map.bind(JoinUserCommand, JoinUserCommandHandler)
      event_emitter = EventEmitter(event_map, container, message_broker)

      mediator = RequestMediator(
        request_map=request_map,
        container=container,
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
      )

      # Handles command, processes events (in parallel or sequentially),
      # and publishes events via event emitter.
      await mediator.send(join_user_command)

    """

    def __init__(
        self,
        request_map: RequestMap,
        container: Container,
        event_emitter: EventEmitter | None = None,
        middleware_chain: MiddlewareChain | None = None,
        event_map: EventMap | None = None,
        max_concurrent_event_handlers: int = 1,
        concurrent_event_handle_enable: bool | None = None,
        scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
        *,
        dispatcher_type: typing.Type[RequestDispatcher] = RequestDispatcher,
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
            request_map=request_map,  # type: ignore
            container=self._container,  # type: ignore
            middleware_chain=middleware_chain,  # type: ignore
            scope_strategy=scope_strategy,  # type: ignore
        )

    async def send(self, request: IRequest) -> _ResponseT:
        """
        Send a request and return the response.

        The return type is inferred from the request type based on the handler
        registered in the RequestMap. For proper type inference, ensure your
        RequestHandler is properly typed with RequestHandler[RequestType, ResponseType].

        Note: TypeVar usage here is intentional for type inference purposes.
        """
        if self._scope_strategy == ScopeStrategy.SEND:
            async with enter_scope(self._container):
                return await self._send_impl(request)
        return await self._send_impl(request)

    async def _send_impl(self, request: IRequest) -> _ResponseT:
        dispatch_result = await self._dispatcher.dispatch(request)
        await self._event_processor.emit_events(dispatch_result.events)
        return dispatch_result.response


class EventMediator:
    """
    The event mediator object.

    Usage::
      event_map = EventMap()
      event_map.bind(UserJoinedECSTEvent, UserJoinedECSTEventHandler)
      mediator = EventMediator(
        event_map=event_map,
        container=container
      )

      # Handles ecst and notification events.
      await mediator.send(user_joined_event)
    """

    def __init__(
        self,
        event_map: EventMap,
        container: Container,
        middleware_chain: MiddlewareChain | None = None,
        scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
        *,
        dispatcher_type: typing.Type[EventDispatcher] = EventDispatcher,
    ):
        self._container = wrap_container(container)
        self._scope_strategy = scope_strategy
        self._dispatcher = dispatcher_type(
            event_map=event_map,  # type: ignore
            container=self._container,  # type: ignore
            middleware_chain=middleware_chain,  # type: ignore
            scope_strategy=scope_strategy,  # type: ignore
        )

    async def send(self, event: IEvent) -> None:
        if self._scope_strategy == ScopeStrategy.SEND:
            async with enter_scope(self._container):
                await self._dispatcher.dispatch(event)
                return
        await self._dispatcher.dispatch(event)


class StreamingRequestMediator:
    """
    The streaming request mediator object.

    This mediator works with handlers that are generators. It processes requests
    by iterating through the generator, emitting events after each yield, and
    streaming results back to the client.

    Usage::

      message_broker = AMQPMessageBroker(
        dsn=f"amqp://{LOGIN}:{PASSWORD}@{HOSTNAME}/",
        queue_name="user_joined_domain",
        exchange_name="user_joined",
      )
      event_map = EventMap()
      event_map.bind(UserJoinedDomainEvent, UserJoinedDomainEventHandler)
      request_map = RequestMap()
      request_map.bind(ProcessItemsCommand, ProcessItemsCommandHandler)
      event_emitter = EventEmitter(event_map, container, message_broker)

      mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,
        event_emitter=event_emitter,
      )

      # Streams results and publishes events after each yield.
      async for result in mediator.stream(process_items_command):
          print(f"Processed: {result.response}")
    """

    def __init__(
        self,
        request_map: RequestMap,
        container: Container,
        event_emitter: EventEmitter | None = None,
        middleware_chain: MiddlewareChain | None = None,
        event_map: EventMap | None = None,
        max_concurrent_event_handlers: int = 1,
        concurrent_event_handle_enable: bool | None = None,
        scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
        *,
        dispatcher_type: typing.Type[StreamingRequestDispatcher] = StreamingRequestDispatcher,
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
            request_map=request_map,  # type: ignore
            container=self._container,  # type: ignore
            middleware_chain=middleware_chain,  # type: ignore
            scope_strategy=scope_strategy,  # type: ignore
        )

    def stream(
        self,
        request: IRequest,
    ) -> typing.AsyncGenerator[IResponse | None, None]:
        """
        Stream results from a generator-based handler.

        Called without await; returns an AsyncIterator consumed with async for.
        After each yield from the handler:
        1. Events are processed (in parallel with semaphore limit or sequentially
           depending on concurrent_event_handle_enable) via event dispatcher
        2. Events are emitted via the event emitter
        3. The response is yielded to the client

        The generator continues until StopIteration is raised.

        The consumer must exhaust the iterator or call ``aclose()`` /
        ``async with contextlib.aclosing(...)``. An abandoned SEND stream keeps
        the UoW alive until the generator is garbage-collected.
        """
        return self._stream_impl(request)

    async def _stream_impl(
        self,
        request: IRequest,
    ) -> typing.AsyncGenerator[IResponse | None, None]:
        # Scope must live inside the generator body so exit/finalization runs
        # when the generator is exhausted or aclosed (not when stream() returns).
        if self._scope_strategy == ScopeStrategy.SEND:
            async with enter_scope(self._container):
                async for response in self._iterate_stream(request):
                    yield response
            return
        async for response in self._iterate_stream(request):
            yield response

    async def _iterate_stream(
        self,
        request: IRequest,
    ) -> typing.AsyncGenerator[IResponse | None, None]:
        async for dispatch_result in self._dispatcher.dispatch(request):
            await self._event_processor.emit_events(dispatch_result.events)
            yield dispatch_result.response


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
      async for result in mediator.stream(order_context):
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

    def stream(
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
        return self._stream_impl(context, saga_id=saga_id)

    async def _stream_impl(
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
