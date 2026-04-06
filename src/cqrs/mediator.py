import typing
import uuid

from cqrs.container.protocol import Container
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

_ResponseT = typing.TypeVar("_ResponseT", IResponse, None, covariant=True)


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
        event_emitter: typing.Optional[EventEmitter] = None,
        middleware_chain: typing.Optional[MiddlewareChain] = None,
        event_map: typing.Optional[EventMap] = None,
        max_concurrent_event_handlers: int = 1,
        concurrent_event_handle_enable: bool = True,
        *,
        dispatcher_type: typing.Type[RequestDispatcher] = RequestDispatcher,
    ) -> None:
        self._event_processor = EventProcessor(
            event_map=event_map or EventMap(),
            event_emitter=event_emitter,
            max_concurrent_event_handlers=max_concurrent_event_handlers,
            concurrent_event_handle_enable=concurrent_event_handle_enable,
        )
        self._dispatcher = dispatcher_type(
            request_map=request_map,  # type: ignore
            container=container,  # type: ignore
            middleware_chain=middleware_chain,  # type: ignore
        )

    async def send(self, request: IRequest) -> _ResponseT:
        """
        Send a request and return the response.

        The return type is inferred from the request type based on the handler
        registered in the RequestMap. For proper type inference, ensure your
        RequestHandler is properly typed with RequestHandler[RequestType, ResponseType].

        Note: TypeVar usage here is intentional for type inference purposes.
        """
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
        middleware_chain: typing.Optional[MiddlewareChain] = None,
        *,
        dispatcher_type: typing.Type[EventDispatcher] = EventDispatcher,
    ):
        self._dispatcher = dispatcher_type(
            event_map=event_map,  # type: ignore
            container=container,  # type: ignore
            middleware_chain=middleware_chain,  # type: ignore
        )

    async def send(self, event: IEvent) -> None:
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
        event_emitter: typing.Optional[EventEmitter] = None,
        middleware_chain: typing.Optional[MiddlewareChain] = None,
        event_map: typing.Optional[EventMap] = None,
        max_concurrent_event_handlers: int = 1,
        concurrent_event_handle_enable: bool = True,
        *,
        dispatcher_type: typing.Type[StreamingRequestDispatcher] = StreamingRequestDispatcher,
    ) -> None:
        self._event_processor = EventProcessor(
            event_map=event_map or EventMap(),
            event_emitter=event_emitter,
            max_concurrent_event_handlers=max_concurrent_event_handlers,
            concurrent_event_handle_enable=concurrent_event_handle_enable,
        )
        self._dispatcher = dispatcher_type(
            request_map=request_map,  # type: ignore
            container=container,  # type: ignore
            middleware_chain=middleware_chain,  # type: ignore
        )

    def stream(
        self,
        request: IRequest,
    ) -> typing.AsyncIterator[IResponse]:
        """
        Stream results from a generator-based handler.

        Called without await; returns an AsyncIterator consumed with async for.
        After each yield from the handler:
        1. Events are processed (in parallel with semaphore limit or sequentially
           depending on concurrent_event_handle_enable) via event dispatcher
        2. Events are emitted via the event emitter
        3. The response is yielded to the client

        The generator continues until StopIteration is raised.
        """
        return self._stream_impl(request)

    async def _stream_impl(
        self,
        request: IRequest,
    ) -> typing.AsyncIterator[IResponse]:
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
        event_emitter: typing.Optional[EventEmitter] = None,
        middleware_chain: typing.Optional[MiddlewareChain] = None,
        event_map: typing.Optional[EventMap] = None,
        max_concurrent_event_handlers: int = 1,
        concurrent_event_handle_enable: bool = True,
        storage: typing.Optional[ISagaStorage] = None,
        compensation_retry_count: int = 3,
        compensation_retry_delay: float = 1.0,
        compensation_retry_backoff: float = 2.0,
        *,
        dispatcher_type: typing.Type[SagaDispatcher] = SagaDispatcher,
    ) -> None:
        self._event_processor = EventProcessor(
            event_map=event_map or EventMap(),
            event_emitter=event_emitter,
            max_concurrent_event_handlers=max_concurrent_event_handlers,
            concurrent_event_handle_enable=concurrent_event_handle_enable,
        )
        self._dispatcher = dispatcher_type(
            saga_map=saga_map,  # type: ignore
            container=container,  # type: ignore
            storage=storage or MemorySagaStorage(),  # type: ignore
            middleware_chain=middleware_chain,  # type: ignore
            compensation_retry_count=compensation_retry_count,  # type: ignore
            compensation_retry_delay=compensation_retry_delay,  # type: ignore
            compensation_retry_backoff=compensation_retry_backoff,  # type: ignore
        )

    def stream(
        self,
        context: SagaContext,
        saga_id: typing.Optional[uuid.UUID] = None,
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
        """
        return self._stream_impl(context, saga_id=saga_id)

    async def _stream_impl(
        self,
        context: SagaContext,
        saga_id: typing.Optional[uuid.UUID] = None,
    ) -> typing.AsyncIterator[SagaStepResult]:
        async for dispatch_result in self._dispatcher.dispatch(
            context,
            saga_id=saga_id,
        ):
            await self._event_processor.emit_events(dispatch_result.events)
            yield dispatch_result.step_result
