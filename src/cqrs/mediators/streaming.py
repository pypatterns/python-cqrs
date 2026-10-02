import typing

from cqrs.container.protocol import Container
from cqrs.container.scope import (
    ScopeStrategy,
    enter_scope,
    resolve_concurrent_event_handling,
    wrap_container,
)
from cqrs.dispatcher.streaming import StreamingRequestDispatcher
from cqrs.events.event_emitter import EventEmitter
from cqrs.events.event_processor import EventProcessor
from cqrs.mapping.events import EventMap
from cqrs.mapping.requests import RequestMap
from cqrs.mediators._common import _warn_on_emitter_strategy_mismatch
from cqrs.middlewares.base import MiddlewareChain
from cqrs.models.request import IRequest
from cqrs.models.response import IResponse


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
