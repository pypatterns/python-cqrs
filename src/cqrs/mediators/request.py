import typing

from cqrs.container.protocol import Container
from cqrs.container.scope import (
    ScopeStrategy,
    enter_scope,
    resolve_concurrent_event_handling,
    wrap_container,
)
from cqrs.dispatcher.request import RequestDispatcher
from cqrs.events.event_emitter import EventEmitter
from cqrs.events.event_processor import EventProcessor
from cqrs.mapping.events import EventMap
from cqrs.mapping.requests import RequestMap
from cqrs.mediators._common import _warn_on_emitter_strategy_mismatch
from cqrs.middlewares.base import MiddlewareChain
from cqrs.models.request import IRequest
from cqrs.models.response import IResponse

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
