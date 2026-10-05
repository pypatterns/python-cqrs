import typing

from cqrs.container.protocol import Container
from cqrs.container.scope import ScopeStrategy, enter_scope, wrap_container
from cqrs.dispatcher.event import EventDispatcher
from cqrs.mapping.events import EventMap
from cqrs.middlewares.base import MiddlewareChain
from cqrs.models.event import IEvent


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
