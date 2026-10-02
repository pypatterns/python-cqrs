import logging
import typing

from cqrs.circuit_breaker import should_use_fallback
from cqrs.container.protocol import Container
from cqrs.container.scope import (
    ScopeStrategy,
    _Fallback,
    _run_with_fallback_scope,
    handler_scope,
)
from cqrs.models.event import IEvent
from cqrs.handlers.event import EventHandler
from cqrs.events.fallback import EventHandlerFallback
from cqrs.mapping.events import EventMap
from cqrs.middlewares.base import MiddlewareChain

_EventHandler: typing.TypeAlias = EventHandler

logger = logging.getLogger("cqrs")


class EventDispatcher:
    def __init__(
        self,
        event_map: EventMap,
        container: Container,
        middleware_chain: MiddlewareChain | None = None,
        scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    ) -> None:
        self._event_map = event_map
        self._container = container
        self._middleware_chain = middleware_chain or MiddlewareChain()
        self._scope_strategy = scope_strategy

    async def _handle_event(
        self,
        event: IEvent,
        handle_type: typing.Type[_EventHandler],
    ) -> None:
        async with handler_scope(self._container, self._scope_strategy):
            handler: _EventHandler = await self._container.resolve(handle_type)
            await handler.handle(event)
            follow_ups = list(handler.events)
        for follow_up in follow_ups:
            await self.dispatch(follow_up)

    async def _handle_event_fallback(
        self,
        event: IEvent,
        fallback_config: EventHandlerFallback,
    ) -> None:
        """Run primary handler with fallback on failure; dispatch follow-up events from the handler that ran."""

        async def primary() -> list[IEvent]:
            primary_handler: _EventHandler = await self._container.resolve(fallback_config.primary)
            try:
                if fallback_config.circuit_breaker is not None:
                    await fallback_config.circuit_breaker.call(
                        fallback_config.primary,
                        primary_handler.handle,
                        event,
                    )
                else:
                    await primary_handler.handle(event)
                return list(primary_handler.events)
            except Exception as primary_error:
                should_fallback = should_use_fallback(
                    primary_error,
                    fallback_config.circuit_breaker,
                    fallback_config.failure_exceptions,
                )
                if should_fallback:
                    logger.warning(
                        "Primary event handler %s failed: %s. Switching to fallback %s.",
                        fallback_config.primary.__name__,
                        primary_error,
                        fallback_config.fallback.__name__,
                    )
                    raise _Fallback(primary_error) from primary_error
                raise primary_error

        async def fallback() -> list[IEvent]:
            fallback_handler: _EventHandler = await self._container.resolve(
                fallback_config.fallback,
            )
            await fallback_handler.handle(event)
            return list(fallback_handler.events)

        follow_ups = await _run_with_fallback_scope(
            self._container,
            self._scope_strategy,
            primary,
            fallback,
        )
        for follow_up in follow_ups:
            await self.dispatch(follow_up)

    async def dispatch(self, event: IEvent) -> None:
        handler_types = self._event_map.get(type(event), [])
        if not handler_types:
            logger.warning(
                "Handlers for event %s not found",
                type(event).__name__,
            )
            return
        for h_type in handler_types:
            if isinstance(h_type, EventHandlerFallback):
                await self._handle_event_fallback(event, h_type)
            else:
                await self._handle_event(event, h_type)
