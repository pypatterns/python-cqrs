import logging
import typing

from cqrs.container.protocol import Container
from cqrs.events.event import IEvent
from cqrs.events.event_handler import EventHandler
from cqrs.events.fallback import EventHandlerFallback
from cqrs.events.map import EventMap
from cqrs.middlewares.base import MiddlewareChain

_EventHandler = EventHandler

logger = logging.getLogger("cqrs")


class EventDispatcher:
    def __init__(
        self,
        event_map: EventMap,
        container: Container,
        middleware_chain: typing.Optional[MiddlewareChain] = None,
    ) -> None:
        self._event_map = event_map
        self._container = container
        self._middleware_chain = middleware_chain or MiddlewareChain()

    async def _handle_event(
        self,
        event: IEvent,
        handle_type: typing.Type[_EventHandler],
    ) -> None:
        handler: _EventHandler = await self._container.resolve(handle_type)
        await handler.handle(event)
        for follow_up in handler.events:
            await self.dispatch(follow_up)

    async def _handle_event_fallback(
        self,
        event: IEvent,
        fallback_config: EventHandlerFallback,
    ) -> None:
        """Run primary handler with fallback on failure; dispatch follow-up events from the handler that ran."""
        primary: _EventHandler = await self._container.resolve(fallback_config.primary)
        try:
            if fallback_config.circuit_breaker is not None:
                await fallback_config.circuit_breaker.call(
                    fallback_config.primary,
                    primary.handle,
                    event,
                )
            else:
                await primary.handle(event)
            for follow_up in primary.events:
                await self.dispatch(follow_up)
        except Exception as primary_error:
            should_fallback = False
            if fallback_config.circuit_breaker is not None and fallback_config.circuit_breaker.is_circuit_breaker_error(
                primary_error,
            ):
                should_fallback = True
            elif fallback_config.failure_exceptions and isinstance(
                primary_error,
                fallback_config.failure_exceptions,
            ):
                should_fallback = True
            elif not fallback_config.failure_exceptions:
                should_fallback = True
            if should_fallback:
                logger.warning(
                    "Primary event handler %s failed: %s. Switching to fallback %s.",
                    fallback_config.primary.__name__,
                    primary_error,
                    fallback_config.fallback.__name__,
                )
                fallback_handler: _EventHandler = await self._container.resolve(
                    fallback_config.fallback,
                )
                await fallback_handler.handle(event)
                for follow_up in fallback_handler.events:
                    await self.dispatch(follow_up)
            else:
                raise primary_error

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
