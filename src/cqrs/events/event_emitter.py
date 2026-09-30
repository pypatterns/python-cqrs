import asyncio
import functools
import logging
import typing

from cqrs import container as di_container, message_brokers
from cqrs.circuit_breaker import should_use_fallback
from cqrs.container.scope import (
    ScopeStrategy,
    _Fallback,
    _run_with_fallback_scope,
    handler_scope,
    wrap_container,
)
from cqrs.events.event import IDomainEvent, IEvent, INotificationEvent
from cqrs.events import event_handler, map
from cqrs.events.fallback import EventHandlerFallback
from cqrs.serializers.default import headers_for_content_type
from cqrs.serializers.json import JsonEventSerializer
from cqrs.serializers.protocol import EventSerializer

logger = logging.getLogger("cqrs")

_H: typing.TypeAlias = event_handler.EventHandler


class EventEmitter:
    """
    Sends events to registered handlers or to a message broker.

    For :class:`~cqrs.events.event.IDomainEvent`: resolves handlers from the
    container, runs :meth:`~cqrs.events.event_handler.EventHandler.handle`, and
    returns follow-up events from :attr:`~cqrs.events.event_handler.EventHandler.events`.
    For :class:`~cqrs.events.event.INotificationEvent`: sends the event to the
    message broker (if configured) and returns an empty sequence.
    """

    def __init__(
        self,
        event_map: map.EventMap,
        container: di_container.Container,
        message_broker: message_brokers.MessageBroker | None = None,
        scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
        *,
        serializer: EventSerializer | None = None,
    ) -> None:
        """
        Initialize the event emitter.

        Args:
            event_map: Map of event types to handler types (used for domain events).
            container: DI container to resolve handler instances.
            message_broker: Optional broker for notification events; required
                when emitting :class:`~cqrs.events.event.INotificationEvent`.
            scope_strategy: DI scope boundary; HANDLER opens a scope per handler.
            serializer: Codec used to fill ``Message.payload_bytes``. Defaults to JSON.

        Example::

            event_map = EventMap()
            event_map.bind(OrderCreatedEvent, OrderCreatedEventHandler)
            emitter = EventEmitter(
                event_map=event_map,
                container=di_container,
                message_broker=kafka_broker,
            )
            follow_ups = await emitter.emit(OrderCreatedEvent(order_id="1"))
        """
        self._event_map = event_map
        self._container = wrap_container(container)
        self._message_broker = message_broker
        self._scope_strategy = scope_strategy
        self._serializer = serializer or JsonEventSerializer()

    @functools.singledispatchmethod
    async def emit(self, event: IEvent) -> typing.Sequence[IEvent]:
        """
        Emit an event and return follow-up events from handlers.

        For unknown event types returns an empty sequence. For domain events
        invokes all registered handlers and collects events from
        :attr:`~cqrs.events.event_handler.EventHandler.events`. For notification
        events sends to the message broker.

        Args:
            event: The event to emit (domain or notification).

        Returns:
            Follow-up events returned by domain event handlers; empty for
            notification events or when no handlers are registered.

        Example::

            follow_ups = await emitter.emit(OrderCreatedEvent(order_id="1"))
            for e in follow_ups:
                await emitter.emit(e)  # or process via EventProcessor
        """
        return ()

    async def _send_to_broker(
        self,
        event: INotificationEvent,
    ) -> None:
        """
        Send a notification event to the message broker.

        Args:
            event: Notification event to send.

        Raises:
            RuntimeError: If no message broker was configured.
        """
        if not self._message_broker:
            raise RuntimeError(
                f"To send event {event}, message broker must be specified.",
            )

        # payload (dict) and payload_bytes are both intentional: stub brokers /
        # Message.to_dict() need the structured dict; transport uses wire bytes.
        content_type, headers = headers_for_content_type(
            self._serializer.content_type_for(event),
            event,
        )
        message = message_brokers.Message(
            message_name=type(event).__name__,
            message_id=event.event_id,
            topic=event.topic,
            payload=event.to_dict(),
            payload_bytes=self._serializer.serialize(event),
            content_type=content_type,
            headers=headers,
        )

        logger.debug(
            "Sending Event(%s) to message broker %s",
            event.event_id,
            type(self._message_broker).__name__,
        )

        await self._message_broker.send_message(message)

    async def _handle_with_fallback(
        self,
        event: IDomainEvent,
        fallback_config: EventHandlerFallback,
    ) -> typing.Sequence[IEvent]:
        """Run primary handler with fallback on failure; return events from the handler that ran."""

        async def primary() -> typing.Sequence[IEvent]:
            primary_handler: _H = await self._container.resolve(fallback_config.primary)
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
                    if (
                        fallback_config.circuit_breaker is not None
                        and fallback_config.circuit_breaker.is_circuit_breaker_error(
                            primary_error,
                        )
                    ):
                        logger.warning(
                            "Circuit breaker open for event handler %s, switching to fallback %s",
                            fallback_config.primary.__name__,
                            fallback_config.fallback.__name__,
                        )
                    else:
                        logger.warning(
                            "Primary event handler %s failed: %s. Switching to fallback %s.",
                            fallback_config.primary.__name__,
                            primary_error,
                            fallback_config.fallback.__name__,
                        )
                    raise _Fallback(primary_error) from primary_error
                raise primary_error

        async def fallback() -> typing.Sequence[IEvent]:
            fallback_handler: _H = await self._container.resolve(fallback_config.fallback)
            await fallback_handler.handle(event)
            return list(fallback_handler.events)

        return await _run_with_fallback_scope(
            self._container,
            self._scope_strategy,
            primary,
            fallback,
        )

    @emit.register(IDomainEvent)
    async def _(self, event: IDomainEvent) -> typing.Sequence[IEvent]:
        """Emit domain event: run all registered handlers and return their follow-up events."""
        handlers_types = self._event_map.get(type(event), [])
        if not handlers_types:
            logger.warning(
                "Handlers for domain event %s not found",
                type(event).__name__,
            )
            return ()

        results = await self._run_handlers(event, handlers_types)

        follow_ups: list[IEvent] = []
        for handler_item, res in zip(handlers_types, results):
            if isinstance(res, Exception):
                handler_name = (
                    handler_item.primary.__name__
                    if isinstance(handler_item, EventHandlerFallback)
                    else handler_item.__name__
                )
                logger.exception(
                    "Error occurred while processing domain event %s in handler %s: %s",
                    type(event).__name__,
                    handler_name,
                    res,
                    exc_info=res,
                )
            elif isinstance(res, BaseException):
                raise res
            else:
                follow_ups.extend(res)

        return follow_ups

    @emit.register(INotificationEvent)
    async def _(self, event: INotificationEvent) -> typing.Sequence[IEvent]:
        """Emit notification event: send to message broker; no follow-ups."""
        await self._send_to_broker(event)
        return ()

    async def _run_handlers(
        self,
        event: IDomainEvent,
        handlers_types: list[typing.Union[typing.Type[_H], EventHandlerFallback]],
    ) -> list[typing.Sequence[IEvent] | BaseException]:
        """Run handlers for one event; SEND stays sequential so they share one UoW."""
        if self._scope_strategy == ScopeStrategy.SEND:
            results: list[typing.Sequence[IEvent] | BaseException] = []
            for item in handlers_types:
                try:
                    results.append(await self._process_single_handler(event, item))
                except Exception as e:
                    results.append(e)
            return results
        return await asyncio.gather(
            *(self._process_single_handler(event, item) for item in handlers_types),
            return_exceptions=True,
        )

    async def _process_single_handler(
        self,
        event: IDomainEvent,
        handler_item: typing.Union[typing.Type[_H], EventHandlerFallback],
    ) -> typing.Sequence[IEvent]:
        """Process a single event handler and return its follow-up events."""
        if isinstance(handler_item, EventHandlerFallback):
            return await self._handle_with_fallback(event, handler_item)

        handler_type = handler_item
        async with handler_scope(self._container, self._scope_strategy):
            handler: _H = await self._container.resolve(handler_type)
            logger.debug(
                "Handling Event(%s) via event handler(%s)",
                type(event).__name__,
                handler_type.__name__,
            )
            await handler.handle(event)
            return list(handler.events)
