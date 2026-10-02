"""Event types, handlers, emitter, and event map for the CQRS events layer.

Public API:
- Event types: :class:`Event`, :class:`DomainEvent`, :class:`NotificationEvent`,
  and their interfaces/base classes.
- :class:`EventHandler` — handler interface; implement :meth:`EventHandler.handle`
  and optionally :attr:`EventHandler.events` for follow-up events.
- :class:`EventEmitter` — sends domain events to handlers and notification events
  to a message broker.
- :class:`EventMap` — registry of event type -> handler types; use :meth:`EventMap.bind`.
- :class:`EventHandlerFallback` — fallback wrapper for event handlers with optional circuit breaker.

Pydantic event classes require ``python-cqrs[pydantic]`` and are loaded when available.
"""

import typing

from cqrs._dataclass_utils import pydantic_extra_error
from cqrs.events.event import (
    DCEvent,
    DCDomainEvent,
    DCNotificationEvent,
    DomainEvent,
    Event,
    IDomainEvent,
    IEvent,
    INotificationEvent,
    NotificationEvent,
)
from cqrs.events.event_emitter import EventEmitter
from cqrs.events.event_handler import EventHandler
from cqrs.events.fallback import EventHandlerFallback
from cqrs.events.map import EventMap

try:
    from cqrs.events.pydantic import (
        PydanticDomainEvent,
        PydanticEvent,
        PydanticNotificationEvent,
    )
except ImportError:  # pragma: no cover
    PydanticEvent = None  # type: ignore[misc, assignment]
    PydanticDomainEvent = None  # type: ignore[misc, assignment]
    PydanticNotificationEvent = None  # type: ignore[misc, assignment]

__all__ = (
    "Event",
    "IEvent",
    "DCEvent",
    "PydanticEvent",
    "DomainEvent",
    "IDomainEvent",
    "DCDomainEvent",
    "PydanticDomainEvent",
    "NotificationEvent",
    "INotificationEvent",
    "DCNotificationEvent",
    "PydanticNotificationEvent",
    "EventEmitter",
    "EventHandler",
    "EventHandlerFallback",
    "EventMap",
)


def __getattr__(name: str) -> typing.Any:
    if name in {
        "PydanticEvent",
        "PydanticDomainEvent",
        "PydanticNotificationEvent",
    }:
        value = globals().get(name)
        if value is not None:
            return value
        raise pydantic_extra_error(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
