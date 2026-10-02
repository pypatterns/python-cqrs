"""Event handlers, emitter, and map for the CQRS events layer."""

import typing

from cqrs._dataclass_utils import pydantic_extra_error
from cqrs.events.event_emitter import EventEmitter
from cqrs.events.fallback import EventHandlerFallback
from cqrs.handlers.event import EventHandler
from cqrs.mapping.events import EventMap
from cqrs.models.event import (
    DCDomainEvent,
    DCEvent,
    DCNotificationEvent,
    DomainEvent,
    Event,
    IDomainEvent,
    IEvent,
    INotificationEvent,
    NotificationEvent,
)

try:
    from cqrs.models.pydantic import (
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
