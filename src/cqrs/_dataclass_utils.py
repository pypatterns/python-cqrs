"""Shared helpers for dataclass-based request/response/event models."""

from __future__ import annotations

import dataclasses
import typing


def ensure_dataclass(
    cls: type,
    *,
    frozen: bool = False,
    kw_only: bool = False,
) -> type:
    """Apply ``@dataclass`` to ``cls`` if it is not already a dataclass.

    Subclasses of DC* bases can omit an explicit ``@dataclass`` decorator;
    ``__init_subclass__`` calls this helper. Pass ``frozen=True`` via
    ``class MyEvent(DomainEvent, frozen=True):`` for immutable subclasses.

    Note: undecorated subclasses inherit ``__dataclass_fields__`` from a
    dataclass parent, so ``dataclasses.is_dataclass(cls)`` alone is not
    enough — we require fields defined on ``cls`` itself.
    """
    if "__dataclass_fields__" in cls.__dict__:
        return cls
    return dataclasses.dataclass(frozen=frozen, kw_only=kw_only)(cls)


def pydantic_extra_error(symbol: str) -> ImportError:
    return ImportError(
        f"{symbol} requires the optional pydantic extra. " "Install it with: pip install python-cqrs[pydantic]",
    )


_PYDANTIC_EVENT_NAMES = frozenset(
    {
        "PydanticEvent",
        "PydanticDomainEvent",
        "PydanticNotificationEvent",
    },
)


def load_pydantic_events() -> typing.Any:
    try:
        from cqrs.events import pydantic as module
    except ImportError as exc:
        raise pydantic_extra_error("Pydantic event types") from exc
    return module
