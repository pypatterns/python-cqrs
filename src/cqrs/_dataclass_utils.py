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


def to_plain_dict(obj: typing.Any) -> typing.Any:
    """Recursively convert dataclass instances to plain ``dict`` trees.

    Mirrors :func:`dataclasses.asdict` structure conversion (nested dataclasses,
    lists, tuples — including namedtuples — and dicts) but **does not**
    ``deepcopy`` leaf values. That keeps recursive conversion correct while
    avoiding the main cost of ``dataclasses.asdict``.

    Leaf values (``str``, ``int``, ``UUID``, ``datetime``, ``Enum``, ``None``,
    custom objects, …) are returned as-is. Container nodes are always rebuilt,
    so list/dict structure is isolated from the source instance.

    Dict keys are not converted (``dataclasses.asdict`` would turn dataclass
    keys into unhashable ``dict`` and raise); values are converted recursively.
    """
    obj_type = type(obj)

    # Scalars dominate nested JSON trees — bail out before heavier checks.
    if obj_type is str or obj_type is int or obj_type is float or obj_type is bool or obj is None:
        return obj

    # Fast paths for the common JSON-tree containers (exact types first).
    if obj_type is dict:
        return {k: to_plain_dict(v) for k, v in obj.items()}
    if obj_type is list:
        return [to_plain_dict(v) for v in obj]
    if obj_type is tuple:
        if hasattr(obj, "_fields"):
            return obj_type(*(to_plain_dict(v) for v in obj))
        return tuple(to_plain_dict(v) for v in obj)

    if dataclasses.is_dataclass(obj):
        if isinstance(obj, type):
            return obj
        return {f.name: to_plain_dict(getattr(obj, f.name)) for f in dataclasses.fields(obj)}

    # dict/list/tuple subclasses (e.g. OrderedDict, namedtuple) — preserve type.
    if isinstance(obj, dict):
        return obj_type((k, to_plain_dict(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return obj_type(to_plain_dict(v) for v in obj)
    if isinstance(obj, tuple):
        if hasattr(obj, "_fields"):
            return obj_type(*(to_plain_dict(v) for v in obj))
        return obj_type(to_plain_dict(v) for v in obj)

    return obj


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
