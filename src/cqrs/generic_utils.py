"""Shared utilities for extracting generic type parameters from handler classes."""

import typing

_MISSING = object()
_REQUEST_ORIGIN_NAMES = frozenset({"IRequest", "DCRequest", "Request", "PydanticRequest"})


def get_generic_args_for_origin(
    klass: type,
    origin_classes: tuple[type, ...],
    min_args: int = 1,
) -> tuple[type, ...] | None:
    """
    Extract generic type arguments from a class that inherits from a Generic base.

    Walks __orig_bases__ and __bases__ to find the first base whose origin is
    one of the given origin_classes, then returns typing.get_args(base).

    Args:
        klass: The handler class (e.g. a subclass of RequestHandler[Req, Res]).
        origin_classes: Tuple of possible origin classes (e.g. (RequestHandler, StreamingRequestHandler)).
        min_args: Minimum number of type arguments required to consider the result valid.

    Returns:
        Tuple of type arguments (e.g. (ReqT, ResT) or (ET,)), or None if not found
        or if the base has fewer than min_args concrete arguments.
    """
    # Prefer __orig_bases__ (Python 3.12+ / generic subclass)
    orig_bases = getattr(klass, "__orig_bases__", ())
    for base in orig_bases:
        origin = typing.get_origin(base)
        if origin in origin_classes:
            args = typing.get_args(base)
            if len(args) >= min_args:
                return args

    # Fallback: __bases__ may contain the parameterized base
    for base in klass.__bases__:
        origin = typing.get_origin(base)
        if origin in origin_classes:
            args = typing.get_args(base)
            if len(args) >= min_args:
                return args

    return None


def _is_typevar(arg: typing.Any) -> bool:
    return type(arg).__name__ == "TypeVar"


def _is_request_origin(origin: typing.Any) -> bool:
    if not isinstance(origin, type):
        return False
    if origin.__name__ not in _REQUEST_ORIGIN_NAMES:
        return False
    return getattr(origin, "__module__", "").startswith("cqrs.")


def _response_arg_on(klass: type) -> typing.Any:
    """Return a concrete response type argument declared on ``klass``, or a sentinel."""
    meta = getattr(klass, "__pydantic_generic_metadata__", None)
    if isinstance(meta, dict):
        origin = meta.get("origin")
        args = meta.get("args") or ()
        if args and _is_request_origin(origin) and not _is_typevar(args[0]):
            return args[0]
    for base in getattr(klass, "__orig_bases__", ()):
        origin = typing.get_origin(base)
        if not _is_request_origin(origin):
            continue
        args = typing.get_args(base)
        if args and not _is_typevar(args[0]):
            return args[0]
    return _MISSING


def response_type_of_request(request_type: typing.Any) -> typing.Any:
    """Read ``Request[Result]`` / ``DCRequest[Result]`` / ``PydanticRequest[Result]``.

    An unparameterized request projects to ``Any``. ``Request[None]`` projects
    to ``None``. Type variables left by an unspecialized generic base are skipped.
    """
    if not isinstance(request_type, type):
        return typing.Any
    seen: set[int] = set()
    stack: list[type] = [request_type]
    while stack:
        klass = stack.pop()
        marker = id(klass)
        if marker in seen:
            continue
        seen.add(marker)
        found = _response_arg_on(klass)
        if found is not _MISSING:
            return found
        bases = (*getattr(klass, "__orig_bases__", ()), *klass.__bases__)
        for base in bases:
            origin = typing.get_origin(base)
            nxt = origin if isinstance(origin, type) else base
            if isinstance(nxt, type) and id(nxt) not in seen:
                stack.append(nxt)
    return typing.Any


def handler_class_getitem(cls: type, params: typing.Any) -> typing.Any:
    """Subscript a handler, expanding one request argument to ``(request, response)``.

    Two arguments pass through unchanged. Pyright 1.1.408 keeps normal ``Generic``
    semantics for a custom ``__class_getitem__`` on a ``Generic`` subclass, so the
    static one-argument form uses ``ResT``'s default and the method body supplies
    the concrete result. The alias stored here still carries the projected pair
    for :func:`typing.get_args`.
    """
    if not isinstance(params, tuple):
        params = (params, response_type_of_request(params))
    getitem = getattr(super(cls, cls), "__class_getitem__")
    return getitem(params)
