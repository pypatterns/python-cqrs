"""Shared utilities for extracting generic type parameters from handler classes."""

import typing


def get_generic_args_for_origin(
    klass: type,
    origin_classes: tuple[type, ...],
    min_args: int = 1,
) -> typing.Optional[tuple[type, ...]]:
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
