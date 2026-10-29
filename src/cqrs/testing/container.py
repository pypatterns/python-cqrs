"""Container for harness tests and the resolve wrapper that records handler entry.

[AI Instruction]
``TestContainer.register`` stores one instance. ``resolve`` returns it, or
constructs the type with no arguments. ``ScopeStrategy.SEND`` needs a
``di.Container``, not ``TestContainer``. The resolve wrapper is what
``test_harness`` installs on the mediators' containers.
"""

import inspect
import typing

from cqrs.testing.recording import InvocationJournal

_RECORDING = "_cqrs_testing_recording"
_T = typing.TypeVar("_T")


class TestContainer:
    """
    Small container for harness tests.

    ``register(type, instance)`` pins that instance. ``resolve`` returns the
    registration or calls ``type_()``.
    """

    __test__ = False

    def __init__(self) -> None:
        self._registry: dict[type, typing.Any] = {}

    def register(self, type_: type, instance: typing.Any) -> None:
        self._registry[type_] = instance

    @property
    def external_container(self) -> "TestContainer":
        return self

    def attach_external_container(self, container: typing.Any) -> None:
        return None

    async def resolve(self, type_: typing.Type[_T]) -> _T:
        if type_ in self._registry:
            return typing.cast(_T, self._registry[type_])
        return type_()


def _first_argument(args: tuple[typing.Any, ...], kwargs: dict[str, typing.Any]) -> typing.Any:
    if args:
        return args[0]
    for name in ("request", "event", "context"):
        if name in kwargs:
            return kwargs[name]
    if kwargs:
        return next(iter(kwargs.values()))
    return None


def _wrap_coroutine(
    original: typing.Callable[..., typing.Awaitable[typing.Any]],
    handler_type: type,
    method_name: str,
    instance: typing.Any,
    journal: InvocationJournal,
) -> typing.Callable[..., typing.Awaitable[typing.Any]]:
    async def wrapped(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        journal.record(
            handler_type,
            method_name,
            _first_argument(args, kwargs),
            instance,
        )
        return await original(*args, **kwargs)

    setattr(wrapped, _RECORDING, True)
    return wrapped


def _wrap_async_gen(
    original: typing.Callable[..., typing.AsyncIterator[typing.Any]],
    handler_type: type,
    method_name: str,
    instance: typing.Any,
    journal: InvocationJournal,
) -> typing.Callable[..., typing.AsyncIterator[typing.Any]]:
    async def wrapped(*args: typing.Any, **kwargs: typing.Any) -> typing.AsyncIterator[typing.Any]:
        journal.record(
            handler_type,
            method_name,
            _first_argument(args, kwargs),
            instance,
        )
        async for item in original(*args, **kwargs):
            yield item

    setattr(wrapped, _RECORDING, True)
    return wrapped


def instrument_instance(
    instance: typing.Any,
    handler_type: type,
    journal: InvocationJournal,
) -> None:
    """Record entry into ``handle``, ``act``, and ``compensate`` on ``instance``."""

    for method_name in ("handle", "act", "compensate"):
        current = getattr(instance, method_name, None)
        if current is None or not callable(current):
            continue
        if getattr(current, _RECORDING, False):
            continue
        if inspect.isasyncgenfunction(current):
            replacement = _wrap_async_gen(current, handler_type, method_name, instance, journal)
        elif inspect.iscoroutinefunction(current):
            replacement = _wrap_coroutine(current, handler_type, method_name, instance, journal)
        else:
            continue
        setattr(instance, method_name, replacement)


def wrap_resolve(container: typing.Any, journal: InvocationJournal) -> typing.Callable[[], None]:
    """
    Wrap ``container.resolve`` so each handler records entry.

    Returns a callable that restores the previous ``resolve``.
    """

    original = container.resolve
    if getattr(original, _RECORDING, False):
        return lambda: None

    async def resolve(type_: typing.Type[_T]) -> _T:
        instance = await original(type_)
        instrument_instance(instance, type_, journal)
        return instance

    setattr(resolve, _RECORDING, True)
    container.resolve = resolve

    def restore() -> None:
        container.resolve = original

    return restore


__all__ = (
    "TestContainer",
    "instrument_instance",
    "wrap_resolve",
)
