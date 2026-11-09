"""Default container used when bootstrap is given none.

[AI Instruction]
``SimpleContainer`` is the container ``bootstrap`` uses when ``di_container``
is omitted. It constructs a handler with ``type_()``. ``register`` pins an
instance for a type whose constructor needs arguments. Scopes
(``ScopeStrategy.SEND`` and ``HANDLER``) need a container that implements
``SupportsScope``: ``python-cqrs[di]`` or ``python-cqrs[dishka]``.
``dependency-injector`` is ``python-cqrs[dependency-injector]`` and does not
implement ``SupportsScope``.
"""

from __future__ import annotations

import inspect
import typing

from cqrs.container.protocol import Container

T = typing.TypeVar("T")

_MISSING_DEPENDENCIES = (
    "Cannot construct {name} without constructor arguments. "
    "Register an instance on cqrs.SimpleContainer, or pass a DI container: "
    "python-cqrs[di], python-cqrs[dependency-injector], or python-cqrs[dishka]."
)


def _requires_arguments(type_: type) -> bool:
    try:
        signature = inspect.signature(type_)
    except (TypeError, ValueError):
        return False
    for name, param in signature.parameters.items():
        if name == "self":
            continue
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue
        if param.default is inspect.Parameter.empty:
            return True
    return False


class SimpleContainer(Container["SimpleContainer"]):
    """
    Construct handlers that take no constructor arguments.

    ``register(type, instance)`` returns that instance from ``resolve``.
    A type that is not registered is called as ``type_()``. A required
    constructor argument raises ``TypeError`` naming the optional DI extras.
    """

    def __init__(self) -> None:
        self._registry: dict[type, typing.Any] = {}

    def register(self, type_: type, instance: typing.Any) -> None:
        """Pin ``instance`` for later ``resolve(type_)`` calls.

        Every ``resolve`` of ``type_`` returns this same object, including
        concurrent calls. A handler that appends to its event list must
        replace or clear that list at the start of ``handle``, or the next
        send publishes the previous events again.
        """
        self._registry[type_] = instance

    @property
    def external_container(self) -> SimpleContainer:
        return self

    def attach_external_container(self, container: SimpleContainer) -> None:
        raise TypeError(
            "SimpleContainer does not wrap another container. "
            "Pass that container to bootstrap, or register instances here.",
        )

    async def resolve(self, type_: typing.Type[T]) -> T:
        if type_ in self._registry:
            return typing.cast(T, self._registry[type_])
        if _requires_arguments(type_):
            raise TypeError(_MISSING_DEPENDENCIES.format(name=type_.__qualname__))
        return type_()


def as_container(value: typing.Any | None) -> Container[typing.Any]:
    """
    Return the container bootstrap should keep.

    ``None`` becomes :class:`SimpleContainer`. A ``di.Container`` is wrapped
    in :class:`cqrs.container.di.DIContainer` when the ``di`` extra is
    installed. Any other object is used as a CQRS container.
    """
    if value is None:
        return SimpleContainer()
    try:
        import di
    except ImportError:
        return value
    if isinstance(value, di.Container):
        from cqrs.container.di import DIContainer

        wrapped = DIContainer()
        wrapped.attach_external_container(value)
        return wrapped
    return value
