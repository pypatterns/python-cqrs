from __future__ import annotations

import contextlib
import enum
import logging
import typing
from contextvars import ContextVar, Token
from weakref import WeakKeyDictionary

from cqrs.container.protocol import Container, SupportsScope

T = typing.TypeVar("T")
C = typing.TypeVar("C")
R = typing.TypeVar("R")

logger = logging.getLogger("cqrs")

_current_container: ContextVar[Container | None] = ContextVar(
    "cqrs_current_container",
    default=None,
)


class ScopeStrategy(enum.Enum):
    """
    Controls where the framework opens a DI scope.

    The default everywhere is :attr:`NONE`, so scoping never changes the
    behaviour of an existing application; ``SEND`` and ``HANDLER`` are opt-in
    and must be passed explicitly to ``bootstrap``/mediators/sagas.

    SEND:
        One scope per ``mediator.send()`` / ``stream()``, including domain-event
        handlers that run after the command. Matches outbox / shared UoW semantics.
        Events are sequential (``concurrent=None`` becomes ``False``). Fallback
        runs in the same scope as the primary handler (same UoW, including a
        dirty session). Pass the same strategy to ``recover_saga`` / ``transaction``.

    HANDLER:
        A fresh scope around each resolve+handle (command, event, saga step),
        including fallback: the primary scope is closed with rollback, then
        fallback runs in a new scope.

    NONE (default):
        Do not open scopes (legacy behaviour). Fallback is one-shot like today.
    """

    SEND = "send"
    HANDLER = "handler"
    NONE = "none"


def assert_send_compatible_with_concurrent_events(
    scope_strategy: ScopeStrategy,
    concurrent_event_handle_enable: bool,
) -> None:
    if scope_strategy == ScopeStrategy.SEND and concurrent_event_handle_enable:
        raise ValueError(
            "ScopeStrategy.SEND cannot be used with concurrent_event_handle_enable=True: "
            "parallel event handlers would share one UoW/session. "
            "Use ScopeStrategy.HANDLER or NONE, or set concurrent_event_handle_enable=False.",
        )


def resolve_concurrent_event_handling(
    scope_strategy: ScopeStrategy,
    concurrent_event_handle_enable: bool | None,
) -> bool:
    """Infer concurrent event handling: ``None`` is ``False`` under SEND, else ``True``."""
    if concurrent_event_handle_enable is None:
        concurrent_event_handle_enable = scope_strategy != ScopeStrategy.SEND
    assert_send_compatible_with_concurrent_events(
        scope_strategy,
        concurrent_event_handle_enable,
    )
    return concurrent_event_handle_enable


def current_container() -> Container | None:
    """Return the container bound to the current scope, if any."""
    return _current_container.get()


def wrap_container(container: Container) -> ScopeAwareContainer:
    """
    Internal: wrap a user container so ``resolve`` reads the ambient scope.

    Idempotent. Not part of the public API; mediators and ``SagaTransaction``
    call this so callers do not need a manual ``ScopeAwareContainer``.
    """
    if isinstance(container, ScopeAwareContainer):
        return container
    return ScopeAwareContainer(container)


_SCOPE_CLOSED_ATTR = "_cqrs_scope_closed"
_closed_scopes: WeakKeyDictionary[Container, bool] = WeakKeyDictionary()


def _is_scope_closed(container: Container) -> bool:
    try:
        if _closed_scopes.get(container, False):
            return True
    except TypeError:
        pass
    return bool(getattr(container, _SCOPE_CLOSED_ATTR, False))


def _mark_scope_closed(container: Container) -> None:
    """Mark a scoped container so reuse_existing will not join it after exit."""
    try:
        _closed_scopes[container] = True
        return
    except TypeError:
        pass
    try:
        object.__setattr__(container, _SCOPE_CLOSED_ATTR, True)
    except (AttributeError, TypeError):
        logger.warning(
            "Cannot mark scoped container %s as closed; reuse_existing may join "
            "a dead scope. Add __weakref__ to slots or a writable attribute.",
            type(container).__name__,
        )


def _reset_token(token: Token[Container | None]) -> None:
    """
    Reset a ContextVar token, ignoring ValueError from async-generator leakage.

    Async generators do not get their own context (PEP 568 unimplemented), so
    ``ContextVar.set`` inside a stream/saga body can leak into the caller's
    context; resetting that token from another context raises ``ValueError``.
    ``set(None)`` clears a leak in the *current* context; ``enter_scope`` also
    marks the scoped container closed so a leftover value in the caller is
    not reused.
    """
    try:
        _current_container.reset(token)
    except ValueError:
        _current_container.set(None)


def _root_container(container: Container) -> Container:
    if isinstance(container, ScopeAwareContainer):
        return container.root
    return container


class _Fallback(Exception):
    """Internal sentinel: primary failed and fallback should run. Not public."""

    def __init__(self, error: BaseException) -> None:
        super().__init__()
        self.error = error


async def _run_with_fallback_scope(
    container: Container,
    strategy: ScopeStrategy,
    primary: typing.Callable[[], typing.Awaitable[R]],
    fallback: typing.Callable[[], typing.Awaitable[R]],
) -> R:
    """
    Run primary then fallback with the strategy's scope split.

    HANDLER: re-raise ``_Fallback`` through the primary ``handler_scope`` so a
    generator UoW rolls back, then open a new scope for fallback.
    SEND/NONE: catch ``_Fallback`` inside the current (no-op) scope so the
    outer ``enter_scope`` is not aborted.
    """
    if strategy == ScopeStrategy.HANDLER:
        try:
            async with handler_scope(container, strategy):
                return await primary()
        except _Fallback:
            pass
        async with handler_scope(container, strategy):
            return await fallback()

    async with handler_scope(container, strategy):
        try:
            return await primary()
        except _Fallback:
            return await fallback()


async def _stream_with_fallback_scope(
    container: Container,
    strategy: ScopeStrategy,
    primary: typing.Callable[[], typing.AsyncIterator[R]],
    fallback: typing.Callable[[], typing.AsyncIterator[R]],
) -> typing.AsyncIterator[R]:
    """Async-generator counterpart of :func:`_run_with_fallback_scope`."""
    if strategy == ScopeStrategy.HANDLER:
        try:
            async with handler_scope(container, strategy):
                async for item in primary():
                    yield item
        except _Fallback:
            pass
        else:
            return
        async with handler_scope(container, strategy):
            async for item in fallback():
                yield item
        return

    async with handler_scope(container, strategy):
        try:
            async for item in primary():
                yield item
        except _Fallback:
            async for item in fallback():
                yield item


class ScopeAwareContainer(typing.Generic[C]):
    """
    Thin wrapper that resolves against the ambient scoped container when set.

    Mediators wrap the user container once so dispatchers, emitters and saga
    code keep receiving a normal :class:`~cqrs.container.protocol.Container`.
    """

    def __init__(self, root: Container[C]) -> None:
        self._root = root

    @property
    def root(self) -> Container[C]:
        return self._root

    @property
    def external_container(self) -> C:
        return self._root.external_container

    def attach_external_container(self, container: C) -> None:
        self._root.attach_external_container(container)

    async def resolve(self, type_: typing.Type[T]) -> T:
        current = _current_container.get()
        if current is not None and not _is_scope_closed(current):
            return await current.resolve(type_)
        return await self._root.resolve(type_)

    @contextlib.asynccontextmanager
    async def open_scope(
        self,
        context: typing.Mapping[type, typing.Any] | None = None,
    ) -> typing.AsyncIterator[Container]:
        root = self._root
        if not isinstance(root, SupportsScope):
            yield self
            return
        async with root.open_scope(context=context) as scoped:
            yield scoped


@contextlib.asynccontextmanager
async def enter_scope(
    container: Container,
    *,
    context: typing.Mapping[type, typing.Any] | None = None,
    reuse_existing: bool = True,
) -> typing.AsyncIterator[Container]:
    """
    Open a DI scope and bind it to the current context for the duration of the block.

    If a scope is already active and ``reuse_existing`` is true, joins it (no nested
    scope). If the container does not implement :class:`SupportsScope`, yields the
    container without opening a scope.
    """
    existing = _current_container.get()
    if existing is not None and _is_scope_closed(existing):
        _current_container.set(None)
        existing = None
    if existing is not None and reuse_existing:
        yield existing
        return

    root = _root_container(container)
    if not isinstance(root, SupportsScope):
        yield container
        return

    async with root.open_scope(context=context) as scoped:
        token = _current_container.set(scoped)
        try:
            yield scoped
        finally:
            _mark_scope_closed(scoped)
            _reset_token(token)


@contextlib.asynccontextmanager
async def bind_scope(
    scoped_container: Container,
) -> typing.AsyncIterator[Container]:
    """
    Bind an externally opened scoped container to the current context.

    Use when FastAPI/dishka middleware (or similar) already owns the scope and
    CQRS handlers must resolve from that same instance.
    """
    token = _current_container.set(scoped_container)
    try:
        yield scoped_container
    finally:
        _reset_token(token)


@contextlib.asynccontextmanager
async def handler_scope(
    container: Container,
    strategy: ScopeStrategy,
) -> typing.AsyncIterator[None]:
    """Open a fresh scope when ``strategy`` is :attr:`ScopeStrategy.HANDLER`."""
    if strategy == ScopeStrategy.HANDLER:
        async with enter_scope(container, reuse_existing=False):
            yield
    else:
        yield
