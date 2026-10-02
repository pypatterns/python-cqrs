"""Tests for DI scoped / generator-based dependencies (issue #70)."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import typing
import uuid
from dataclasses import dataclass, field

import di
import pytest
from di import dependent

import cqrs
from cqrs.container.di import DIContainer
from cqrs.container.scope import (
    ScopeAwareContainer,
    ScopeStrategy,
    bind_scope,
    current_container,
    enter_scope,
)
from cqrs.bootstrap import requests as bootstrap
from cqrs.saga.execution import SagaStepRef
from cqrs.saga.fallback import Fallback
from cqrs.saga.models import SagaContext
from cqrs.saga.recovery import recover_saga
from cqrs.handlers.saga import SagaStepResult
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.memory import MemorySagaStorage


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


@dataclass
class FakeUoW:
    name: str
    closed: bool = False
    committed: bool = False
    rolled_back: bool = False
    commits: list[str] = field(default_factory=list)

    async def commit(self) -> None:
        self.commits.append(self.name)

    async def close(self) -> None:
        self.closed = True


LIFECYCLE: list[str] = []
UOW_INSTANCES: list[FakeUoW] = []


async def uow_provider() -> typing.AsyncIterator[FakeUoW]:
    uow = FakeUoW(name=f"uow-{len(UOW_INSTANCES)}")
    UOW_INSTANCES.append(uow)
    LIFECYCLE.append("enter")
    try:
        yield uow
    finally:
        await uow.close()
        LIFECYCLE.append("exit")


async def tracking_uow_provider() -> typing.AsyncIterator[FakeUoW]:
    uow = FakeUoW(name=f"uow-{len(UOW_INSTANCES)}")
    UOW_INSTANCES.append(uow)
    LIFECYCLE.append("enter")
    try:
        yield uow
        uow.committed = True
        LIFECYCLE.append("commit")
    except BaseException:
        uow.rolled_back = True
        LIFECYCLE.append("rollback")
        raise
    finally:
        await uow.close()
        LIFECYCLE.append("exit")


class Command(cqrs.Request):
    pass


class DomainEvt(cqrs.DomainEvent, frozen=True):
    payload: str = "x"


class CommandHandler(cqrs.RequestHandler[Command, None]):
    def __init__(self, uow: FakeUoW) -> None:
        self.uow = uow
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return self._events

    async def handle(self, request: Command) -> None:
        LIFECYCLE.append("handle")
        await self.uow.commit()
        self._events.append(DomainEvt())


class EventHandler(cqrs.EventHandler[DomainEvt]):
    def __init__(self, uow: FakeUoW) -> None:
        self.uow = uow
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return list(self._events)

    async def handle(self, event: DomainEvt) -> None:
        LIFECYCLE.append("event")
        EventHandler.seen_uow = self.uow  # type: ignore[attr-defined]
        await self.uow.commit()


class BoomCommand(cqrs.Request):
    pass


class BoomHandler(cqrs.RequestHandler[BoomCommand, None]):
    def __init__(self, uow: FakeUoW) -> None:
        self.uow = uow

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return []

    async def handle(self, request: BoomCommand) -> None:
        LIFECYCLE.append("handle")
        raise RuntimeError("boom")


class StreamCommand(cqrs.Request):
    pass


class StreamChunk(cqrs.Response):
    value: str = ""


class StreamHandler(cqrs.StreamingRequestHandler[StreamCommand, StreamChunk]):
    def __init__(self, uow: FakeUoW) -> None:
        self.uow = uow
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return list(self._events)

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(self, request: StreamCommand) -> typing.AsyncIterator[StreamChunk]:
        LIFECYCLE.append("handle")
        yield StreamChunk(value="one")
        LIFECYCLE.append("after_yield")
        yield StreamChunk(value="two")


class PlainContainer:
    """Container without SupportsScope — framework must no-op on scopes."""

    def __init__(self) -> None:
        self.resolved: list[type] = []

    @property
    def external_container(self) -> object:
        return self

    def attach_external_container(self, container: object) -> None:
        pass

    async def resolve(self, type_: typing.Type[typing.Any]) -> typing.Any:
        self.resolved.append(type_)
        if type_ is CommandHandler:
            return CommandHandler(FakeUoW("plain"))
        raise ValueError(type_)


def _reset() -> None:
    LIFECYCLE.clear()
    UOW_INSTANCES.clear()
    COMPENSATE_LOG.clear()
    ObservedReserveStep.instances.clear()
    if hasattr(EventHandler, "seen_uow"):
        delattr(EventHandler, "seen_uow")


def _di_container() -> di.Container:
    container = di.Container()
    container.bind(
        di.bind_by_type(
            dependent.Dependent(uow_provider, scope="request"),
            FakeUoW,
        ),
    )
    return container


def _plain_di_container() -> DIContainer:
    cqrs_c = DIContainer()
    cqrs_c.attach_external_container(_di_container())
    return cqrs_c


def _tracking_di_container() -> DIContainer:
    container = di.Container()
    container.bind(
        di.bind_by_type(
            dependent.Dependent(tracking_uow_provider, scope="request"),
            FakeUoW,
        ),
    )
    cqrs_c = DIContainer()
    cqrs_c.attach_external_container(container)
    return cqrs_c


def _commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(Command, CommandHandler)
    mapper.bind(BoomCommand, BoomHandler)


def _events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(DomainEvt, EventHandler)


def _stream_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(StreamCommand, StreamHandler)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generator_finalized_after_handle() -> None:
    _reset()
    mediator = bootstrap.bootstrap(
        di_container=_di_container(),
        commands_mapper=_commands_mapper,
        scope_strategy=ScopeStrategy.SEND,
    )
    await mediator.send(Command())
    assert LIFECYCLE == ["enter", "handle", "exit"]
    assert UOW_INSTANCES[0].closed is True
    assert UOW_INSTANCES[0].commits == ["uow-0"]


@pytest.mark.asyncio
async def test_send_strategy_shares_uow_with_domain_events() -> None:
    _reset()
    mediator = bootstrap.bootstrap(
        di_container=_di_container(),
        commands_mapper=_commands_mapper,
        domain_events_mapper=_events_mapper,
        scope_strategy=ScopeStrategy.SEND,
    )
    await mediator.send(Command())
    assert len(UOW_INSTANCES) == 1
    assert EventHandler.seen_uow is UOW_INSTANCES[0]  # type: ignore[attr-defined]
    assert UOW_INSTANCES[0].commits == ["uow-0", "uow-0"]
    assert LIFECYCLE == ["enter", "handle", "event", "exit"]


@pytest.mark.asyncio
async def test_handler_strategy_separate_uow_per_handler() -> None:
    _reset()
    mediator = bootstrap.bootstrap(
        di_container=_di_container(),
        commands_mapper=_commands_mapper,
        domain_events_mapper=_events_mapper,
        scope_strategy=ScopeStrategy.HANDLER,
    )
    await mediator.send(Command())
    assert len(UOW_INSTANCES) == 2
    assert EventHandler.seen_uow is UOW_INSTANCES[1]  # type: ignore[attr-defined]
    assert UOW_INSTANCES[0].closed and UOW_INSTANCES[1].closed
    assert LIFECYCLE == ["enter", "handle", "exit", "enter", "event", "exit"]


@pytest.mark.asyncio
async def test_noop_without_supports_scope() -> None:
    _reset()
    plain = PlainContainer()
    mediator = bootstrap.bootstrap(
        di_container=plain,  # type: ignore[arg-type]
        commands_mapper=_commands_mapper,
    )
    await mediator.send(Command())
    assert CommandHandler in plain.resolved
    assert LIFECYCLE == ["handle"]  # no enter/exit from generator scope


@pytest.mark.asyncio
async def test_scope_exits_on_handler_exception() -> None:
    _reset()
    mediator = bootstrap.bootstrap(
        di_container=_di_container(),
        commands_mapper=_commands_mapper,
        scope_strategy=ScopeStrategy.SEND,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await mediator.send(BoomCommand())
    assert LIFECYCLE == ["enter", "handle", "exit"]
    assert UOW_INSTANCES[0].closed is True


@pytest.mark.asyncio
async def test_stream_abort_finalizes_scope() -> None:
    _reset()
    mediator = bootstrap.bootstrap_streaming(
        di_container=_di_container(),
        commands_mapper=_stream_mapper,
        scope_strategy=ScopeStrategy.SEND,
    )
    stream = mediator.stream(StreamCommand())
    first = await stream.__anext__()
    assert first == StreamChunk(value="one")
    await stream.aclose()
    assert "exit" in LIFECYCLE
    assert UOW_INSTANCES[0].closed is True


@pytest.mark.asyncio
async def test_enter_scope_over_multiple_sends() -> None:
    _reset()
    di_c = _di_container()
    cqrs_c = DIContainer()
    cqrs_c.attach_external_container(di_c)
    mediator = bootstrap.bootstrap(
        di_container=cqrs_c,
        commands_mapper=_commands_mapper,
        scope_strategy=ScopeStrategy.SEND,
    )
    async with enter_scope(cqrs_c):
        await mediator.send(Command())
        await mediator.send(Command())
    assert len(UOW_INSTANCES) == 1
    assert LIFECYCLE.count("enter") == 1
    assert LIFECYCLE.count("exit") == 1
    assert LIFECYCLE.count("handle") == 2


@pytest.mark.asyncio
async def test_bind_scope_uses_external_container() -> None:
    _reset()
    di_c = _di_container()
    root = DIContainer()
    root.attach_external_container(di_c)

    seen: list[FakeUoW] = []

    class BindHandler(cqrs.RequestHandler[Command, None]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return []

        async def handle(self, request: Command) -> None:
            seen.append(self.uow)
            LIFECYCLE.append("handle")

    def mapper(m: cqrs.RequestMap) -> None:
        m.bind(Command, BindHandler)

    mediator = bootstrap.bootstrap(
        di_container=root,
        commands_mapper=mapper,
        scope_strategy=ScopeStrategy.NONE,
    )
    async with root.open_scope() as scoped:
        async with bind_scope(scoped):
            assert current_container() is scoped
            await mediator.send(Command())
    assert len(seen) == 1
    assert seen[0].closed is True
    assert LIFECYCLE == ["enter", "handle", "exit"]


@pytest.mark.asyncio
async def test_default_strategy_opens_no_scopes() -> None:
    """Without an explicit strategy the framework must behave exactly like NONE."""
    _reset()
    default_mediator = bootstrap.bootstrap(
        di_container=_di_container(),
        commands_mapper=_commands_mapper,
        domain_events_mapper=_events_mapper,
    )
    await default_mediator.send(Command())
    default_lifecycle = list(LIFECYCLE)
    default_uow_count = len(UOW_INSTANCES)

    _reset()
    none_mediator = bootstrap.bootstrap(
        di_container=_di_container(),
        commands_mapper=_commands_mapper,
        domain_events_mapper=_events_mapper,
        scope_strategy=ScopeStrategy.NONE,
    )
    await none_mediator.send(Command())

    assert default_lifecycle == LIFECYCLE
    assert default_uow_count == len(UOW_INSTANCES)
    # No framework scope: each resolve is a one-shot scope closed before handling
    assert default_lifecycle == ["enter", "exit", "handle", "enter", "exit", "event"]
    assert current_container() is None


@pytest.mark.asyncio
async def test_bind_scope_with_handler_strategy_nests_scope() -> None:
    """HANDLER nests a fresh scope: the bound outer container is not resolved from."""
    _reset()
    di_c = _di_container()
    root = DIContainer()
    root.attach_external_container(di_c)

    mediator = bootstrap.bootstrap(
        di_container=root,
        commands_mapper=_commands_mapper,
        scope_strategy=ScopeStrategy.HANDLER,
    )
    async with root.open_scope() as scoped:
        outer_uow = await scoped.resolve(FakeUoW)
        async with bind_scope(scoped):
            await mediator.send(Command())
            assert current_container() is scoped
        assert outer_uow.closed is False

    assert len(UOW_INSTANCES) == 2
    handler_uow = UOW_INSTANCES[1]
    assert handler_uow is not outer_uow
    # The nested handler scope is finalized independently of the outer one
    assert handler_uow.closed is True
    assert handler_uow.commits == [handler_uow.name]
    assert outer_uow.closed is True


@pytest.mark.asyncio
async def test_contextvar_isolation_under_gather() -> None:
    """Parallel event handlers with HANDLER strategy must not share UoW under gather."""
    _reset()
    ids: list[int] = []

    class SameEvent(cqrs.DomainEvent, frozen=True):
        pass

    class HandlerA(cqrs.EventHandler[SameEvent]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return []

        async def handle(self, event: SameEvent) -> None:
            await asyncio.sleep(0.01)
            ids.append(id(self.uow))

    class HandlerB(cqrs.EventHandler[SameEvent]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return []

        async def handle(self, event: SameEvent) -> None:
            await asyncio.sleep(0.01)
            ids.append(id(self.uow))

    event_map = cqrs.EventMap()
    event_map.bind(SameEvent, HandlerA)
    event_map.bind(SameEvent, HandlerB)
    emitter = cqrs.EventEmitter(
        event_map=event_map,
        container=_saga_di_container(),
        scope_strategy=ScopeStrategy.HANDLER,
    )
    await emitter.emit(SameEvent())
    assert len(ids) == 2
    assert ids[0] != ids[1]
    assert len(UOW_INSTANCES) == 2


@pytest.mark.asyncio
async def test_none_strategy_uses_one_shot_resolve() -> None:
    """With NONE, each resolve opens a one-shot scope (legacy DIContainer)."""
    _reset()
    mediator = bootstrap.bootstrap(
        di_container=_di_container(),
        commands_mapper=_commands_mapper,
        domain_events_mapper=_events_mapper,
        scope_strategy=ScopeStrategy.NONE,
    )
    await mediator.send(Command())
    # Command handler and event handler each get one-shot scopes
    assert len(UOW_INSTANCES) == 2
    assert UOW_INSTANCES[0].closed and UOW_INSTANCES[1].closed


@pytest.mark.asyncio
async def test_stream_send_shares_uow() -> None:
    """SEND keeps one UoW for the whole stream lifetime."""
    _reset()
    mediator = bootstrap.bootstrap_streaming(
        di_container=_di_container(),
        commands_mapper=_stream_mapper,
        scope_strategy=ScopeStrategy.SEND,
    )
    chunks = [chunk async for chunk in mediator.stream(StreamCommand())]
    assert chunks == [StreamChunk(value="one"), StreamChunk(value="two")]
    assert len(UOW_INSTANCES) == 1
    assert UOW_INSTANCES[0].closed is True
    assert LIFECYCLE == ["enter", "handle", "after_yield", "exit"]


def _fallback_handlers() -> tuple[type, type]:
    class PrimaryBoom(cqrs.RequestHandler[Command, None]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return []

        async def handle(self, request: Command) -> None:
            LIFECYCLE.append("primary")
            raise RuntimeError("primary failed")

    class FallbackOk(cqrs.RequestHandler[Command, None]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            FallbackOk.seen_uow = uow  # type: ignore[attr-defined]

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return []

        async def handle(self, request: Command) -> None:
            LIFECYCLE.append("fallback")
            await self.uow.commit()

    return PrimaryBoom, FallbackOk


@pytest.mark.asyncio
async def test_request_fallback_under_handler_strategy() -> None:
    """HANDLER rolls back the primary UoW, then fallback runs in a new scope."""
    _reset()
    primary_cls, fallback_cls = _fallback_handlers()

    def mapper(m: cqrs.RequestMap) -> None:
        m.bind(Command, cqrs.RequestHandlerFallback(primary_cls, fallback_cls))

    mediator = bootstrap.bootstrap(
        di_container=_tracking_di_container(),
        commands_mapper=mapper,
        scope_strategy=ScopeStrategy.HANDLER,
    )
    await mediator.send(Command())
    assert LIFECYCLE == [
        "enter",
        "primary",
        "rollback",
        "exit",
        "enter",
        "fallback",
        "commit",
        "exit",
    ]
    assert len(UOW_INSTANCES) == 2
    assert UOW_INSTANCES[0].rolled_back is True
    assert UOW_INSTANCES[0].committed is False
    assert UOW_INSTANCES[1].committed is True
    assert UOW_INSTANCES[1].rolled_back is False
    assert fallback_cls.seen_uow is UOW_INSTANCES[1]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_request_fallback_under_send_strategy() -> None:
    """SEND keeps primary and fallback in one UoW; primary error does not roll it back."""
    _reset()
    primary_cls, fallback_cls = _fallback_handlers()

    def mapper(m: cqrs.RequestMap) -> None:
        m.bind(Command, cqrs.RequestHandlerFallback(primary_cls, fallback_cls))

    mediator = bootstrap.bootstrap(
        di_container=_tracking_di_container(),
        commands_mapper=mapper,
        scope_strategy=ScopeStrategy.SEND,
    )
    await mediator.send(Command())
    assert LIFECYCLE == ["enter", "primary", "fallback", "commit", "exit"]
    assert len(UOW_INSTANCES) == 1
    assert UOW_INSTANCES[0].rolled_back is False
    assert UOW_INSTANCES[0].committed is True
    assert fallback_cls.seen_uow is UOW_INSTANCES[0]  # type: ignore[attr-defined]


def _event_fallback_handlers() -> tuple[type, type]:
    class PrimaryBoomEvent(cqrs.EventHandler[DomainEvt]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return list(self._events)

        async def handle(self, event: DomainEvt) -> None:
            LIFECYCLE.append("primary")
            raise RuntimeError("primary failed")

    class FallbackOkEvent(cqrs.EventHandler[DomainEvt]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return list(self._events)

        async def handle(self, event: DomainEvt) -> None:
            LIFECYCLE.append("fallback")
            await self.uow.commit()

    return PrimaryBoomEvent, FallbackOkEvent


@pytest.mark.asyncio
async def test_event_dispatcher_fallback_under_handler_strategy() -> None:
    _reset()
    primary_cls, fallback_cls = _event_fallback_handlers()
    event_map = cqrs.EventMap()
    event_map.bind(DomainEvt, cqrs.EventHandlerFallback(primary_cls, fallback_cls))
    mediator = cqrs.EventMediator(
        event_map=event_map,
        container=_tracking_di_container(),
        scope_strategy=ScopeStrategy.HANDLER,
    )
    await mediator.send(DomainEvt())
    assert LIFECYCLE == [
        "enter",
        "primary",
        "rollback",
        "exit",
        "enter",
        "fallback",
        "commit",
        "exit",
    ]
    assert UOW_INSTANCES[0].rolled_back is True
    assert UOW_INSTANCES[0].committed is False
    assert UOW_INSTANCES[1].committed is True


@pytest.mark.asyncio
async def test_event_emitter_fallback_under_handler_strategy() -> None:
    _reset()
    primary_cls, fallback_cls = _event_fallback_handlers()
    event_map = cqrs.EventMap()
    event_map.bind(DomainEvt, cqrs.EventHandlerFallback(primary_cls, fallback_cls))
    emitter = cqrs.EventEmitter(
        event_map=event_map,
        container=_tracking_di_container(),
        scope_strategy=ScopeStrategy.HANDLER,
    )
    await emitter.emit(DomainEvt())
    assert LIFECYCLE == [
        "enter",
        "primary",
        "rollback",
        "exit",
        "enter",
        "fallback",
        "commit",
        "exit",
    ]
    assert UOW_INSTANCES[0].rolled_back is True
    assert UOW_INSTANCES[1].committed is True


@pytest.mark.asyncio
async def test_event_fallback_under_send_strategy() -> None:
    _reset()
    primary_cls, fallback_cls = _event_fallback_handlers()
    event_map = cqrs.EventMap()
    event_map.bind(DomainEvt, cqrs.EventHandlerFallback(primary_cls, fallback_cls))
    mediator = cqrs.EventMediator(
        event_map=event_map,
        container=_tracking_di_container(),
        scope_strategy=ScopeStrategy.SEND,
    )
    await mediator.send(DomainEvt())
    assert LIFECYCLE == ["enter", "primary", "fallback", "commit", "exit"]
    assert len(UOW_INSTANCES) == 1
    assert UOW_INSTANCES[0].rolled_back is False
    assert UOW_INSTANCES[0].committed is True


@pytest.mark.asyncio
async def test_stream_fallback_under_handler_strategy() -> None:
    _reset()

    class PrimaryBoomStream(cqrs.StreamingRequestHandler[StreamCommand, StreamChunk]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return list(self._events)

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(self, request: StreamCommand) -> typing.AsyncIterator[StreamChunk]:
            LIFECYCLE.append("primary")
            raise RuntimeError("primary failed")
            yield StreamChunk(value="unreachable")  # noqa: B901

    class FallbackOkStream(cqrs.StreamingRequestHandler[StreamCommand, StreamChunk]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return list(self._events)

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(self, request: StreamCommand) -> typing.AsyncIterator[StreamChunk]:
            LIFECYCLE.append("fallback")
            yield StreamChunk(value="ok")

    def mapper(m: cqrs.RequestMap) -> None:
        m.bind(StreamCommand, cqrs.RequestHandlerFallback(PrimaryBoomStream, FallbackOkStream))

    mediator = bootstrap.bootstrap_streaming(
        di_container=_tracking_di_container(),
        commands_mapper=mapper,
        scope_strategy=ScopeStrategy.HANDLER,
    )
    chunks = [chunk async for chunk in mediator.stream(StreamCommand())]
    assert chunks == [StreamChunk(value="ok")]
    assert LIFECYCLE == [
        "enter",
        "primary",
        "rollback",
        "exit",
        "enter",
        "fallback",
        "commit",
        "exit",
    ]
    assert UOW_INSTANCES[0].rolled_back is True
    assert UOW_INSTANCES[1].committed is True


@pytest.mark.asyncio
async def test_stream_fallback_under_send_strategy() -> None:
    _reset()

    class PrimaryBoomStream(cqrs.StreamingRequestHandler[StreamCommand, StreamChunk]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return list(self._events)

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(self, request: StreamCommand) -> typing.AsyncIterator[StreamChunk]:
            LIFECYCLE.append("primary")
            raise RuntimeError("primary failed")
            yield StreamChunk(value="unreachable")  # noqa: B901

    class FallbackOkStream(cqrs.StreamingRequestHandler[StreamCommand, StreamChunk]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return list(self._events)

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(self, request: StreamCommand) -> typing.AsyncIterator[StreamChunk]:
            LIFECYCLE.append("fallback")
            yield StreamChunk(value="ok")

    def mapper(m: cqrs.RequestMap) -> None:
        m.bind(StreamCommand, cqrs.RequestHandlerFallback(PrimaryBoomStream, FallbackOkStream))

    mediator = bootstrap.bootstrap_streaming(
        di_container=_tracking_di_container(),
        commands_mapper=mapper,
        scope_strategy=ScopeStrategy.SEND,
    )
    chunks = [chunk async for chunk in mediator.stream(StreamCommand())]
    assert chunks == [StreamChunk(value="ok")]
    assert LIFECYCLE == ["enter", "primary", "fallback", "commit", "exit"]
    assert len(UOW_INSTANCES) == 1
    assert UOW_INSTANCES[0].rolled_back is False
    assert UOW_INSTANCES[0].committed is True


@pytest.mark.asyncio
async def test_event_emitter_strategy_mismatch_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _reset()
    container = DIContainer()
    container.attach_external_container(_di_container())
    event_map = cqrs.EventMap()
    event_map.bind(DomainEvt, EventHandler)
    emitter = cqrs.EventEmitter(
        event_map=event_map,
        container=container,
        scope_strategy=ScopeStrategy.HANDLER,
    )
    request_map = cqrs.RequestMap()
    request_map.bind(Command, CommandHandler)

    with caplog.at_level("WARNING", logger="cqrs"):
        cqrs.RequestMediator(
            request_map=request_map,
            container=container,
            event_emitter=emitter,
            event_map=event_map,
            concurrent_event_handle_enable=False,
            scope_strategy=ScopeStrategy.SEND,
        )
    assert any("scope_strategy" in record.message for record in caplog.records)

    caplog.clear()
    with caplog.at_level("WARNING", logger="cqrs"):
        cqrs.RequestMediator(
            request_map=request_map,
            container=container,
            event_emitter=emitter,
            event_map=event_map,
            scope_strategy=ScopeStrategy.HANDLER,
        )
    assert not [record for record in caplog.records if "scope_strategy" in record.message]


@pytest.mark.asyncio
async def test_streaming_mediator_emitter_strategy_mismatch_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _reset()
    container = DIContainer()
    container.attach_external_container(_di_container())
    event_map = cqrs.EventMap()
    event_map.bind(DomainEvt, EventHandler)
    emitter = cqrs.EventEmitter(
        event_map=event_map,
        container=container,
        scope_strategy=ScopeStrategy.HANDLER,
    )
    request_map = cqrs.RequestMap()
    request_map.bind(StreamCommand, StreamHandler)

    with caplog.at_level("WARNING", logger="cqrs"):
        cqrs.StreamingRequestMediator(
            request_map=request_map,
            container=container,
            event_emitter=emitter,
            event_map=event_map,
            concurrent_event_handle_enable=False,
            scope_strategy=ScopeStrategy.SEND,
        )
    assert any("scope_strategy" in record.message for record in caplog.records)

    caplog.clear()
    with caplog.at_level("WARNING", logger="cqrs"):
        cqrs.StreamingRequestMediator(
            request_map=request_map,
            container=container,
            event_emitter=emitter,
            event_map=event_map,
            scope_strategy=ScopeStrategy.HANDLER,
        )
    assert not [record for record in caplog.records if "scope_strategy" in record.message]


@pytest.mark.asyncio
async def test_saga_mediator_emitter_strategy_mismatch_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _reset()
    container = DIContainer()
    container.attach_external_container(_di_container())
    event_map = cqrs.EventMap()
    event_map.bind(DomainEvt, EventHandler)
    emitter = cqrs.EventEmitter(
        event_map=event_map,
        container=container,
        scope_strategy=ScopeStrategy.HANDLER,
    )
    saga_map = cqrs.SagaMap()
    saga_map.bind(ScopedSagaContext, ScopedSaga)

    with caplog.at_level("WARNING", logger="cqrs"):
        cqrs.SagaMediator(
            saga_map=saga_map,
            container=container,
            event_emitter=emitter,
            event_map=event_map,
            concurrent_event_handle_enable=False,
            scope_strategy=ScopeStrategy.SEND,
        )
    assert any("scope_strategy" in record.message for record in caplog.records)

    caplog.clear()
    with caplog.at_level("WARNING", logger="cqrs"):
        cqrs.SagaMediator(
            saga_map=saga_map,
            container=container,
            event_emitter=emitter,
            event_map=event_map,
            scope_strategy=ScopeStrategy.HANDLER,
        )
    assert not [record for record in caplog.records if "scope_strategy" in record.message]


# ---------------------------------------------------------------------------
# Saga SEND / HANDLER
# ---------------------------------------------------------------------------


@dataclass
class ScopedSagaContext(SagaContext):
    order_id: str = "1"


class StepDone(cqrs.DomainEvent, frozen=True):
    step: str = ""


class ReserveStep(cqrs.SagaStepHandler[ScopedSagaContext, None]):
    def __init__(self, uow: FakeUoW) -> None:
        self.uow = uow
        self._events: list[cqrs.Event] = []
        self.compensate_called = False

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return list(self._events)

    async def act(self, context: ScopedSagaContext) -> SagaStepResult[ScopedSagaContext, None]:
        LIFECYCLE.append("reserve")
        await self.uow.commit()
        self._events.append(StepDone(step="reserve"))
        return self._generate_step_result(None)

    async def compensate(self, context: ScopedSagaContext) -> None:
        self.compensate_called = True
        LIFECYCLE.append("compensate_reserve")


class ChargeStep(cqrs.SagaStepHandler[ScopedSagaContext, None]):
    def __init__(self, uow: FakeUoW) -> None:
        self.uow = uow
        self._events: list[cqrs.Event] = []
        self.compensate_called = False

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return list(self._events)

    async def act(self, context: ScopedSagaContext) -> SagaStepResult[ScopedSagaContext, None]:
        LIFECYCLE.append("charge")
        await self.uow.commit()
        self._events.append(StepDone(step="charge"))
        return self._generate_step_result(None)

    async def compensate(self, context: ScopedSagaContext) -> None:
        self.compensate_called = True
        LIFECYCLE.append("compensate_charge")


class BoomStep(cqrs.SagaStepHandler[ScopedSagaContext, None]):
    def __init__(self, uow: FakeUoW) -> None:
        self.uow = uow
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return list(self._events)

    async def act(self, context: ScopedSagaContext) -> SagaStepResult[ScopedSagaContext, None]:
        LIFECYCLE.append("boom")
        raise RuntimeError("saga boom")

    async def compensate(self, context: ScopedSagaContext) -> None:
        pass


class CompensationRecord(typing.NamedTuple):
    step: object
    uow: FakeUoW
    uow_closed: bool


COMPENSATE_LOG: list[CompensationRecord] = []


class ObservedReserveStep(cqrs.SagaStepHandler[ScopedSagaContext, None]):
    """Reserve step that records which UoW ``compensate`` actually received."""

    instances: typing.ClassVar[list["ObservedReserveStep"]] = []

    def __init__(self, uow: FakeUoW) -> None:
        self.uow = uow
        self._events: list[cqrs.Event] = []
        ObservedReserveStep.instances.append(self)

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return list(self._events)

    async def act(self, context: ScopedSagaContext) -> SagaStepResult[ScopedSagaContext, None]:
        LIFECYCLE.append("reserve")
        await self.uow.commit()
        self._events.append(StepDone(step="reserve"))
        return self._generate_step_result(None)

    async def compensate(self, context: ScopedSagaContext) -> None:
        LIFECYCLE.append("compensate_reserve")
        COMPENSATE_LOG.append(
            CompensationRecord(step=self, uow=self.uow, uow_closed=self.uow.closed),
        )
        await self.uow.commit()


class ScopedSaga(cqrs.Saga[ScopedSagaContext]):
    steps = [ReserveStep, ChargeStep]


class BoomSaga(cqrs.Saga[ScopedSagaContext]):
    steps = [ReserveStep, BoomStep]


class ObservedBoomSaga(cqrs.Saga[ScopedSagaContext]):
    steps = [ObservedReserveStep, BoomStep]


def _saga_di_container() -> ScopeAwareContainer:
    external = _di_container()
    cqrs_c = DIContainer()
    cqrs_c.attach_external_container(external)
    return ScopeAwareContainer(cqrs_c)


@pytest.mark.asyncio
async def test_saga_send_shares_uow_across_steps() -> None:
    _reset()
    container = _saga_di_container()
    saga = ScopedSaga()
    results = []
    async with enter_scope(container):
        async with saga.transaction(
            context=ScopedSagaContext(order_id="ord-1"),
            container=container,
            storage=MemorySagaStorage(),
            scope_strategy=ScopeStrategy.SEND,
        ) as tx:
            results = [r async for r in tx]
    assert len(results) == 2
    assert len(UOW_INSTANCES) == 1
    assert UOW_INSTANCES[0].commits == ["uow-0", "uow-0"]
    assert UOW_INSTANCES[0].closed is True
    assert LIFECYCLE.count("reserve") == 1
    assert LIFECYCLE.count("charge") == 1


@pytest.mark.asyncio
async def test_saga_handler_reuses_executed_step_for_events() -> None:
    """HANDLER must keep the instance that ran act for events / completed_steps."""
    _reset()
    container = _saga_di_container()
    saga = BoomSaga()
    tx = None
    with pytest.raises(RuntimeError, match="saga boom"):
        async with saga.transaction(
            context=ScopedSagaContext(order_id="ord-2"),
            container=container,
            storage=MemorySagaStorage(),
            scope_strategy=ScopeStrategy.HANDLER,
        ) as tx:
            async for _ in tx:
                pass

    assert tx is not None

    # Three scopes: ReserveStep act, BoomStep act, ReserveStep compensate
    assert len(UOW_INSTANCES) == 3
    assert all(uow.closed for uow in UOW_INSTANCES)
    assert LIFECYCLE == [
        "enter",
        "reserve",
        "exit",
        "enter",
        "boom",
        "exit",
        "enter",
        "compensate_reserve",
        "exit",
    ]

    # Events / completed_steps must come from the executed ReserveStep instance
    completed = tx.completed_steps
    assert len(completed) == 1
    reserve = completed[0]
    assert isinstance(reserve, ReserveStep)
    assert any(isinstance(e, StepDone) and e.step == "reserve" for e in reserve.events)
    assert reserve.uow is UOW_INSTANCES[0]
    # compensate() ran on a freshly resolved instance, not on this one
    assert reserve.compensate_called is False


@pytest.mark.asyncio
async def test_saga_handler_compensates_with_live_uow() -> None:
    """Under HANDLER the step is re-resolved so compensate() gets a live UoW."""
    _reset()
    container = _saga_di_container()
    saga = ObservedBoomSaga()
    tx = None
    with pytest.raises(RuntimeError, match="saga boom"):
        async with saga.transaction(
            context=ScopedSagaContext(order_id="ord-live"),
            container=container,
            storage=MemorySagaStorage(),
            scope_strategy=ScopeStrategy.HANDLER,
        ) as tx:
            async for _ in tx:
                pass

    assert tx is not None

    assert len(COMPENSATE_LOG) == 1
    record = COMPENSATE_LOG[0]
    # The UoW was alive while compensating and finalized right after
    assert record.uow_closed is False
    assert record.uow.closed is True
    assert record.uow.commits == ["uow-2"]
    # Fresh instance and fresh UoW, not the ones that ran act
    acted = ObservedReserveStep.instances[0]
    assert record.step is not acted
    assert record.uow is not acted.uow
    assert record.uow is UOW_INSTANCES[2]
    assert tx.completed_steps == [acted]


@pytest.mark.asyncio
async def test_saga_send_compensates_with_same_instance_and_uow() -> None:
    """Under SEND compensation stays on the instance and UoW that ran act."""
    _reset()
    container = _saga_di_container()
    saga = ObservedBoomSaga()
    async with enter_scope(container):
        with pytest.raises(RuntimeError, match="saga boom"):
            async with saga.transaction(
                context=ScopedSagaContext(order_id="ord-send"),
                container=container,
                storage=MemorySagaStorage(),
                scope_strategy=ScopeStrategy.SEND,
            ) as tx:
                async for _ in tx:
                    pass
        assert len(COMPENSATE_LOG) == 1
        record = COMPENSATE_LOG[0]
        assert record.uow_closed is False

    assert len(UOW_INSTANCES) == 1
    acted = ObservedReserveStep.instances[0]
    assert record.step is acted
    assert record.uow is acted.uow is UOW_INSTANCES[0]
    assert UOW_INSTANCES[0].closed is True
    assert UOW_INSTANCES[0].commits == ["uow-0", "uow-0"]


async def _seed_saga(
    storage: MemorySagaStorage,
    saga_id: uuid.UUID,
    context: ScopedSagaContext,
    status: SagaStatus,
) -> None:
    """Persist a saga whose first step already finished ``act``."""
    await storage.create_saga(saga_id, ObservedBoomSaga.__name__, context.to_dict())
    await storage.log_step(
        saga_id,
        ObservedReserveStep.__name__,
        "act",
        SagaStepStatus.COMPLETED,
    )
    await storage.update_status(saga_id, status)


@pytest.mark.asyncio
async def test_saga_handler_recovery_compensates_with_live_uow() -> None:
    """Steps reconstructed during recovery are compensated in a live scope."""
    _reset()
    container = _saga_di_container()
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()
    context = ScopedSagaContext(order_id="ord-recovery")
    await _seed_saga(storage, saga_id, context, SagaStatus.COMPENSATING)

    with pytest.raises(RuntimeError, match="compensation was completed"):
        async with ObservedBoomSaga().transaction(
            context=context,
            container=container,
            storage=storage,
            saga_id=saga_id,
            scope_strategy=ScopeStrategy.HANDLER,
        ) as tx:
            async for _ in tx:
                pass

    assert len(COMPENSATE_LOG) == 1
    record = COMPENSATE_LOG[0]
    assert record.uow_closed is False
    assert record.uow.closed is True
    assert record.uow.commits == [record.uow.name]


@pytest.mark.asyncio
async def test_saga_handler_skipped_step_compensates_with_live_uow() -> None:
    """A skipped (already completed) step is compensated with fresh dependencies."""
    _reset()
    container = _saga_di_container()
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()
    context = ScopedSagaContext(order_id="ord-skip")
    await _seed_saga(storage, saga_id, context, SagaStatus.RUNNING)

    tx = None
    with pytest.raises(RuntimeError, match="saga boom"):
        async with ObservedBoomSaga().transaction(
            context=context,
            container=container,
            storage=storage,
            saga_id=saga_id,
            scope_strategy=ScopeStrategy.HANDLER,
        ) as tx:
            async for _ in tx:
                pass

    assert tx is not None

    assert "reserve" not in LIFECYCLE  # act was skipped
    assert LIFECYCLE == [
        "enter",
        "boom",
        "exit",
        "enter",
        "compensate_reserve",
        "exit",
    ]
    assert len(COMPENSATE_LOG) == 1
    record = COMPENSATE_LOG[0]
    assert record.uow_closed is False
    assert record.uow.closed is True
    assert isinstance(tx.completed_steps[0], SagaStepRef)
    assert record.step is ObservedReserveStep.instances[0]


@pytest.mark.asyncio
async def test_saga_transaction_forwards_scope_strategy() -> None:
    """Saga.transaction() must accept scope_strategy like SagaMediator."""
    _reset()
    container = _saga_di_container()
    saga = ScopedSaga()
    async with saga.transaction(
        context=ScopedSagaContext(order_id="ord-3"),
        container=container,
        storage=MemorySagaStorage(),
        scope_strategy=ScopeStrategy.HANDLER,
    ) as tx:
        async for _ in tx:
            pass
    assert len(UOW_INSTANCES) == 2
    assert UOW_INSTANCES[0] is not UOW_INSTANCES[1]


@pytest.mark.asyncio
async def test_recover_saga_handler_compensates_with_live_uow() -> None:
    """recover_saga(HANDLER) on a COMPENSATING saga re-resolves compensate in a live scope."""
    _reset()
    container = _saga_di_container()
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()
    context = ScopedSagaContext(order_id="ord-recover-handler")
    await _seed_saga(storage, saga_id, context, SagaStatus.COMPENSATING)

    with pytest.raises(RuntimeError, match="compensation was completed"):
        await recover_saga(
            ObservedBoomSaga(),
            saga_id,
            ScopedSagaContext,
            container,
            storage,
            scope_strategy=ScopeStrategy.HANDLER,
        )

    assert LIFECYCLE == ["enter", "compensate_reserve", "exit"]
    assert len(COMPENSATE_LOG) == 1
    record = COMPENSATE_LOG[0]
    assert record.uow_closed is False
    assert record.uow.closed is True


@pytest.mark.asyncio
async def test_recover_saga_send_skip_and_compensate_share_uow() -> None:
    """recover_saga(SEND) after a partial run shares one UoW for skip+compensate."""
    _reset()
    container = _plain_di_container()
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()
    context = ScopedSagaContext(order_id="ord-recover-send")
    await _seed_saga(storage, saga_id, context, SagaStatus.RUNNING)

    with pytest.raises(RuntimeError, match="saga boom"):
        await recover_saga(
            ObservedBoomSaga(),
            saga_id,
            ScopedSagaContext,
            container,
            storage,
            scope_strategy=ScopeStrategy.SEND,
        )

    assert "reserve" not in LIFECYCLE
    assert len(UOW_INSTANCES) == 1
    assert len(COMPENSATE_LOG) == 1
    record = COMPENSATE_LOG[0]
    assert record.uow is UOW_INSTANCES[0]
    assert record.uow_closed is False
    assert UOW_INSTANCES[0].closed is True


@pytest.mark.asyncio
async def test_enter_scope_plus_transaction_send_shares_uow_plain_container() -> None:
    """Direct enter_scope + transaction(SEND) on a plain DIContainer shares one UoW."""
    _reset()
    container = _plain_di_container()
    saga = ScopedSaga()
    results = []
    async with enter_scope(container):
        async with saga.transaction(
            context=ScopedSagaContext(order_id="ord-plain-send"),
            container=container,
            storage=MemorySagaStorage(),
            scope_strategy=ScopeStrategy.SEND,
        ) as tx:
            results = [r async for r in tx]
    assert len(results) == 2
    assert len(UOW_INSTANCES) == 1
    assert UOW_INSTANCES[0].closed is True


@pytest.mark.asyncio
async def test_recover_saga_handler_plain_container_does_not_double_open() -> None:
    """recover_saga(HANDLER) on a plain container opens one compensate scope, not two."""
    _reset()
    container = _plain_di_container()
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()
    context = ScopedSagaContext(order_id="ord-recover-handler-plain")
    await _seed_saga(storage, saga_id, context, SagaStatus.COMPENSATING)

    with pytest.raises(RuntimeError, match="compensation was completed"):
        await recover_saga(
            ObservedBoomSaga(),
            saga_id,
            ScopedSagaContext,
            container,
            storage,
            scope_strategy=ScopeStrategy.HANDLER,
        )

    assert LIFECYCLE == ["enter", "compensate_reserve", "exit"]
    assert len(UOW_INSTANCES) == 1


def test_request_mediator_send_rejects_concurrent_events() -> None:
    request_map = cqrs.RequestMap()
    request_map.bind(Command, CommandHandler)
    with pytest.raises(ValueError, match="concurrent_event_handle_enable"):
        cqrs.RequestMediator(
            request_map=request_map,
            container=DIContainer(),
            scope_strategy=ScopeStrategy.SEND,
            concurrent_event_handle_enable=True,
        )


def test_request_mediator_send_defaults_are_sequential() -> None:
    request_map = cqrs.RequestMap()
    request_map.bind(Command, CommandHandler)
    mediator = cqrs.RequestMediator(
        request_map=request_map,
        container=DIContainer(),
        scope_strategy=ScopeStrategy.SEND,
    )
    assert mediator._event_processor._concurrent_event_handle_enable is False


def test_request_mediator_default_is_concurrent() -> None:
    request_map = cqrs.RequestMap()
    request_map.bind(Command, CommandHandler)
    mediator = cqrs.RequestMediator(
        request_map=request_map,
        container=DIContainer(),
    )
    assert mediator._event_processor._concurrent_event_handle_enable is True


def test_streaming_mediator_send_defaults_are_sequential() -> None:
    request_map = cqrs.RequestMap()
    request_map.bind(StreamCommand, StreamHandler)
    mediator = cqrs.StreamingRequestMediator(
        request_map=request_map,
        container=DIContainer(),
        scope_strategy=ScopeStrategy.SEND,
    )
    assert mediator._event_processor._concurrent_event_handle_enable is False


def test_streaming_mediator_send_rejects_explicit_concurrent() -> None:
    request_map = cqrs.RequestMap()
    request_map.bind(StreamCommand, StreamHandler)
    with pytest.raises(ValueError, match="concurrent_event_handle_enable"):
        cqrs.StreamingRequestMediator(
            request_map=request_map,
            container=DIContainer(),
            scope_strategy=ScopeStrategy.SEND,
            concurrent_event_handle_enable=True,
            max_concurrent_event_handlers=1,
        )


def test_bootstrap_send_rejects_concurrent_events() -> None:
    with pytest.raises(ValueError, match="concurrent_event_handle_enable"):
        bootstrap.bootstrap(
            di_container=_di_container(),
            commands_mapper=_commands_mapper,
            scope_strategy=ScopeStrategy.SEND,
            concurrent_event_handle_enable=True,
        )


@pytest.mark.asyncio
async def test_bootstrap_send_with_sequential_events_ok() -> None:
    _reset()
    mediator = bootstrap.bootstrap(
        di_container=_di_container(),
        commands_mapper=_commands_mapper,
        scope_strategy=ScopeStrategy.SEND,
        concurrent_event_handle_enable=False,
    )
    await mediator.send(Command())
    assert UOW_INSTANCES[0].closed is True


def test_saga_mediator_send_defaults_are_sequential() -> None:
    saga_map = cqrs.SagaMap()
    saga_map.bind(ScopedSagaContext, ScopedSaga)
    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=DIContainer(),
        scope_strategy=ScopeStrategy.SEND,
    )
    assert mediator._event_processor._concurrent_event_handle_enable is False


def test_saga_mediator_send_rejects_explicit_concurrent() -> None:
    saga_map = cqrs.SagaMap()
    saga_map.bind(ScopedSagaContext, ScopedSaga)
    with pytest.raises(ValueError, match="concurrent_event_handle_enable"):
        cqrs.SagaMediator(
            saga_map=saga_map,
            container=DIContainer(),
            scope_strategy=ScopeStrategy.SEND,
            concurrent_event_handle_enable=True,
        )


def test_saga_bootstrap_send_defaults_are_sequential() -> None:
    from cqrs.bootstrap import saga as saga_bootstrap

    mediator = saga_bootstrap.bootstrap(
        di_container=di.Container(),
        scope_strategy=ScopeStrategy.SEND,
    )
    assert mediator._event_processor._concurrent_event_handle_enable is False


def test_setup_mediator_send_defaults_are_sequential() -> None:
    container = DIContainer()
    container.attach_external_container(di.Container())
    emitter = bootstrap.setup_event_emitter(container)
    mediator = bootstrap.setup_mediator(
        emitter,
        container,
        middlewares=[],
        scope_strategy=ScopeStrategy.SEND,
    )
    assert mediator._event_processor._concurrent_event_handle_enable is False


def test_setup_streaming_mediator_send_defaults_are_sequential() -> None:
    container = DIContainer()
    container.attach_external_container(di.Container())
    emitter = bootstrap.setup_event_emitter(container)
    mediator = bootstrap.setup_streaming_mediator(
        emitter,
        container,
        middlewares=[],
        scope_strategy=ScopeStrategy.SEND,
    )
    assert mediator._event_processor._concurrent_event_handle_enable is False


def test_wrap_container_is_internal() -> None:
    assert "wrap_container" not in cqrs.__all__
    from cqrs.container import scope as scope_mod

    assert hasattr(scope_mod, "wrap_container")


@pytest.mark.asyncio
async def test_event_emitter_send_runs_handlers_sequentially() -> None:
    """SEND must not gather handlers of one event so they can share one UoW."""
    _reset()
    in_flight = 0
    max_in_flight = 0
    uow_ids: list[int] = []

    class SameEvent(cqrs.DomainEvent, frozen=True):
        pass

    class HandlerA(cqrs.EventHandler[SameEvent]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return []

        async def handle(self, event: SameEvent) -> None:
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await asyncio.sleep(0.02)
            uow_ids.append(id(self.uow))
            in_flight -= 1

    class HandlerB(cqrs.EventHandler[SameEvent]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return []

        async def handle(self, event: SameEvent) -> None:
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await asyncio.sleep(0.02)
            uow_ids.append(id(self.uow))
            in_flight -= 1

    container = _saga_di_container()
    event_map = cqrs.EventMap()
    event_map.bind(SameEvent, HandlerA)
    event_map.bind(SameEvent, HandlerB)
    emitter = cqrs.EventEmitter(
        event_map=event_map,
        container=container,
        scope_strategy=ScopeStrategy.SEND,
    )
    async with enter_scope(container):
        await emitter.emit(SameEvent())
    assert max_in_flight == 1
    assert len(uow_ids) == 2
    assert uow_ids[0] == uow_ids[1]
    assert len(UOW_INSTANCES) == 1


@pytest.mark.asyncio
async def test_event_emitter_handler_gather_can_overlap() -> None:
    """HANDLER still uses gather, so two handlers of one event may overlap."""
    _reset()
    in_flight = 0
    max_in_flight = 0

    class SameEvent(cqrs.DomainEvent, frozen=True):
        pass

    class HandlerA(cqrs.EventHandler[SameEvent]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return []

        async def handle(self, event: SameEvent) -> None:
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await asyncio.sleep(0.02)
            in_flight -= 1

    class HandlerB(cqrs.EventHandler[SameEvent]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return []

        async def handle(self, event: SameEvent) -> None:
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await asyncio.sleep(0.02)
            in_flight -= 1

    event_map = cqrs.EventMap()
    event_map.bind(SameEvent, HandlerA)
    event_map.bind(SameEvent, HandlerB)
    emitter = cqrs.EventEmitter(
        event_map=event_map,
        container=_saga_di_container(),
        scope_strategy=ScopeStrategy.HANDLER,
    )
    await emitter.emit(SameEvent())
    assert max_in_flight == 2
    assert len(UOW_INSTANCES) == 2


@pytest.mark.asyncio
async def test_compensation_retry_opens_fresh_handler_scope() -> None:
    """Each HANDLER compensate retry gets a new live UoW; the failed attempt is already closed."""
    _reset()

    class FlakyReserveStep(cqrs.SagaStepHandler[ScopedSagaContext, None]):
        attempts: typing.ClassVar[int] = 0
        first_uow: typing.ClassVar[FakeUoW | None] = None
        second_uow: typing.ClassVar[FakeUoW | None] = None

        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return list(self._events)

        async def act(
            self,
            context: ScopedSagaContext,
        ) -> SagaStepResult[ScopedSagaContext, None]:
            LIFECYCLE.append("reserve")
            return self._generate_step_result(None)

        async def compensate(self, context: ScopedSagaContext) -> None:
            FlakyReserveStep.attempts += 1
            if FlakyReserveStep.attempts == 1:
                FlakyReserveStep.first_uow = self.uow
                raise RuntimeError("compensate failed once")
            assert FlakyReserveStep.first_uow is not None
            assert FlakyReserveStep.first_uow.closed is True
            assert self.uow.closed is False
            FlakyReserveStep.second_uow = self.uow
            LIFECYCLE.append("compensate_retry")

    class FlakyBoomSaga(cqrs.Saga[ScopedSagaContext]):
        steps = [FlakyReserveStep, BoomStep]

    FlakyReserveStep.attempts = 0
    FlakyReserveStep.first_uow = None
    FlakyReserveStep.second_uow = None

    tx = None
    with pytest.raises(RuntimeError, match="saga boom"):
        async with FlakyBoomSaga().transaction(
            context=ScopedSagaContext(order_id="ord-retry"),
            container=_saga_di_container(),
            storage=MemorySagaStorage(),
            compensation_retry_count=2,
            compensation_retry_delay=0.0,
            scope_strategy=ScopeStrategy.HANDLER,
        ) as tx:
            async for _ in tx:
                pass

    assert tx is not None

    assert FlakyReserveStep.attempts == 2
    assert FlakyReserveStep.first_uow is not None
    assert FlakyReserveStep.second_uow is not None
    assert FlakyReserveStep.first_uow is not FlakyReserveStep.second_uow
    assert FlakyReserveStep.first_uow.closed is True
    assert FlakyReserveStep.second_uow.closed is True
    assert "compensate_retry" in LIFECYCLE
    assert tx.completed_steps  # reserve ran before boom


@pytest.mark.asyncio
async def test_saga_transaction_send_without_scope_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _reset()
    container = _saga_di_container()
    saga = ScopedSaga()
    with caplog.at_level("WARNING", logger="cqrs.saga"):
        async with saga.transaction(
            context=ScopedSagaContext(order_id="ord-warn"),
            container=container,
            storage=MemorySagaStorage(),
            scope_strategy=ScopeStrategy.SEND,
        ):
            pass
    assert any("no DI scope is active" in record.message for record in caplog.records)

    caplog.clear()
    with caplog.at_level("WARNING", logger="cqrs.saga"):
        async with enter_scope(container):
            async with saga.transaction(
                context=ScopedSagaContext(order_id="ord-no-warn"),
                container=container,
                storage=MemorySagaStorage(),
                scope_strategy=ScopeStrategy.SEND,
            ):
                pass
    assert not any("no DI scope is active" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_recover_saga_send_does_not_warn(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """recover_saga(SEND) opens the scope before transaction, so no false warning."""
    _reset()
    container = _plain_di_container()
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()
    context = ScopedSagaContext(order_id="ord-recover-nowarn")
    await storage.create_saga(saga_id, ScopedSaga.__name__, context.to_dict())
    await storage.update_status(saga_id, SagaStatus.RUNNING)

    with caplog.at_level("WARNING", logger="cqrs.saga"):
        await recover_saga(
            ScopedSaga(),
            saga_id,
            ScopedSagaContext,
            container,
            storage,
            scope_strategy=ScopeStrategy.SEND,
        )
    assert not any("no DI scope is active" in record.message for record in caplog.records)
    assert len(UOW_INSTANCES) == 1
    assert UOW_INSTANCES[0].closed is True


@pytest.mark.asyncio
async def test_fallback_handler_scopes_are_split() -> None:
    """HANDLER closes the primary UoW before fallback runs on a different open UoW."""
    _reset()

    class PrimaryFailStep(cqrs.SagaStepHandler[ScopedSagaContext, None]):
        seen_uow: typing.ClassVar[FakeUoW | None] = None

        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return list(self._events)

        async def act(
            self,
            context: ScopedSagaContext,
        ) -> SagaStepResult[ScopedSagaContext, None]:
            PrimaryFailStep.seen_uow = self.uow
            raise RuntimeError("primary failed")

        async def compensate(self, context: ScopedSagaContext) -> None:
            pass

    class FallbackOkStep(cqrs.SagaStepHandler[ScopedSagaContext, None]):
        seen_uow: typing.ClassVar[FakeUoW | None] = None
        primary_closed: typing.ClassVar[bool | None] = None

        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return list(self._events)

        async def act(
            self,
            context: ScopedSagaContext,
        ) -> SagaStepResult[ScopedSagaContext, None]:
            FallbackOkStep.seen_uow = self.uow
            FallbackOkStep.primary_closed = (
                PrimaryFailStep.seen_uow.closed if PrimaryFailStep.seen_uow is not None else None
            )
            assert self.uow.closed is False
            return self._generate_step_result(None)

        async def compensate(self, context: ScopedSagaContext) -> None:
            pass

    class FallbackSaga(cqrs.Saga[ScopedSagaContext]):
        steps = [Fallback(step=PrimaryFailStep, fallback=FallbackOkStep)]

    PrimaryFailStep.seen_uow = None
    FallbackOkStep.seen_uow = None
    FallbackOkStep.primary_closed = None

    results = []
    async with FallbackSaga().transaction(
        context=ScopedSagaContext(order_id="ord-fb"),
        container=_tracking_di_container(),
        storage=MemorySagaStorage(),
        scope_strategy=ScopeStrategy.HANDLER,
    ) as tx:
        results = [r async for r in tx]

    assert len(results) == 1
    assert FallbackOkStep.primary_closed is True
    assert PrimaryFailStep.seen_uow is not None
    assert FallbackOkStep.seen_uow is not None
    assert PrimaryFailStep.seen_uow is not FallbackOkStep.seen_uow
    assert PrimaryFailStep.seen_uow.closed is True
    assert PrimaryFailStep.seen_uow.rolled_back is True
    assert PrimaryFailStep.seen_uow.committed is False
    assert FallbackOkStep.seen_uow.closed is True
    assert FallbackOkStep.seen_uow.committed is True
    assert FallbackOkStep.seen_uow.rolled_back is False
    assert LIFECYCLE == [
        "enter",
        "rollback",
        "exit",
        "enter",
        "commit",
        "exit",
    ]


@pytest.mark.asyncio
async def test_fallback_send_shares_uow_without_primary_rollback() -> None:
    """SEND saga fallback stays in the same UoW; primary error is not a rollback."""
    _reset()

    class PrimaryFailStep(cqrs.SagaStepHandler[ScopedSagaContext, None]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return list(self._events)

        async def act(
            self,
            context: ScopedSagaContext,
        ) -> SagaStepResult[ScopedSagaContext, None]:
            LIFECYCLE.append("primary")
            raise RuntimeError("primary failed")

        async def compensate(self, context: ScopedSagaContext) -> None:
            pass

    class FallbackOkStep(cqrs.SagaStepHandler[ScopedSagaContext, None]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return list(self._events)

        async def act(
            self,
            context: ScopedSagaContext,
        ) -> SagaStepResult[ScopedSagaContext, None]:
            LIFECYCLE.append("fallback")
            return self._generate_step_result(None)

        async def compensate(self, context: ScopedSagaContext) -> None:
            pass

    class FallbackSaga(cqrs.Saga[ScopedSagaContext]):
        steps = [Fallback(step=PrimaryFailStep, fallback=FallbackOkStep)]

    container = _tracking_di_container()
    results = []
    async with enter_scope(container):
        async with FallbackSaga().transaction(
            context=ScopedSagaContext(order_id="ord-fb-send"),
            container=container,
            storage=MemorySagaStorage(),
            scope_strategy=ScopeStrategy.SEND,
        ) as tx:
            results = [r async for r in tx]

    assert len(results) == 1
    assert len(UOW_INSTANCES) == 1
    assert UOW_INSTANCES[0].rolled_back is False
    assert UOW_INSTANCES[0].committed is True
    assert LIFECYCLE == ["enter", "primary", "fallback", "commit", "exit"]


@pytest.mark.asyncio
async def test_abandoned_send_stream_does_not_poison_next_send() -> None:
    """A SEND stream dropped without aclose must not leak its scope into the next send()."""
    _reset()
    di_c = _di_container()
    cqrs_c = DIContainer()
    cqrs_c.attach_external_container(di_c)
    stream_mediator = bootstrap.bootstrap_streaming(
        di_container=cqrs_c,
        commands_mapper=_stream_mapper,
        scope_strategy=ScopeStrategy.SEND,
        concurrent_event_handle_enable=False,
    )
    request_mediator = bootstrap.bootstrap(
        di_container=cqrs_c,
        commands_mapper=_commands_mapper,
        scope_strategy=ScopeStrategy.SEND,
        concurrent_event_handle_enable=False,
    )
    agen = stream_mediator.stream(StreamCommand())
    first = await agen.__anext__()
    assert first == StreamChunk(value="one")
    stream_uow = UOW_INSTANCES[0]
    # Drop without aclose(): CPython may finalize the async-gen later; _reset_token
    # must not leave a dead scoped container in the ContextVar either way.
    del agen
    gc.collect()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await request_mediator.send(Command())
    assert len(UOW_INSTANCES) >= 2
    assert UOW_INSTANCES[-1] is not stream_uow


class _SlottedCloneWeak:
    __slots__ = ("label", "__weakref__")

    def __init__(self, label: str) -> None:
        self.label = label

    @property
    def external_container(self) -> object:
        return self

    def attach_external_container(self, container: object) -> None:
        pass

    async def resolve(self, type_: type) -> object:
        raise NotImplementedError


class _SlottedCloneAttr:
    __slots__ = ("label", "_cqrs_scope_closed")

    def __init__(self, label: str) -> None:
        self.label = label

    @property
    def external_container(self) -> object:
        return self

    def attach_external_container(self, container: object) -> None:
        pass

    async def resolve(self, type_: type) -> object:
        raise NotImplementedError


class _SlottedCloneNeither:
    __slots__ = ("label",)

    def __init__(self, label: str) -> None:
        self.label = label

    @property
    def external_container(self) -> object:
        return self

    def attach_external_container(self, container: object) -> None:
        pass

    async def resolve(self, type_: type) -> object:
        raise NotImplementedError


class _SlottedRoot:
    def __init__(self, clone_type: type) -> None:
        self.opens = 0
        self._clone_type = clone_type

    @property
    def external_container(self) -> object:
        return self

    def attach_external_container(self, container: object) -> None:
        pass

    async def resolve(self, type_: type) -> object:
        raise NotImplementedError

    @contextlib.asynccontextmanager
    async def open_scope(
        self,
        context: typing.Mapping[type, typing.Any] | None = None,
    ) -> typing.AsyncIterator[object]:
        _ = context
        self.opens += 1
        yield self._clone_type(f"c{self.opens}")


async def _assert_closed_scope_not_reused(root: _SlottedRoot) -> None:
    from cqrs.container import scope as scope_mod

    async with enter_scope(root) as first:  # type: ignore[arg-type]
        pass
    assert root.opens == 1
    token = scope_mod._current_container.set(first)  # type: ignore[arg-type]
    try:
        async with enter_scope(root) as second:  # type: ignore[arg-type]
            assert second is not first
            assert root.opens == 2
    finally:
        try:
            scope_mod._current_container.reset(token)
        except ValueError:
            scope_mod._current_container.set(None)


@pytest.mark.asyncio
async def test_mark_scope_closed_slotted_with_weakref() -> None:
    await _assert_closed_scope_not_reused(_SlottedRoot(_SlottedCloneWeak))


@pytest.mark.asyncio
async def test_mark_scope_closed_slotted_via_setattr() -> None:
    await _assert_closed_scope_not_reused(_SlottedRoot(_SlottedCloneAttr))


@pytest.mark.asyncio
async def test_mark_scope_closed_slotted_without_weakref_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    root = _SlottedRoot(_SlottedCloneNeither)
    with caplog.at_level("WARNING", logger="cqrs"):
        async with enter_scope(root):  # type: ignore[arg-type]
            pass
    assert any("Cannot mark scoped container" in record.message for record in caplog.records)


# ---------------------------------------------------------------------------
# dishka adapter
# ---------------------------------------------------------------------------


class _DishkaUoW:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _DishkaCmd(cqrs.Request):
    pass


class _DishkaHandler(cqrs.RequestHandler[_DishkaCmd, None]):
    seen: _DishkaUoW | None = None

    def __init__(self, uow: _DishkaUoW) -> None:
        self.uow = uow
        _DishkaHandler.seen = uow

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return []

    async def handle(self, request: _DishkaCmd) -> None:
        LIFECYCLE.append("handle")


@pytest.mark.asyncio
async def test_dishka_scoped_resolve_and_finalization() -> None:
    pytest.importorskip("dishka")
    from dishka import Provider, Scope, make_async_container, provide

    from cqrs.container.dishka import DishkaCQRSContainer

    _reset()
    _DishkaHandler.seen = None

    class AppProvider(Provider):
        @provide(scope=Scope.REQUEST)
        async def uow(self) -> typing.AsyncIterator[_DishkaUoW]:
            uow = _DishkaUoW()
            LIFECYCLE.append("enter")
            try:
                yield uow
            finally:
                await uow.close()
                LIFECYCLE.append("exit")

        handler = provide(_DishkaHandler, scope=Scope.REQUEST)

    def mapper(m: cqrs.RequestMap) -> None:
        m.bind(_DishkaCmd, _DishkaHandler)

    root = make_async_container(AppProvider())
    try:
        mediator = bootstrap.bootstrap(
            di_container=DishkaCQRSContainer(root),
            commands_mapper=mapper,
            scope_strategy=ScopeStrategy.SEND,
        )
        await mediator.send(_DishkaCmd())
    finally:
        await root.close()

    assert LIFECYCLE == ["enter", "handle", "exit"]
    assert _DishkaHandler.seen is not None
    assert _DishkaHandler.seen.closed is True


@pytest.mark.asyncio
async def test_dishka_recover_saga_send_shares_uow() -> None:
    pytest.importorskip("dishka")
    from dishka import Provider, Scope, make_async_container, provide

    from cqrs.container.dishka import DishkaCQRSContainer

    _reset()

    class DishkaReserve(cqrs.SagaStepHandler[ScopedSagaContext, None]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return list(self._events)

        async def act(
            self,
            context: ScopedSagaContext,
        ) -> SagaStepResult[ScopedSagaContext, None]:
            LIFECYCLE.append("reserve")
            return self._generate_step_result(None)

        async def compensate(self, context: ScopedSagaContext) -> None:
            LIFECYCLE.append("compensate_reserve")
            COMPENSATE_LOG.append(
                CompensationRecord(step=self, uow=self.uow, uow_closed=self.uow.closed),
            )

    class DishkaBoom(cqrs.SagaStepHandler[ScopedSagaContext, None]):
        def __init__(self, uow: FakeUoW) -> None:
            self.uow = uow
            self._events: list[cqrs.Event] = []

        @property
        def events(self) -> typing.List[cqrs.Event]:
            return list(self._events)

        async def act(
            self,
            context: ScopedSagaContext,
        ) -> SagaStepResult[ScopedSagaContext, None]:
            LIFECYCLE.append("boom")
            raise RuntimeError("saga boom")

        async def compensate(self, context: ScopedSagaContext) -> None:
            pass

    class DishkaBoomSaga(cqrs.Saga[ScopedSagaContext]):
        steps = [DishkaReserve, DishkaBoom]

    class AppProvider(Provider):
        @provide(scope=Scope.REQUEST)
        async def uow(self) -> typing.AsyncIterator[FakeUoW]:
            uow = FakeUoW(name=f"uow-{len(UOW_INSTANCES)}")
            UOW_INSTANCES.append(uow)
            LIFECYCLE.append("enter")
            try:
                yield uow
            finally:
                await uow.close()
                LIFECYCLE.append("exit")

        reserve = provide(DishkaReserve, scope=Scope.REQUEST)
        boom = provide(DishkaBoom, scope=Scope.REQUEST)

    root = make_async_container(AppProvider())
    container = DishkaCQRSContainer(root)
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()
    context = ScopedSagaContext(order_id="ord-dishka-recover")
    await storage.create_saga(saga_id, DishkaBoomSaga.__name__, context.to_dict())
    await storage.log_step(
        saga_id,
        DishkaReserve.__name__,
        "act",
        SagaStepStatus.COMPLETED,
    )
    await storage.update_status(saga_id, SagaStatus.RUNNING)
    try:
        with pytest.raises(RuntimeError, match="saga boom"):
            await recover_saga(
                DishkaBoomSaga(),
                saga_id,
                ScopedSagaContext,
                container,
                storage,
                scope_strategy=ScopeStrategy.SEND,
            )
    finally:
        await root.close()

    assert "reserve" not in LIFECYCLE
    assert len(UOW_INSTANCES) == 1
    assert LIFECYCLE.count("boom") == 1
    assert LIFECYCLE.count("compensate_reserve") == 1
    assert UOW_INSTANCES[0].closed is True
