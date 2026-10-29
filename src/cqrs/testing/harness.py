"""Build or wrap mediators and expose journals of what they did.

[AI Instruction]
Import ``test_harness`` from ``cqrs.testing``. ``async with test_harness(...)``
builds the mediators with the existing bootstrap functions.
``async with test_harness.around(...)`` records an application mediator for
the block and restores resolve, the broker, saga storage, and the emitter
listener afterwards. A ``DevnullMessageBroker`` on the mediator is replaced
for the block by ``CapturingMessageBroker``.
"""

import typing
from types import TracebackType

from cqrs.bootstrap import events as events_bootstrap
from cqrs.bootstrap import requests as requests_bootstrap
from cqrs.bootstrap import saga as saga_bootstrap
from cqrs.container.scope import ScopeStrategy
from cqrs.inbox.map import InboxMessageMap
from cqrs.inbox.memory import MemoryInboxRepository
from cqrs.inbox.processor import InboxProcessor
from cqrs.mediators.event import EventMediator
from cqrs.mediators.request import RequestMediator
from cqrs.mediators.saga import SagaMediator
from cqrs.mediators.streaming import StreamingRequestMediator
from cqrs.message_brokers.devnull import DevnullMessageBroker
from cqrs.outbox.memory import MemoryOutboxRepository
from cqrs.saga.storage.memory import MemorySagaStorage
from cqrs.testing.broker import CapturingMessageBroker
from cqrs.testing.container import wrap_resolve
from cqrs.testing.recording import (
    EventJournal,
    HandlerErrorJournal,
    InboxJournal,
    InvocationJournal,
    MiddlewareJournal,
    OutboxJournal,
    PublishedJournal,
    RequestJournal,
    SagaJournal,
    listener_for,
    wrap_emit,
    wrap_middleware,
    wrap_send,
    wrap_stream,
)


class TestHarness:
    """Mediators and journals for one ``test_harness`` or ``around`` block."""

    def __init__(
        self,
        *,
        mediator: RequestMediator | None,
        event_mediator: EventMediator | None,
        streaming_mediator: StreamingRequestMediator | None,
        saga_mediator: SagaMediator | None,
        broker: CapturingMessageBroker,
        invocations: InvocationJournal,
        handled: EventJournal,
        published: PublishedJournal,
        outbox: OutboxJournal,
        requests: RequestJournal,
        yields: list[typing.Any],
        middlewares: MiddlewareJournal,
        handler_errors: HandlerErrorJournal,
        unhandled: EventJournal,
        sagas: SagaJournal,
        inbox: InboxJournal,
        outbox_repository: typing.Any,
        inbox_repository: typing.Any,
        saga_storage: typing.Any,
    ) -> None:
        self.mediator = mediator
        self.event_mediator = event_mediator
        self.streaming_mediator = streaming_mediator
        self.saga_mediator = saga_mediator
        self.broker = broker
        self.invocations = invocations
        self.handled = handled
        self.published = published
        self.outbox = outbox
        self.requests = requests
        self.yields = yields
        self.middlewares = middlewares
        self.handler_errors = handler_errors
        self.unhandled = unhandled
        self.sagas = sagas
        self.inbox = inbox
        self.outbox_repository = outbox_repository
        self.inbox_repository = inbox_repository
        self.saga_storage = saga_storage


class _ReadyHarness(TestHarness):
    """Result of ``test_harness``; bootstrap built every mediator."""

    mediator: RequestMediator
    event_mediator: EventMediator
    streaming_mediator: StreamingRequestMediator
    saga_mediator: SagaMediator


def _emitter(mediator: typing.Any) -> typing.Any:
    processor = getattr(mediator, "_event_processor", None)
    if processor is None:
        return None
    return getattr(processor, "_event_emitter", None)


def _mediators(*mediators: typing.Any) -> list[typing.Any]:
    return [mediator for mediator in mediators if mediator is not None]


def _unique(items: typing.Iterable[typing.Any]) -> list[typing.Any]:
    found: list[typing.Any] = []
    seen: set[int] = set()
    for item in items:
        if item is None or id(item) in seen:
            continue
        seen.add(id(item))
        found.append(item)
    return found


def _containers(mediators: list[typing.Any]) -> list[typing.Any]:
    found: list[typing.Any] = []
    for mediator in mediators:
        found.append(getattr(mediator, "_container", None))
        emitter = _emitter(mediator)
        if emitter is not None:
            found.append(getattr(emitter, "_container", None))
        dispatcher = getattr(mediator, "_dispatcher", None)
        if dispatcher is not None:
            found.append(getattr(dispatcher, "_container", None))
    return _unique(found)


def _emitters(mediators: list[typing.Any]) -> list[typing.Any]:
    return _unique(_emitter(mediator) for mediator in mediators)


def _chains(mediators: list[typing.Any]) -> list[typing.Any]:
    found: list[typing.Any] = []
    for mediator in mediators:
        dispatcher = getattr(mediator, "_dispatcher", None)
        if dispatcher is None:
            continue
        found.append(getattr(dispatcher, "_middleware_chain", None))
    return _unique(found)


def _restore_attr(owner: typing.Any, name: str, previous: typing.Any) -> typing.Callable[[], None]:
    def restore() -> None:
        setattr(owner, name, previous)

    return restore


def _bridge_broker(
    broker: typing.Any,
    chosen: CapturingMessageBroker,
) -> typing.Callable[[], None]:
    original = broker.send_message

    async def send_message(message: typing.Any) -> None:
        await chosen.send_message(message)

    broker.send_message = send_message
    return _restore_attr(broker, "send_message", original)


def _connect_brokers(
    emitters: list[typing.Any],
    explicit: typing.Any,
) -> tuple[CapturingMessageBroker, list[typing.Callable[[], None]]]:
    restorers: list[typing.Callable[[], None]] = []
    chosen: CapturingMessageBroker | None = None
    if isinstance(explicit, CapturingMessageBroker):
        chosen = explicit
    else:
        for emitter in emitters:
            current = getattr(emitter, "_message_broker", None)
            if isinstance(current, CapturingMessageBroker):
                chosen = current
                break
    if chosen is None:
        inner = None
        if explicit is not None and not isinstance(explicit, DevnullMessageBroker):
            inner = explicit
        else:
            real = [
                emitter._message_broker
                for emitter in emitters
                if getattr(emitter, "_message_broker", None) is not None
                and not isinstance(emitter._message_broker, DevnullMessageBroker)
                and not isinstance(emitter._message_broker, CapturingMessageBroker)
            ]
            if len({id(broker) for broker in real}) == 1:
                inner = real[0]
        chosen = CapturingMessageBroker(inner=inner)

    for emitter in emitters:
        current = getattr(emitter, "_message_broker", None)
        if current is chosen:
            continue
        if (
            current is None
            or isinstance(current, DevnullMessageBroker)
            or current is explicit
            or current is chosen._inner
        ):
            emitter._message_broker = chosen
            restorers.append(_restore_attr(emitter, "_message_broker", current))
            continue
        if isinstance(current, CapturingMessageBroker):
            continue
        restorers.append(_bridge_broker(current, chosen))
    return chosen, restorers


def _classify(
    mediator: typing.Any,
    event_mediator: EventMediator | None,
    streaming_mediator: StreamingRequestMediator | None,
    saga_mediator: SagaMediator | None,
) -> tuple[
    typing.Optional[RequestMediator],
    typing.Optional[EventMediator],
    typing.Optional[StreamingRequestMediator],
    typing.Optional[SagaMediator],
]:
    if isinstance(mediator, EventMediator):
        return None, event_mediator or mediator, streaming_mediator, saga_mediator
    if isinstance(mediator, StreamingRequestMediator):
        return None, event_mediator, streaming_mediator or mediator, saga_mediator
    if isinstance(mediator, SagaMediator):
        return None, event_mediator, streaming_mediator, saga_mediator or mediator
    if isinstance(mediator, RequestMediator):
        return mediator, event_mediator, streaming_mediator, saga_mediator
    return None, event_mediator, streaming_mediator, saga_mediator


def _attach(
    *,
    request_mediator: RequestMediator | None,
    event_mediator: EventMediator | None,
    streaming_mediator: StreamingRequestMediator | None,
    saga_mediator: SagaMediator | None,
    broker: typing.Any,
    outbox_repository: typing.Any,
    inbox_repository: typing.Any,
    inbox_message_map: typing.Any,
    saga_storage: typing.Any,
) -> tuple[TestHarness, list[typing.Callable[[], None]]]:
    invocations = InvocationJournal()
    handled = EventJournal()
    requests = RequestJournal()
    yields: list[typing.Any] = []
    middlewares = MiddlewareJournal()
    handler_errors = HandlerErrorJournal()
    unhandled = EventJournal()
    restorers: list[typing.Callable[[], None]] = []

    mediators = _mediators(
        request_mediator,
        event_mediator,
        streaming_mediator,
        saga_mediator,
    )
    emitters = _emitters(mediators)
    chosen, broker_restorers = _connect_brokers(emitters, broker)
    restorers.extend(broker_restorers)
    published = PublishedJournal(chosen.messages)
    published.bind(handled, invocations)

    for container in _containers(mediators):
        restorers.append(wrap_resolve(container, invocations))

    if request_mediator is not None:
        restorers.append(wrap_send(request_mediator, requests))
    if streaming_mediator is not None:
        restorers.append(wrap_stream(streaming_mediator, yields))

    for emitter in emitters:
        restorers.append(wrap_emit(emitter, handled))
        previous = getattr(emitter, "_listener", None)
        emitter._listener = listener_for(handler_errors, unhandled, previous)
        restorers.append(_restore_attr(emitter, "_listener", previous))

    seen_middleware: set[int] = set()
    for chain in _chains(mediators):
        for middleware in getattr(chain, "_chain", []):
            if id(middleware) in seen_middleware:
                continue
            seen_middleware.add(id(middleware))
            restorers.append(wrap_middleware(middleware, middlewares))

    scope_strategy = ScopeStrategy.NONE
    saga_container = None
    if saga_mediator is not None:
        scope_strategy = saga_mediator._scope_strategy
        saga_container = saga_mediator._container
        if saga_storage is None:
            saga_storage = saga_mediator._dispatcher._storage
    sagas = SagaJournal(saga_storage, saga_container, scope_strategy)
    restorers.append(sagas.restore)

    outbox = OutboxJournal(outbox_repository, chosen)
    restorers.append(outbox.restore)

    if inbox_repository is not None and inbox_message_map is None:
        inbox_message_map = getattr(inbox_repository, "_message_map", None)
    processor = None
    if inbox_repository is not None and inbox_message_map is not None:
        processor = InboxProcessor(
            inbox_repository,
            inbox_message_map,
            request_mediator=request_mediator,
            event_mediator=event_mediator,
        )
    inbox = InboxJournal(inbox_repository, inbox_message_map, processor)
    restorers.append(inbox.restore)

    harness = TestHarness(
        mediator=request_mediator,
        event_mediator=event_mediator,
        streaming_mediator=streaming_mediator,
        saga_mediator=saga_mediator,
        broker=chosen,
        invocations=invocations,
        handled=handled,
        published=published,
        outbox=outbox,
        requests=requests,
        yields=yields,
        middlewares=middlewares,
        handler_errors=handler_errors,
        unhandled=unhandled,
        sagas=sagas,
        inbox=inbox,
        outbox_repository=outbox_repository,
        inbox_repository=inbox_repository,
        saga_storage=saga_storage,
    )
    return harness, restorers


def _restore_all(restorers: list[typing.Callable[[], None]]) -> None:
    for restore in reversed(restorers):
        restore()


class _Around:
    def __init__(
        self,
        mediator: typing.Any,
        *,
        event_mediator: EventMediator | None = None,
        streaming_mediator: StreamingRequestMediator | None = None,
        saga_mediator: SagaMediator | None = None,
        outbox: typing.Any = None,
        inbox: typing.Any = None,
        broker: typing.Any = None,
    ) -> None:
        request_mediator, event_mediator, streaming_mediator, saga_mediator = _classify(
            mediator,
            event_mediator,
            streaming_mediator,
            saga_mediator,
        )
        self._request_mediator = request_mediator
        self._event_mediator = event_mediator
        self._streaming_mediator = streaming_mediator
        self._saga_mediator = saga_mediator
        self._outbox = outbox
        self._inbox = inbox
        self._broker = broker
        self._restorers: list[typing.Callable[[], None]] = []

    async def __aenter__(self) -> TestHarness:
        inbox_map = getattr(self._inbox, "_message_map", None) if self._inbox is not None else None
        harness, restorers = _attach(
            request_mediator=self._request_mediator,
            event_mediator=self._event_mediator,
            streaming_mediator=self._streaming_mediator,
            saga_mediator=self._saga_mediator,
            broker=self._broker,
            outbox_repository=self._outbox,
            inbox_repository=self._inbox,
            inbox_message_map=inbox_map,
            saga_storage=None,
        )
        self._restorers = restorers
        return harness

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        _restore_all(self._restorers)
        self._restorers = []


class test_harness:
    """
    Async context manager that builds mediators and records what they do.

    ``test_harness.around`` wraps mediators the application already built.
    """

    def __init__(
        self,
        container: typing.Any,
        *,
        commands_mapper: typing.Callable[..., None] | None = None,
        queries_mapper: typing.Callable[..., None] | None = None,
        domain_events_mapper: typing.Callable[..., None] | None = None,
        events_mapper: typing.Callable[..., None] | None = None,
        streaming_mapper: typing.Callable[..., None] | None = None,
        sagas_mapper: typing.Callable[..., None] | None = None,
        middlewares: typing.Sequence[typing.Any] | None = None,
        scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
        concurrent_event_handle_enable: bool | None = None,
        max_concurrent_event_handlers: int | None = None,
        message_broker: typing.Any = None,
        serializer: typing.Any = None,
        saga_storage: typing.Any = None,
        compensation_retry_delay: float = 0,
        on_startup: list[typing.Callable[[], None]] | None = None,
    ) -> None:
        self._container = container
        self._commands_mapper = commands_mapper
        self._queries_mapper = queries_mapper
        self._domain_events_mapper = domain_events_mapper
        self._events_mapper = events_mapper
        self._streaming_mapper = streaming_mapper
        self._sagas_mapper = sagas_mapper
        self._middlewares = list(middlewares or [])
        self._scope_strategy = scope_strategy
        self._concurrent_event_handle_enable = concurrent_event_handle_enable
        self._max_concurrent_event_handlers = max_concurrent_event_handlers
        self._message_broker = message_broker
        self._serializer = serializer
        self._saga_storage = saga_storage
        self._compensation_retry_delay = compensation_retry_delay
        self._on_startup = on_startup
        self._restorers: list[typing.Callable[[], None]] = []

    async def __aenter__(self) -> _ReadyHarness:
        broker = self._message_broker or CapturingMessageBroker()
        outbox_repository = MemoryOutboxRepository(serializer=self._serializer)
        inbox_map = InboxMessageMap()
        inbox_repository = MemoryInboxRepository(inbox_map)
        saga_storage = self._saga_storage or MemorySagaStorage()

        request_kwargs = self._request_kwargs(broker)
        mediator = requests_bootstrap.bootstrap(self._container, **request_kwargs)
        streaming = requests_bootstrap.bootstrap_streaming(
            self._container,
            **self._streaming_kwargs(broker),
        )
        event_mediator = events_bootstrap.bootstrap(
            self._container,
            middlewares=self._middlewares,
            events_mapper=self._events_mapper,
            on_startup=self._on_startup,
            scope_strategy=self._scope_strategy,
        )
        saga_kwargs = self._saga_kwargs(broker, saga_storage)
        saga_mediator = saga_bootstrap.bootstrap(self._container, **saga_kwargs)
        saga_mediator._dispatcher._compensation_retry_delay = self._compensation_retry_delay

        harness, restorers = _attach(
            request_mediator=mediator,
            event_mediator=event_mediator,
            streaming_mediator=streaming,
            saga_mediator=saga_mediator,
            broker=broker,
            outbox_repository=outbox_repository,
            inbox_repository=inbox_repository,
            inbox_message_map=inbox_map,
            saga_storage=saga_storage,
        )
        self._restorers = restorers
        return typing.cast(_ReadyHarness, harness)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        _restore_all(self._restorers)
        self._restorers = []

    def _common_kwargs(self, broker: typing.Any) -> dict[str, typing.Any]:
        kwargs: dict[str, typing.Any] = {
            "message_broker": broker,
            "middlewares": self._middlewares,
            "domain_events_mapper": self._domain_events_mapper,
            "on_startup": self._on_startup,
            "scope_strategy": self._scope_strategy,
            "serializer": self._serializer,
        }
        if self._concurrent_event_handle_enable is not None:
            kwargs["concurrent_event_handle_enable"] = self._concurrent_event_handle_enable
        if self._max_concurrent_event_handlers is not None:
            kwargs["max_concurrent_event_handlers"] = self._max_concurrent_event_handlers
        return kwargs

    def _request_kwargs(self, broker: typing.Any) -> dict[str, typing.Any]:
        kwargs = self._common_kwargs(broker)
        kwargs["commands_mapper"] = self._commands_mapper
        kwargs["queries_mapper"] = self._queries_mapper
        return kwargs

    def _streaming_kwargs(self, broker: typing.Any) -> dict[str, typing.Any]:
        kwargs = self._common_kwargs(broker)
        kwargs["commands_mapper"] = self._streaming_mapper
        kwargs["queries_mapper"] = self._queries_mapper
        return kwargs

    def _saga_kwargs(self, broker: typing.Any, saga_storage: typing.Any) -> dict[str, typing.Any]:
        kwargs = self._common_kwargs(broker)
        kwargs["sagas_mapper"] = self._sagas_mapper
        kwargs["saga_storage"] = saga_storage
        return kwargs

    @staticmethod
    def around(
        mediator: typing.Any,
        event_mediator: EventMediator | None = None,
        streaming_mediator: StreamingRequestMediator | None = None,
        saga_mediator: SagaMediator | None = None,
        *,
        outbox: typing.Any = None,
        inbox: typing.Any = None,
        broker: typing.Any = None,
    ) -> _Around:
        """Record ``mediator`` for the block, then restore what the block wrapped."""

        return _Around(
            mediator,
            event_mediator=event_mediator,
            streaming_mediator=streaming_mediator,
            saga_mediator=saga_mediator,
            outbox=outbox,
            inbox=inbox,
            broker=broker,
        )


__all__ = (
    "TestHarness",
    "test_harness",
)
