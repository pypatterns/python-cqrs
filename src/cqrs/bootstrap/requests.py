from __future__ import annotations

import typing
from typing import overload

import cqrs
from cqrs import events
from cqrs.container.protocol import Container as CQRSContainer
from cqrs.container.scope import ScopeStrategy
from cqrs.container.simple import as_container
from cqrs.message_brokers import devnull, protocol
from cqrs.middlewares import base as mediator_middlewares, logging as logging_middleware
from cqrs.mapping.requests import RequestMap
from cqrs.serializers.protocol import EventSerializer

if typing.TYPE_CHECKING:
    import di
    from cqrs.container.di import DIContainer

DEFAULT_MESSAGE_BROKER = devnull.DevnullMessageBroker()


@overload
def setup_event_emitter(
    container: DIContainer,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    message_broker: protocol.MessageBroker | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    *,
    serializer: EventSerializer | None = None,
) -> events.EventEmitter: ...


@overload
def setup_event_emitter(
    container: CQRSContainer,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    message_broker: protocol.MessageBroker | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    *,
    serializer: EventSerializer | None = None,
): ...


def setup_event_emitter(
    container: DIContainer | CQRSContainer,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    message_broker: protocol.MessageBroker | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    *,
    serializer: EventSerializer | None = None,
) -> events.EventEmitter:
    """
    [AI Instruction]
    Build the emitter ``bootstrap`` uses to publish handler events. Call
    ``bootstrap`` from application startup. Use this only when the
    application already owns the container and must pass a custom emitter
    into ``setup_mediator``.
    """
    if message_broker is None:
        message_broker = DEFAULT_MESSAGE_BROKER
    event_mapper = events.EventMap()

    if domain_events_mapper:
        domain_events_mapper(event_mapper)

    return events.EventEmitter(
        event_map=event_mapper,
        container=container,
        message_broker=message_broker,
        scope_strategy=scope_strategy,
        serializer=serializer,
    )


@overload
def setup_mediator(
    event_emitter: events.EventEmitter,
    container: DIContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    event_map: events.EventMap | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.RequestMediator: ...


@overload
def setup_mediator(
    event_emitter: events.EventEmitter,
    container: CQRSContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    event_map: events.EventMap | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.RequestMediator: ...


def setup_mediator(
    event_emitter: events.EventEmitter,
    container: DIContainer | CQRSContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    event_map: events.EventMap | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.RequestMediator:
    """
    [AI Instruction]
    Wire maps and middleware into a ``RequestMediator``. Application startup
    should call ``bootstrap``, which also creates the emitter and runs
    ``on_startup``. Use this when those pieces already exist.
    """
    requests_mapper = RequestMap()
    if commands_mapper:
        commands_mapper(requests_mapper)
    if queries_mapper:
        queries_mapper(requests_mapper)

    # Use event_map from event_emitter if not provided
    if event_map is None:
        event_map = event_emitter._event_map

    middleware_chain = mediator_middlewares.MiddlewareChain()

    for middleware in middlewares:
        middleware_chain.add(middleware)

    return cqrs.RequestMediator(
        request_map=requests_mapper,
        container=container,
        event_emitter=event_emitter,
        middleware_chain=middleware_chain,
        event_map=event_map,
        max_concurrent_event_handlers=max_concurrent_event_handlers,
        concurrent_event_handle_enable=concurrent_event_handle_enable,
        scope_strategy=scope_strategy,
    )


@overload
def bootstrap(
    di_container: di.Container | None = None,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool = False,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    *,
    serializer: EventSerializer | None = None,
) -> cqrs.RequestMediator: ...


@overload
def bootstrap(
    di_container: CQRSContainer | None = None,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool = False,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    *,
    serializer: EventSerializer | None = None,
) -> cqrs.RequestMediator: ...


def bootstrap(
    di_container: di.Container | CQRSContainer | None = None,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool = False,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    *,
    serializer: EventSerializer | None = None,
) -> cqrs.RequestMediator:
    """
    Create a ``RequestMediator``.

    Omit ``di_container`` when handlers take no constructor arguments.
    That path uses :class:`cqrs.SimpleContainer`. Pass a ``di.Container``
    (extra ``di``), a dependency-injector adapter (extra
    ``dependency-injector``), or a Dishka adapter (extra ``dishka``) when
    handlers need dependencies. ``register`` on ``SimpleContainer`` pins an
    instance, and every ``resolve`` returns that same object.

    [AI Instruction]
    Build the ``RequestMediator`` an application calls for commands and
    queries. Pass ``commands_mapper`` and ``queries_mapper`` that call
    ``mapper.bind``. Both kinds of request are ``cqrs.Request``.
    ``domain_events_mapper`` registers in-process reactions to events those
    handlers produce. Logging middleware is added when the list does not
    already contain it. A missing broker becomes a broker that discards
    messages; pass a real broker when notifications must leave the process.
    Omit ``di_container`` for handlers with no constructor dependencies.
    Install optional integrations explicitly (``di``, ``dependency-injector``,
    ``dishka``, ``pydantic``, ``sqlalchemy``, ``sqlmodel``, ``kafka``, and
    the others in ``pyproject.toml``).
    """
    if message_broker is None:
        message_broker = DEFAULT_MESSAGE_BROKER
    if on_startup is None:
        on_startup = []

    for fun in on_startup:
        fun()

    container = as_container(di_container)

    event_emitter = setup_event_emitter(
        container,
        domain_events_mapper,
        message_broker,
        scope_strategy=scope_strategy,
        serializer=serializer,
    )
    middlewares_list: typing.List[mediator_middlewares.Middleware] = list(
        middlewares or [],
    )
    if not any(isinstance(m, logging_middleware.LoggingMiddleware) for m in middlewares_list):
        middlewares_list.append(logging_middleware.LoggingMiddleware())
    return setup_mediator(
        event_emitter,
        container,
        middlewares=middlewares_list,
        commands_mapper=commands_mapper,
        queries_mapper=queries_mapper,
        event_map=event_emitter._event_map,
        max_concurrent_event_handlers=max_concurrent_event_handlers,
        concurrent_event_handle_enable=concurrent_event_handle_enable,
        scope_strategy=scope_strategy,
    )


@overload
def setup_streaming_mediator(
    event_emitter: events.EventEmitter,
    container: DIContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    max_concurrent_event_handlers: int = 10,
    concurrent_event_handle_enable: bool | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.StreamingRequestMediator: ...


@overload
def setup_streaming_mediator(
    event_emitter: events.EventEmitter,
    container: CQRSContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    max_concurrent_event_handlers: int = 10,
    concurrent_event_handle_enable: bool | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.StreamingRequestMediator: ...


def setup_streaming_mediator(
    event_emitter: events.EventEmitter,
    container: DIContainer | CQRSContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    max_concurrent_event_handlers: int = 10,
    concurrent_event_handle_enable: bool | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.StreamingRequestMediator:
    """
    [AI Instruction]
    Wire maps and middleware into a ``StreamingRequestMediator``. Application
    startup should call ``bootstrap_streaming``. Use this when the emitter
    already exists.
    """
    requests_mapper = RequestMap()
    if commands_mapper:
        commands_mapper(requests_mapper)
    if queries_mapper:
        queries_mapper(requests_mapper)

    event_map = events.EventMap()
    if domain_events_mapper:
        domain_events_mapper(event_map)

    middleware_chain = mediator_middlewares.MiddlewareChain()

    for middleware in middlewares:
        middleware_chain.add(middleware)

    return cqrs.StreamingRequestMediator(
        request_map=requests_mapper,
        container=container,
        event_emitter=event_emitter,
        middleware_chain=middleware_chain,
        event_map=event_map,
        max_concurrent_event_handlers=max_concurrent_event_handlers,
        concurrent_event_handle_enable=concurrent_event_handle_enable,
        scope_strategy=scope_strategy,
    )


@overload
def bootstrap_streaming(
    di_container: di.Container | None = None,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    max_concurrent_event_handlers: int = 10,
    concurrent_event_handle_enable: bool = False,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    *,
    serializer: EventSerializer | None = None,
) -> cqrs.StreamingRequestMediator: ...


@overload
def bootstrap_streaming(
    di_container: CQRSContainer | None = None,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    max_concurrent_event_handlers: int = 10,
    concurrent_event_handle_enable: bool = False,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    *,
    serializer: EventSerializer | None = None,
) -> cqrs.StreamingRequestMediator: ...


def bootstrap_streaming(
    di_container: di.Container | CQRSContainer | None = None,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    max_concurrent_event_handlers: int = 10,
    concurrent_event_handle_enable: bool = False,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    *,
    serializer: EventSerializer | None = None,
) -> cqrs.StreamingRequestMediator:
    """
    [AI Instruction]
    Build the mediator for handlers that yield many results. Wire streaming
    handlers through ``commands_mapper`` or ``queries_mapper`` with
    ``mapper.bind``, the same as a normal request. The caller iterates
    ``mediator.stream``, it does not ``await send``. Use ``bootstrap`` when
    each request returns one value. Omit ``di_container`` for handlers with
    no constructor dependencies; that uses ``cqrs.SimpleContainer``.
    """
    if message_broker is None:
        message_broker = DEFAULT_MESSAGE_BROKER
    if on_startup is None:
        on_startup = []

    for fun in on_startup:
        fun()

    container = as_container(di_container)

    event_emitter = setup_event_emitter(
        container,
        domain_events_mapper,
        message_broker,
        scope_strategy=scope_strategy,
        serializer=serializer,
    )
    middlewares_list: typing.List[mediator_middlewares.Middleware] = list(
        middlewares or [],
    )
    if not any(isinstance(m, logging_middleware.LoggingMiddleware) for m in middlewares_list):
        middlewares_list.append(logging_middleware.LoggingMiddleware())
    return setup_streaming_mediator(
        event_emitter,
        container,
        middlewares=middlewares_list,
        commands_mapper=commands_mapper,
        queries_mapper=queries_mapper,
        domain_events_mapper=domain_events_mapper,
        max_concurrent_event_handlers=max_concurrent_event_handlers,
        concurrent_event_handle_enable=concurrent_event_handle_enable,
        scope_strategy=scope_strategy,
    )
