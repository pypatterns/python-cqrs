import typing
from typing import overload

import di

import cqrs
from cqrs import events
from cqrs.container import di as di_container_impl
from cqrs.container.protocol import Container as CQRSContainer
from cqrs.container.scope import ScopeStrategy
from cqrs.message_brokers import devnull, protocol
from cqrs.middlewares import base as mediator_middlewares, logging as logging_middleware
from cqrs.requests.map import RequestMap

DEFAULT_MESSAGE_BROKER = devnull.DevnullMessageBroker()


@overload
def setup_event_emitter(
    container: di_container_impl.DIContainer,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    message_broker: protocol.MessageBroker | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> events.EventEmitter: ...


@overload
def setup_event_emitter(
    container: CQRSContainer,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    message_broker: protocol.MessageBroker | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
): ...


def setup_event_emitter(
    container: di_container_impl.DIContainer | CQRSContainer,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    message_broker: protocol.MessageBroker | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> events.EventEmitter:
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
    )


@overload
def setup_mediator(
    event_emitter: events.EventEmitter,
    container: di_container_impl.DIContainer,
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
    container: di_container_impl.DIContainer | CQRSContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    event_map: events.EventMap | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.RequestMediator:
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
    di_container: di.Container,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool = False,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.RequestMediator: ...


@overload
def bootstrap(
    di_container: CQRSContainer,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool = False,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.RequestMediator: ...


def bootstrap(
    di_container: di.Container | CQRSContainer,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool = False,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.RequestMediator:
    if message_broker is None:
        message_broker = DEFAULT_MESSAGE_BROKER
    if on_startup is None:
        on_startup = []

    for fun in on_startup:
        fun()

    # If the provided container is a container implemented using di package,
    # we need to wrap it into our own container
    if isinstance(di_container, di.Container):
        container = di_container_impl.DIContainer()
        container.attach_external_container(di_container)

    # Otherwise, we can use the provided container directly
    else:
        container = di_container

    event_emitter = setup_event_emitter(
        container,
        domain_events_mapper,
        message_broker,
        scope_strategy=scope_strategy,
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
    container: di_container_impl.DIContainer,
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
    container: di_container_impl.DIContainer | CQRSContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    max_concurrent_event_handlers: int = 10,
    concurrent_event_handle_enable: bool | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.StreamingRequestMediator:
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
    di_container: di.Container,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    max_concurrent_event_handlers: int = 10,
    concurrent_event_handle_enable: bool = False,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.StreamingRequestMediator: ...


@overload
def bootstrap_streaming(
    di_container: CQRSContainer,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    max_concurrent_event_handlers: int = 10,
    concurrent_event_handle_enable: bool = False,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.StreamingRequestMediator: ...


def bootstrap_streaming(
    di_container: di.Container | CQRSContainer,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    commands_mapper: typing.Callable[[RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    queries_mapper: typing.Callable[[RequestMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    max_concurrent_event_handlers: int = 10,
    concurrent_event_handle_enable: bool = False,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.StreamingRequestMediator:
    if message_broker is None:
        message_broker = DEFAULT_MESSAGE_BROKER
    if on_startup is None:
        on_startup = []

    for fun in on_startup:
        fun()

    # If the provided container is a container implemented using di package,
    # we need to wrap it into our own container
    if isinstance(di_container, di.Container):
        container = di_container_impl.DIContainer()
        container.attach_external_container(di_container)

    # Otherwise, we can use the provided container directly
    else:
        container = di_container

    event_emitter = setup_event_emitter(
        container,
        domain_events_mapper,
        message_broker,
        scope_strategy=scope_strategy,
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
