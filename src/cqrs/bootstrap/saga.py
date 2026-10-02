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
from cqrs.bootstrap.requests import setup_event_emitter
from cqrs.mapping.requests import SagaMap
from cqrs.saga.storage.protocol import ISagaStorage
from cqrs.serializers.protocol import EventSerializer

DEFAULT_MESSAGE_BROKER = devnull.DevnullMessageBroker()


@overload
def setup_saga_mediator(
    event_emitter: events.EventEmitter,
    container: di_container_impl.DIContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    sagas_mapper: typing.Callable[[SagaMap], None] | None = None,
    event_map: events.EventMap | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool | None = None,
    saga_storage: ISagaStorage | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.SagaMediator: ...


@overload
def setup_saga_mediator(
    event_emitter: events.EventEmitter,
    container: CQRSContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    sagas_mapper: typing.Callable[[SagaMap], None] | None = None,
    event_map: events.EventMap | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool | None = None,
    saga_storage: ISagaStorage | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.SagaMediator: ...


def setup_saga_mediator(
    event_emitter: events.EventEmitter,
    container: di_container_impl.DIContainer | CQRSContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    sagas_mapper: typing.Callable[[SagaMap], None] | None = None,
    event_map: events.EventMap | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool | None = None,
    saga_storage: ISagaStorage | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
) -> cqrs.SagaMediator:
    """
    Setup SagaMediator with configured saga map and dependencies.

    Args:
        event_emitter: Event emitter for publishing events
        container: DI container for resolving saga instances
        middlewares: List of middlewares to apply
        sagas_mapper: Function to register sagas in SagaMap
        event_map: Event map for processing events. If None, uses event_emitter's map
        max_concurrent_event_handlers: Maximum concurrent event handlers
        concurrent_event_handle_enable: Whether to process events in parallel.
            ``None`` becomes ``False`` under ``ScopeStrategy.SEND`` and ``True`` otherwise.
        saga_storage: Saga storage implementation. If None, uses MemorySagaStorage

    Returns:
        Configured SagaMediator instance
    """
    saga_map = SagaMap()
    if sagas_mapper:
        sagas_mapper(saga_map)

    # Use event_map from event_emitter if not provided
    if event_map is None:
        event_map = event_emitter._event_map

    middleware_chain = mediator_middlewares.MiddlewareChain()

    for middleware in middlewares:
        middleware_chain.add(middleware)

    # Note: saga_storage parameter is for documentation purposes.
    # Storage should be registered in the DI container if sagas require it.
    # Sagas will resolve storage from container when creating instances,
    # or use MemorySagaStorage as default if storage is not provided.

    return cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,
        event_emitter=event_emitter,
        middleware_chain=middleware_chain,
        event_map=event_map,
        max_concurrent_event_handlers=max_concurrent_event_handlers,
        concurrent_event_handle_enable=concurrent_event_handle_enable,
        storage=saga_storage,
        scope_strategy=scope_strategy,
    )


@overload
def bootstrap(
    di_container: di.Container,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    sagas_mapper: typing.Callable[[SagaMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    saga_storage: ISagaStorage | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    *,
    serializer: EventSerializer | None = None,
) -> cqrs.SagaMediator: ...


@overload
def bootstrap(
    di_container: CQRSContainer,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    sagas_mapper: typing.Callable[[SagaMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    saga_storage: ISagaStorage | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    *,
    serializer: EventSerializer | None = None,
) -> cqrs.SagaMediator: ...


def bootstrap(
    di_container: di.Container | CQRSContainer,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    sagas_mapper: typing.Callable[[SagaMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
    saga_storage: ISagaStorage | None = None,
    max_concurrent_event_handlers: int = 1,
    concurrent_event_handle_enable: bool | None = None,
    scope_strategy: ScopeStrategy = ScopeStrategy.NONE,
    *,
    serializer: EventSerializer | None = None,
) -> cqrs.SagaMediator:
    """
    Bootstrap SagaMediator with all necessary dependencies.

    This function sets up a complete SagaMediator instance with:
    - Event emitter for publishing domain events
    - Saga map for registering sagas
    - Middleware chain for request processing
    - Optional saga storage (defaults to MemorySagaStorage)

    Usage::

        def saga_mapper(mapper: cqrs.SagaMap) -> None:
            mapper.bind(OrderContext, OrderSaga)

        def events_mapper(mapper: cqrs.EventMap) -> None:
            mapper.bind(InventoryReservedEvent, InventoryReservedEventHandler)

        mediator = cqrs.bootstrap.saga.bootstrap(
            di_container=di.Container(),
            sagas_mapper=saga_mapper,
            domain_events_mapper=events_mapper,
            saga_storage=MemorySagaStorage(),
        )

        # Execute saga (stream() returns AsyncIterator, consumed with async for)
        async for result in mediator.execute(order_context):
            print(f"Step: {result.step_result.step_type.__name__}")

    Args:
        di_container: DI container (di.Container or CQRSContainer)
        message_broker: Message broker for event publishing. Defaults to DevnullMessageBroker
        middlewares: List of middlewares to apply
        sagas_mapper: Function to register sagas in SagaMap
        domain_events_mapper: Function to register event handlers in EventMap
        on_startup: List of functions to call on startup
        saga_storage: Saga storage implementation. If provided, should be
                     registered in di_container before calling bootstrap.
                     If None and sagas require storage, they will use
                     MemorySagaStorage as default.
        max_concurrent_event_handlers: Maximum concurrent event handlers
        concurrent_event_handle_enable: Whether to process events in parallel.
            ``None`` becomes ``False`` under ``ScopeStrategy.SEND`` and ``True`` otherwise.

    Returns:
        Configured SagaMediator instance

    Note:
        If saga_storage is provided, make sure to register it in your
        di_container before calling bootstrap, so that sagas can resolve
        it when creating instances. For example::

            storage = MemorySagaStorage()
            container = di.Container()
            container.bind(ISagaStorage, instance=storage)  # Register storage
            mediator = bootstrap(container, saga_storage=storage, ...)
    """
    if message_broker is None:
        message_broker = DEFAULT_MESSAGE_BROKER
    if on_startup is None:
        on_startup = []
    # Note: saga_storage parameter is kept for API consistency but storage
    # should be registered in di_container manually if sagas require it

    for fun in on_startup:
        fun()

    # If the provided container is a container implemented using di package,
    # we need to wrap it into our own container
    if isinstance(di_container, di.Container):
        container = di_container_impl.DIContainer()
        container.attach_external_container(di_container)
        # Note: If saga_storage is provided, it should be registered in di_container
        # manually before calling bootstrap, or sagas should handle storage
        # registration themselves when creating instances.

    # Otherwise, we can use the provided container directly
    else:
        container = di_container

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

    return setup_saga_mediator(
        event_emitter,
        container,
        middlewares=middlewares_list,
        sagas_mapper=sagas_mapper,
        event_map=event_emitter._event_map,
        max_concurrent_event_handlers=max_concurrent_event_handlers,
        concurrent_event_handle_enable=concurrent_event_handle_enable,
        saga_storage=saga_storage,
        scope_strategy=scope_strategy,
    )
