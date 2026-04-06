import typing
from typing import overload

import di

import cqrs
from cqrs import events
from cqrs.container import di as di_container_impl
from cqrs.middlewares import base as mediator_middlewares, logging as logging_middleware
from cqrs.container.protocol import Container as CQRSContainer


@overload
def setup_mediator(
    container: di_container_impl.DIContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    events_mapper: typing.Optional[typing.Callable[[events.EventMap], None]] = None,
) -> cqrs.EventMediator: ...


@overload
def setup_mediator(
    container: CQRSContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    events_mapper: typing.Optional[typing.Callable[[events.EventMap], None]] = None,
) -> cqrs.EventMediator: ...


def setup_mediator(
    container: typing.Union[di_container_impl.DIContainer, CQRSContainer],
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    events_mapper: typing.Optional[typing.Callable[[events.EventMap], None]] = None,
) -> cqrs.EventMediator:
    """
    Create an event mediator with the given container and middlewares.

    Args:
        container: DI container (e.g. :class:`cqrs.container.di.DIContainer`) or
            any implementation of :class:`cqrs.container.protocol.Container`.
        middlewares: Middleware chain for the mediator (e.g. logging).
        events_mapper: Optional callable that receives an :class:`~cqrs.events.map.EventMap`
            and binds event types to handler types via :meth:`~cqrs.events.map.EventMap.bind`.

    Returns:
        Configured :class:`cqrs.EventMediator` instance.

    Example::

        def bind_events(event_map: events.EventMap) -> None:
            event_map.bind(OrderCreatedEvent, OrderCreatedEventHandler)

        mediator = setup_mediator(
            container=di_container,
            middlewares=[logging_middleware.LoggingMiddleware()],
            events_mapper=bind_events,
        )
        await mediator.emit(OrderCreatedEvent(order_id="1"))
    """
    _events_mapper = events.EventMap()
    if events_mapper is not None:
        events_mapper(_events_mapper)

    middleware_chain = mediator_middlewares.MiddlewareChain()

    for middleware in middlewares:
        middleware_chain.add(middleware)

    return cqrs.EventMediator(
        event_map=_events_mapper,
        container=container,
        middleware_chain=middleware_chain,
    )


@overload
def bootstrap(
    di_container: di.Container,
    middlewares: typing.Optional[typing.Sequence[mediator_middlewares.Middleware]] = None,
    events_mapper: typing.Optional[typing.Callable[[events.EventMap], None]] = None,
    on_startup: typing.Optional[typing.List[typing.Callable[[], None]]] = None,
) -> cqrs.EventMediator: ...


@overload
def bootstrap(
    di_container: CQRSContainer,
    middlewares: typing.Optional[typing.Sequence[mediator_middlewares.Middleware]] = None,
    events_mapper: typing.Optional[typing.Callable[[events.EventMap], None]] = None,
    on_startup: typing.Optional[typing.List[typing.Callable[[], None]]] = None,
) -> cqrs.EventMediator: ...


def bootstrap(
    di_container: typing.Union[di.Container, CQRSContainer],
    middlewares: typing.Optional[typing.Sequence[mediator_middlewares.Middleware]] = None,
    events_mapper: typing.Optional[typing.Callable[[events.EventMap], None]] = None,
    on_startup: typing.Optional[typing.List[typing.Callable[[], None]]] = None,
) -> cqrs.EventMediator:
    """
    Bootstrap an event mediator with optional middlewares and event bindings.

    If ``di_container`` is a :class:`di.Container`, it is wrapped in
    :class:`cqrs.container.di.DIContainer`. Logging middleware is appended
    to the middleware list. Runs all ``on_startup`` callables before setup.

    Args:
        di_container: DI container from the ``di`` package or a CQRS container.
        middlewares: Optional list of middlewares (e.g. logging, metrics).
        events_mapper: Optional callable that receives an :class:`~cqrs.events.map.EventMap`
            and binds event types to handler types.
        on_startup: Optional list of callables to run before creating the mediator.

    Returns:
        Configured :class:`cqrs.EventMediator` with logging middleware enabled.

    Example::

        def bind_events(event_map: events.EventMap) -> None:
            event_map.bind(OrderCreatedEvent, OrderCreatedEventHandler)

        mediator = bootstrap(
            di_container=di.Container(),
            events_mapper=bind_events,
        )
        await mediator.emit(OrderCreatedEvent(order_id="1"))
    """
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

    middlewares_list: typing.List[mediator_middlewares.Middleware] = list(
        middlewares or [],
    )
    if not any(isinstance(m, logging_middleware.LoggingMiddleware) for m in middlewares_list):
        middlewares_list.append(logging_middleware.LoggingMiddleware())
    return setup_mediator(
        container,
        events_mapper=events_mapper,
        middlewares=middlewares_list,
    )
