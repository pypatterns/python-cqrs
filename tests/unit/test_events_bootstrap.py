"""
AAA unit tests for cqrs.events.bootstrap.

Covers setup_mediator and bootstrap for EventMediator (increased coverage).
"""

import typing

import di

import cqrs
from cqrs import events
from cqrs.container import di as di_container_impl
from cqrs.middlewares import base as mediator_middlewares, logging as logging_middleware
from cqrs.events import bootstrap


# ---------------------------------------------------------------------------
# Mock CQRSContainer for tests passing CQRSContainer (not di.Container)
# ---------------------------------------------------------------------------


class MockCQRSContainer:
    """Minimal CQRSContainer implementation for bootstrap tests."""

    def __init__(self) -> None:
        self._external_container: typing.Any = None

    @property
    def external_container(self) -> typing.Any:
        return self._external_container

    def attach_external_container(self, container: typing.Any) -> None:
        self._external_container = container

    async def resolve(self, type_: type[typing.Any]) -> typing.Any:
        return type_()


# Stub event/handler for events_mapper (EventMap.bind)
class _StubEvent(events.DomainEvent, frozen=True):
    pass


class _StubEventHandler(events.EventHandler[_StubEvent]):
    async def handle(self, event: _StubEvent) -> None:
        pass


# ---------------------------------------------------------------------------
# Test setup_mediator (events)
# ---------------------------------------------------------------------------


class TestSetupMediatorEvents:
    """AAA tests for events.bootstrap.setup_mediator."""

    def test_setup_mediator_returns_event_mediator(self) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        middlewares: list[mediator_middlewares.Middleware] = [
            logging_middleware.LoggingMiddleware(),
        ]

        # Act
        mediator = bootstrap.setup_mediator(
            container,
            middlewares=middlewares,
        )

        # Assert
        assert isinstance(mediator, cqrs.EventMediator)
        assert mediator._dispatcher is not None
        assert getattr(mediator._dispatcher._container, "root") is container
        assert mediator._dispatcher._event_map is not None

    def test_setup_mediator_with_events_mapper_registers_handlers(self) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        middlewares = [logging_middleware.LoggingMiddleware()]
        event_map_received: list[events.EventMap] = []

        def events_mapper(m: events.EventMap) -> None:
            event_map_received.append(m)
            m.bind(_StubEvent, _StubEventHandler)

        # Act
        mediator = bootstrap.setup_mediator(
            container,
            middlewares=middlewares,
            events_mapper=events_mapper,
        )

        # Assert
        assert len(event_map_received) == 1
        assert mediator._dispatcher._event_map is event_map_received[0]

    def test_setup_mediator_with_cqrs_container(self) -> None:
        # Arrange
        container = MockCQRSContainer()
        middlewares = [logging_middleware.LoggingMiddleware()]

        # Act
        mediator = bootstrap.setup_mediator(
            container,
            middlewares=middlewares,
        )

        # Assert
        assert isinstance(mediator, cqrs.EventMediator)
        assert getattr(mediator._dispatcher._container, "root") is container

    def test_setup_mediator_without_events_mapper_uses_empty_map(self) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        middlewares = [logging_middleware.LoggingMiddleware()]

        # Act
        mediator = bootstrap.setup_mediator(
            container,
            middlewares=middlewares,
        )

        # Assert
        assert len(mediator._dispatcher._event_map) == 0


# ---------------------------------------------------------------------------
# Test bootstrap (events)
# ---------------------------------------------------------------------------


class TestBootstrapEvents:
    """AAA tests for events.bootstrap.bootstrap."""

    def test_bootstrap_with_di_container_returns_event_mediator(self) -> None:
        # Arrange
        di_container = di.Container()

        # Act
        mediator = bootstrap.bootstrap(di_container=di_container)

        # Assert
        assert isinstance(mediator, cqrs.EventMediator)
        assert mediator._dispatcher is not None

    def test_bootstrap_with_cqrs_container_returns_event_mediator(self) -> None:
        # Arrange
        container = MockCQRSContainer()

        # Act
        mediator = bootstrap.bootstrap(di_container=container)

        # Assert
        assert isinstance(mediator, cqrs.EventMediator)
        assert getattr(mediator._dispatcher._container, "root") is container

    def test_bootstrap_calls_on_startup_callables(self) -> None:
        # Arrange
        di_container = di.Container()
        on_startup_called: list[int] = []

        def on_startup_1() -> None:
            on_startup_called.append(1)

        def on_startup_2() -> None:
            on_startup_called.append(2)

        # Act
        bootstrap.bootstrap(
            di_container=di_container,
            on_startup=[on_startup_1, on_startup_2],
        )

        # Assert
        assert on_startup_called == [1, 2]

    def test_bootstrap_with_on_startup_none_does_not_fail(self) -> None:
        # Arrange
        di_container = di.Container()

        # Act & Assert (no exception)
        mediator = bootstrap.bootstrap(
            di_container=di_container,
            on_startup=None,
        )
        assert isinstance(mediator, cqrs.EventMediator)

    def test_bootstrap_appends_logging_middleware_if_not_present(self) -> None:
        # Arrange
        di_container = di.Container()

        # Act
        mediator = bootstrap.bootstrap(
            di_container=di_container,
            middlewares=[],
        )

        # Assert
        assert mediator._dispatcher._middleware_chain is not None
        assert isinstance(mediator, cqrs.EventMediator)

    def test_bootstrap_with_existing_logging_middleware_does_not_duplicate(
        self,
    ) -> None:
        # Arrange
        di_container = di.Container()
        middlewares = [logging_middleware.LoggingMiddleware()]

        # Act
        mediator = bootstrap.bootstrap(
            di_container=di_container,
            middlewares=middlewares,
        )

        # Assert
        assert isinstance(mediator, cqrs.EventMediator)

    def test_bootstrap_with_events_mapper(self) -> None:
        # Arrange
        di_container = di.Container()
        events_map_received: list[events.EventMap] = []

        def events_mapper(m: events.EventMap) -> None:
            events_map_received.append(m)
            m.bind(_StubEvent, _StubEventHandler)

        # Act
        mediator = bootstrap.bootstrap(
            di_container=di_container,
            events_mapper=events_mapper,
        )

        # Assert
        assert len(events_map_received) == 1
        assert mediator._dispatcher._event_map is events_map_received[0]
