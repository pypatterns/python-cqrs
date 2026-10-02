"""
AAA unit tests for cqrs.requests.bootstrap.

Covers setup_event_emitter, setup_mediator, bootstrap, setup_streaming_mediator,
and bootstrap_streaming for increased coverage.
"""

import typing
import di
import pytest

import cqrs
from cqrs import events
from cqrs.container import di as di_container_impl
from cqrs.message_brokers import devnull
from cqrs.middlewares import base as mediator_middlewares, logging as logging_middleware
from cqrs.bootstrap import requests as bootstrap
from cqrs.mapping.requests import RequestMap


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


# Stub event/handler used to make EventMap non-empty (empty EventMap is falsy)
class _StubEvent(events.DomainEvent, frozen=True):
    pass


class _StubEventHandler(events.EventHandler[_StubEvent]):
    async def handle(self, event: _StubEvent) -> None:
        pass


# ---------------------------------------------------------------------------
# Test setup_event_emitter
# ---------------------------------------------------------------------------


class TestSetupEventEmitter:
    """AAA tests for bootstrap.setup_event_emitter."""

    def test_setup_event_emitter_with_di_container_returns_event_emitter(self) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())

        # Act
        emitter = bootstrap.setup_event_emitter(container)

        # Assert
        assert isinstance(emitter, events.EventEmitter)
        assert emitter._event_map is not None
        assert emitter._container.root is container

    def test_setup_event_emitter_with_custom_message_broker(self) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        broker = devnull.DevnullMessageBroker()

        # Act
        emitter = bootstrap.setup_event_emitter(
            container,
            message_broker=broker,
        )

        # Assert
        assert emitter._message_broker is broker

    def test_setup_event_emitter_with_domain_events_mapper_registers_handlers(
        self,
    ) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        event_map_received: list[events.EventMap] = []

        def domain_events_mapper(m: events.EventMap) -> None:
            event_map_received.append(m)

        # Act
        emitter = bootstrap.setup_event_emitter(
            container,
            domain_events_mapper=domain_events_mapper,
        )

        # Assert
        assert len(event_map_received) == 1
        assert event_map_received[0] is emitter._event_map

    def test_setup_event_emitter_with_cqrs_container_returns_event_emitter(
        self,
    ) -> None:
        # Arrange
        container = MockCQRSContainer()

        # Act
        emitter = bootstrap.setup_event_emitter(container)

        # Assert
        assert isinstance(emitter, events.EventEmitter)
        assert emitter._container.root is container


# ---------------------------------------------------------------------------
# Test setup_mediator
# ---------------------------------------------------------------------------


class TestSetupMediator:
    """AAA tests for bootstrap.setup_mediator."""

    def test_setup_mediator_returns_request_mediator(self) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = bootstrap.setup_event_emitter(container)
        middlewares: list[mediator_middlewares.Middleware] = [
            logging_middleware.LoggingMiddleware(),
        ]

        # Act
        mediator = bootstrap.setup_mediator(
            emitter,
            container,
            middlewares=middlewares,
        )

        # Assert
        assert isinstance(mediator, cqrs.RequestMediator)
        assert mediator._event_processor._event_emitter is emitter
        assert getattr(mediator._dispatcher._container, "root") is container

    def test_setup_mediator_with_commands_mapper_registers_commands(self) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]
        request_map_received: list[RequestMap] = []

        def commands_mapper(m: RequestMap) -> None:
            request_map_received.append(m)

        # Act
        mediator = bootstrap.setup_mediator(
            emitter,
            container,
            middlewares=middlewares,
            commands_mapper=commands_mapper,
        )

        # Assert
        assert len(request_map_received) == 1
        assert request_map_received[0] is mediator._dispatcher._request_map

    def test_setup_mediator_with_queries_mapper_registers_queries(self) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]
        request_map_received: list[RequestMap] = []

        def queries_mapper(m: RequestMap) -> None:
            request_map_received.append(m)

        # Act
        bootstrap.setup_mediator(
            emitter,
            container,
            middlewares=middlewares,
            queries_mapper=queries_mapper,
        )

        # Assert
        assert len(request_map_received) == 1

    def test_setup_mediator_with_custom_event_map_uses_provided_map(self) -> None:
        # Arrange: non-empty EventMap (empty dict is falsy, so mediator would use new map)
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]
        custom_event_map = events.EventMap()
        custom_event_map.bind(_StubEvent, _StubEventHandler)

        # Act
        mediator = bootstrap.setup_mediator(
            emitter,
            container,
            middlewares=middlewares,
            event_map=custom_event_map,
        )

        # Assert
        assert mediator._event_processor._event_map is custom_event_map

    def test_setup_mediator_with_max_concurrent_and_concurrent_enable(self) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]

        # Act
        mediator = bootstrap.setup_mediator(
            emitter,
            container,
            middlewares=middlewares,
            max_concurrent_event_handlers=5,
            concurrent_event_handle_enable=True,
        )

        # Assert
        assert mediator._event_processor._max_concurrent_event_handlers == 5
        assert mediator._event_processor._concurrent_event_handle_enable is True


# ---------------------------------------------------------------------------
# Test bootstrap (requests)
# ---------------------------------------------------------------------------


class TestBootstrapRequests:
    """AAA tests for bootstrap.bootstrap (request mediator)."""

    @pytest.mark.asyncio
    async def test_bootstrap_with_di_container_returns_request_mediator(
        self,
    ) -> None:
        # Arrange
        di_container = di.Container()

        # Act
        mediator = bootstrap.bootstrap(di_container=di_container)

        # Assert
        assert isinstance(mediator, cqrs.RequestMediator)
        assert mediator._event_processor is not None

    @pytest.mark.asyncio
    async def test_bootstrap_with_cqrs_container_returns_request_mediator(
        self,
    ) -> None:
        # Arrange
        container = MockCQRSContainer()

        # Act
        mediator = bootstrap.bootstrap(di_container=container)

        # Assert
        assert isinstance(mediator, cqrs.RequestMediator)
        assert getattr(mediator._dispatcher._container, "root") is container

    @pytest.mark.asyncio
    async def test_bootstrap_calls_on_startup_callables(self) -> None:
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

    @pytest.mark.asyncio
    async def test_bootstrap_with_on_startup_none_does_not_fail(self) -> None:
        # Arrange
        di_container = di.Container()

        # Act & Assert (no exception)
        mediator = bootstrap.bootstrap(
            di_container=di_container,
            on_startup=None,
        )
        assert isinstance(mediator, cqrs.RequestMediator)

    @pytest.mark.asyncio
    async def test_bootstrap_appends_logging_middleware_if_not_present(
        self,
    ) -> None:
        # Arrange
        di_container = di.Container()

        # Act
        mediator = bootstrap.bootstrap(
            di_container=di_container,
            middlewares=[],
        )

        # Assert: mediator has dispatcher with middleware chain
        assert mediator._dispatcher._middleware_chain is not None
        assert isinstance(mediator, cqrs.RequestMediator)

    @pytest.mark.asyncio
    async def test_bootstrap_with_existing_logging_middleware_does_not_duplicate(
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
        assert isinstance(mediator, cqrs.RequestMediator)

    @pytest.mark.asyncio
    async def test_bootstrap_with_custom_message_broker(self) -> None:
        # Arrange
        di_container = di.Container()
        broker = devnull.DevnullMessageBroker()

        # Act
        mediator = bootstrap.bootstrap(
            di_container=di_container,
            message_broker=broker,
        )

        # Assert
        assert mediator._event_processor._event_emitter is not None
        assert mediator._event_processor._event_emitter._message_broker is broker

    @pytest.mark.asyncio
    async def test_bootstrap_with_commands_and_queries_mapper(self) -> None:
        # Arrange
        di_container = di.Container()
        commands_called = False
        queries_called = False

        def commands_mapper(m: RequestMap) -> None:
            nonlocal commands_called
            commands_called = True

        def queries_mapper(m: RequestMap) -> None:
            nonlocal queries_called
            queries_called = True

        # Act
        mediator = bootstrap.bootstrap(
            di_container=di_container,
            commands_mapper=commands_mapper,
            queries_mapper=queries_mapper,
        )

        # Assert
        assert commands_called
        assert queries_called
        assert isinstance(mediator, cqrs.RequestMediator)


# ---------------------------------------------------------------------------
# Test setup_streaming_mediator
# ---------------------------------------------------------------------------


class TestSetupStreamingMediator:
    """AAA tests for bootstrap.setup_streaming_mediator."""

    def test_setup_streaming_mediator_returns_streaming_request_mediator(
        self,
    ) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]

        # Act
        mediator = bootstrap.setup_streaming_mediator(
            emitter,
            container,
            middlewares=middlewares,
        )

        # Assert
        assert isinstance(mediator, cqrs.StreamingRequestMediator)
        assert mediator._event_processor._event_emitter is emitter

    def test_setup_streaming_mediator_with_commands_and_queries_mapper(
        self,
    ) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]
        requests_map_received: list[RequestMap] = []

        def commands_mapper(m: RequestMap) -> None:
            requests_map_received.append(m)

        def queries_mapper(m: RequestMap) -> None:
            requests_map_received.append(m)

        # Act
        mediator = bootstrap.setup_streaming_mediator(
            emitter,
            container,
            middlewares=middlewares,
            commands_mapper=commands_mapper,
            queries_mapper=queries_mapper,
        )

        # Assert
        assert len(requests_map_received) == 2
        assert mediator._dispatcher._request_map is requests_map_received[0]

    def test_setup_streaming_mediator_with_domain_events_mapper(self) -> None:
        # Arrange: bind in mapper so EventMap is non-empty (empty dict is falsy in mediator)
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]
        event_map_received: list[events.EventMap] = []

        def domain_events_mapper(m: events.EventMap) -> None:
            event_map_received.append(m)
            m.bind(_StubEvent, _StubEventHandler)

        # Act
        mediator = bootstrap.setup_streaming_mediator(
            emitter,
            container,
            middlewares=middlewares,
            domain_events_mapper=domain_events_mapper,
        )

        # Assert
        assert len(event_map_received) == 1
        assert mediator._event_processor._event_map is event_map_received[0]

    def test_setup_streaming_mediator_with_max_concurrent_params(self) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]

        # Act
        mediator = bootstrap.setup_streaming_mediator(
            emitter,
            container,
            middlewares=middlewares,
            max_concurrent_event_handlers=20,
            concurrent_event_handle_enable=False,
        )

        # Assert
        assert mediator._event_processor._max_concurrent_event_handlers == 20
        assert mediator._event_processor._concurrent_event_handle_enable is False


# ---------------------------------------------------------------------------
# Test bootstrap_streaming
# ---------------------------------------------------------------------------


class TestBootstrapStreaming:
    """AAA tests for bootstrap.bootstrap_streaming."""

    @pytest.mark.asyncio
    async def test_bootstrap_streaming_with_di_container_returns_streaming_mediator(
        self,
    ) -> None:
        # Arrange
        di_container = di.Container()

        # Act
        mediator = bootstrap.bootstrap_streaming(di_container=di_container)

        # Assert
        assert isinstance(mediator, cqrs.StreamingRequestMediator)

    @pytest.mark.asyncio
    async def test_bootstrap_streaming_with_cqrs_container(self) -> None:
        # Arrange
        container = MockCQRSContainer()

        # Act
        mediator = bootstrap.bootstrap_streaming(di_container=container)

        # Assert
        assert isinstance(mediator, cqrs.StreamingRequestMediator)
        assert getattr(mediator._dispatcher._container, "root") is container

    @pytest.mark.asyncio
    async def test_bootstrap_streaming_calls_on_startup(self) -> None:
        # Arrange
        di_container = di.Container()
        called: list[str] = []

        def on_startup() -> None:
            called.append("startup")

        # Act
        bootstrap.bootstrap_streaming(
            di_container=di_container,
            on_startup=[on_startup],
        )

        # Assert
        assert called == ["startup"]

    @pytest.mark.asyncio
    async def test_bootstrap_streaming_with_on_startup_none(self) -> None:
        # Arrange
        di_container = di.Container()

        # Act & Assert
        mediator = bootstrap.bootstrap_streaming(
            di_container=di_container,
            on_startup=None,
        )
        assert isinstance(mediator, cqrs.StreamingRequestMediator)

    @pytest.mark.asyncio
    async def test_bootstrap_streaming_with_custom_message_broker(self) -> None:
        # Arrange
        di_container = di.Container()
        broker = devnull.DevnullMessageBroker()

        # Act
        mediator = bootstrap.bootstrap_streaming(
            di_container=di_container,
            message_broker=broker,
        )

        # Assert
        assert mediator._event_processor._event_emitter is not None
        assert mediator._event_processor._event_emitter._message_broker is broker
