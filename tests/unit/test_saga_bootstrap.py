"""
AAA unit tests for cqrs.saga.bootstrap.

Covers setup_saga_mediator and bootstrap for SagaMediator (increased coverage).
"""

import dataclasses
import typing

import di
import pytest

import cqrs
from cqrs import events
from cqrs.container import di as di_container_impl
from cqrs.message_brokers import devnull
from cqrs.middlewares import logging as logging_middleware
from cqrs.bootstrap import requests as requests_bootstrap
from cqrs.mapping.requests import SagaMap
from cqrs.bootstrap import saga as bootstrap
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.storage.memory import MemorySagaStorage
from cqrs.saga.storage.protocol import ISagaStorage


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


# Stub saga context and saga for sagas_mapper (SagaMap.bind)
@dataclasses.dataclass
class _StubSagaContext(SagaContext):
    id: str = ""


class _StubSaga(Saga[_StubSagaContext]):
    steps: typing.ClassVar[list] = []


# Stub event/handler for domain_events_mapper
class _StubEvent(events.DomainEvent, frozen=True):
    pass


class _StubEventHandler(events.EventHandler[_StubEvent]):
    async def handle(self, event: _StubEvent) -> None:
        pass


# ---------------------------------------------------------------------------
# Test setup_saga_mediator
# ---------------------------------------------------------------------------


class TestSetupSagaMediator:
    """AAA tests for saga.bootstrap.setup_saga_mediator."""

    def test_setup_saga_mediator_returns_saga_mediator(self) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = requests_bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]

        # Act
        mediator = bootstrap.setup_saga_mediator(
            emitter,
            container,
            middlewares=middlewares,
        )

        # Assert
        assert isinstance(mediator, cqrs.SagaMediator)
        assert mediator._dispatcher is not None
        assert getattr(mediator._dispatcher._container, "root") is container
        assert mediator._dispatcher._saga_map is not None

    def test_setup_saga_mediator_with_sagas_mapper_registers_sagas(self) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = requests_bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]
        saga_map_received: list[SagaMap] = []

        def sagas_mapper(m: SagaMap) -> None:
            saga_map_received.append(m)
            m.bind(_StubSagaContext, _StubSaga)

        # Act
        mediator = bootstrap.setup_saga_mediator(
            emitter,
            container,
            middlewares=middlewares,
            sagas_mapper=sagas_mapper,
        )

        # Assert
        assert len(saga_map_received) == 1
        assert mediator._dispatcher._saga_map is saga_map_received[0]

    def test_setup_saga_mediator_with_custom_event_map_uses_provided_map(
        self,
    ) -> None:
        # Arrange: non-empty EventMap (empty dict is falsy in mediator)
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = requests_bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]
        custom_event_map = events.EventMap()
        custom_event_map.bind(_StubEvent, _StubEventHandler)

        # Act
        mediator = bootstrap.setup_saga_mediator(
            emitter,
            container,
            middlewares=middlewares,
            event_map=custom_event_map,
        )

        # Assert
        assert mediator._event_processor._event_map is custom_event_map

    def test_setup_saga_mediator_with_saga_storage_passes_to_mediator(
        self,
    ) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = requests_bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]
        storage = MemorySagaStorage()

        # Act
        mediator = bootstrap.setup_saga_mediator(
            emitter,
            container,
            middlewares=middlewares,
            saga_storage=storage,
        )

        # Assert
        assert mediator._dispatcher._storage is storage

    def test_setup_saga_mediator_with_max_concurrent_and_concurrent_enable(
        self,
    ) -> None:
        # Arrange
        container = di_container_impl.DIContainer()
        container.attach_external_container(di.Container())
        emitter = requests_bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]

        # Act
        mediator = bootstrap.setup_saga_mediator(
            emitter,
            container,
            middlewares=middlewares,
            max_concurrent_event_handlers=7,
            concurrent_event_handle_enable=False,
        )

        # Assert
        assert mediator._event_processor._max_concurrent_event_handlers == 7
        assert mediator._event_processor._concurrent_event_handle_enable is False

    def test_setup_saga_mediator_with_cqrs_container(self) -> None:
        # Arrange
        container = MockCQRSContainer()
        emitter = requests_bootstrap.setup_event_emitter(container)
        middlewares = [logging_middleware.LoggingMiddleware()]

        # Act
        mediator = bootstrap.setup_saga_mediator(
            emitter,
            container,
            middlewares=middlewares,
        )

        # Assert
        assert isinstance(mediator, cqrs.SagaMediator)
        assert getattr(mediator._dispatcher._container, "root") is container


# ---------------------------------------------------------------------------
# Test bootstrap (saga)
# ---------------------------------------------------------------------------


class TestBootstrapSaga:
    """AAA tests for saga.bootstrap.bootstrap."""

    @pytest.mark.asyncio
    async def test_bootstrap_with_di_container_returns_saga_mediator(
        self,
    ) -> None:
        # Arrange
        di_container = di.Container()

        # Act
        mediator = bootstrap.bootstrap(di_container=di_container)

        # Assert
        assert isinstance(mediator, cqrs.SagaMediator)
        assert mediator._dispatcher is not None
        assert mediator._event_processor is not None

    @pytest.mark.asyncio
    async def test_bootstrap_with_cqrs_container_returns_saga_mediator(
        self,
    ) -> None:
        # Arrange
        container = MockCQRSContainer()

        # Act
        mediator = bootstrap.bootstrap(di_container=container)

        # Assert
        assert isinstance(mediator, cqrs.SagaMediator)
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
        assert isinstance(mediator, cqrs.SagaMediator)

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

        # Assert
        assert mediator._dispatcher._middleware_chain is not None
        assert isinstance(mediator, cqrs.SagaMediator)

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
        assert isinstance(mediator, cqrs.SagaMediator)

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
    async def test_bootstrap_with_sagas_mapper(self) -> None:
        # Arrange
        di_container = di.Container()
        saga_map_received: list[SagaMap] = []

        def sagas_mapper(m: SagaMap) -> None:
            saga_map_received.append(m)
            m.bind(_StubSagaContext, _StubSaga)

        # Act
        mediator = bootstrap.bootstrap(
            di_container=di_container,
            sagas_mapper=sagas_mapper,
        )

        # Assert
        assert len(saga_map_received) == 1
        assert mediator._dispatcher._saga_map is saga_map_received[0]

    @pytest.mark.asyncio
    async def test_bootstrap_with_domain_events_mapper(self) -> None:
        # Arrange
        di_container = di.Container()
        event_map_received: list[events.EventMap] = []

        def domain_events_mapper(m: events.EventMap) -> None:
            event_map_received.append(m)
            m.bind(_StubEvent, _StubEventHandler)

        # Act
        mediator = bootstrap.bootstrap(
            di_container=di_container,
            domain_events_mapper=domain_events_mapper,
        )

        # Assert
        assert len(event_map_received) == 1
        assert mediator._event_processor._event_map is event_map_received[0]

    @pytest.mark.asyncio
    async def test_bootstrap_with_saga_storage(self) -> None:
        # Arrange
        di_container = di.Container()
        storage: ISagaStorage = MemorySagaStorage()

        # Act
        mediator = bootstrap.bootstrap(
            di_container=di_container,
            saga_storage=storage,
        )

        # Assert
        assert mediator._dispatcher._storage is storage

    @pytest.mark.asyncio
    async def test_bootstrap_with_max_concurrent_and_concurrent_enable(
        self,
    ) -> None:
        # Arrange
        di_container = di.Container()

        # Act
        mediator = bootstrap.bootstrap(
            di_container=di_container,
            max_concurrent_event_handlers=4,
            concurrent_event_handle_enable=False,
        )

        # Assert
        assert mediator._event_processor._max_concurrent_event_handlers == 4
        assert mediator._event_processor._concurrent_event_handle_enable is False
