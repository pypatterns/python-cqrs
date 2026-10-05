import typing

import pytest
import redis
from aiobreaker.storage.memory import CircuitMemoryStorage
from aiobreaker.storage.redis import CircuitRedisStorage
from aiobreaker import CircuitBreakerState
from unittest import mock

import cqrs
from cqrs import events
from cqrs.mapping.requests import SagaMap
from cqrs.saga.storage.sqlalchemy import SqlAlchemySagaStorage

from tests.integration.test_saga_mediator_memory import (
    FailingOrderSaga,
    FailingStep,
    InventoryReservedEvent,
    InventoryReservedEventHandler,
    OrderContext,
    OrderShippedEvent,
    OrderShippedEventHandler,
    OrderSaga,
    PaymentProcessedEvent,
    PaymentProcessedEventHandler,
    ProcessPaymentStep,
    ReserveInventoryStep,
    ShipOrderStep,
)


class _TestContainer:
    """Test container that resolves step handlers, sagas, and event handlers (shared by SQLAlchemy mediator tests)."""

    def __init__(self, storage: SqlAlchemySagaStorage) -> None:
        self._storage = storage
        self._external_container = None
        self._step_handlers = {
            ReserveInventoryStep: ReserveInventoryStep(),
            ProcessPaymentStep: ProcessPaymentStep(),
            ShipOrderStep: ShipOrderStep(),
            FailingStep: FailingStep(),
        }
        self._event_handlers = {
            InventoryReservedEventHandler: InventoryReservedEventHandler(),
            PaymentProcessedEventHandler: PaymentProcessedEventHandler(),
            OrderShippedEventHandler: OrderShippedEventHandler(),
        }
        self._sagas = {
            OrderSaga: OrderSaga(),  # type: ignore[arg-type]
            FailingOrderSaga: FailingOrderSaga(),  # type: ignore[arg-type]
        }

    @property
    def external_container(self) -> typing.Any:
        return self._external_container

    def attach_external_container(self, container: typing.Any) -> None:
        self._external_container = container

    async def resolve(self, type_) -> typing.Any:
        if type_ in self._step_handlers:
            return self._step_handlers[type_]
        if type_ in self._event_handlers:
            return self._event_handlers[type_]
        if type_ in self._sagas:
            return self._sagas[type_]
        if type_ == SqlAlchemySagaStorage:
            return self._storage
        raise ValueError(f"Unknown type: {type_}")


@pytest.fixture
def container(storage: SqlAlchemySagaStorage) -> _TestContainer:
    """Create test container (shared by SQLAlchemy mediator tests; requires storage fixture from test module)."""
    container = _TestContainer(storage)
    for step_handler in container._step_handlers.values():
        if hasattr(step_handler, "_events"):
            step_handler._events.clear()
    for event_handler in container._event_handlers.values():
        if hasattr(event_handler, "handled_events"):
            event_handler.handled_events.clear()
    return container


@pytest.fixture
def saga_mediator(
    container: _TestContainer,
    storage: SqlAlchemySagaStorage,
) -> cqrs.SagaMediator:
    """Create SagaMediator with SqlAlchemySagaStorage (shared; storage comes from test module)."""

    def saga_mapper(mapper: SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    def events_mapper(mapper: events.EventMap) -> None:
        mapper.bind(InventoryReservedEvent, InventoryReservedEventHandler)
        mapper.bind(PaymentProcessedEvent, PaymentProcessedEventHandler)
        mapper.bind(OrderShippedEvent, OrderShippedEventHandler)

    event_map = events.EventMap()
    events_mapper(event_map)
    message_broker = mock.AsyncMock()
    message_broker.produce = mock.AsyncMock()
    event_emitter = events.EventEmitter(
        event_map=event_map,
        container=container,  # type: ignore
        message_broker=message_broker,
    )
    saga_map = SagaMap()
    saga_mapper(saga_map)
    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
        storage=storage,
    )
    return mediator


@pytest.fixture
def memory_storage_factory():
    def _factory(name: str):
        return CircuitMemoryStorage(state=CircuitBreakerState.CLOSED)

    return _factory


@pytest.fixture
def redis_client():
    """
    Creates a real synchronous Redis client connected to the local instance.
    Skips tests if Redis is not available.
    """
    # aiobreaker's CircuitRedisStorage uses synchronous Redis client methods
    # decode_responses=False is critical because aiobreaker tries to .decode('utf-8') the state
    client = redis.from_url(
        "redis://localhost:6379",
        encoding="utf-8",
        decode_responses=False,
    )
    try:
        client.ping()
    except Exception as e:
        client.close()
        pytest.skip(
            f"Redis is not available: {e}. Make sure 'docker-compose up' is running.",
        )

    yield client

    # Clean up
    try:
        client.flushall()
    except Exception:
        pass
    client.close()


@pytest.fixture
def redis_storage_factory(redis_client):
    def _factory(name: str):
        return CircuitRedisStorage(
            state=CircuitBreakerState.CLOSED,
            redis_object=redis_client,
            namespace=name,
        )

    return _factory
