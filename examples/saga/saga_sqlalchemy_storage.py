"""
Example: Saga Pattern with SQLAlchemy Storage (Connection Pool)

This example demonstrates how to configure and use the Saga Pattern with
SqlAlchemySagaStorage, utilizing a database connection pool for efficient
resource management.

Key features demonstrated:
1. Configuring SQLAlchemy async engine with connection pooling
2. Initializing SqlAlchemySagaStorage with a session factory
3. Executing sagas with persistent state in a database (SQLite/MySQL in this example)
4. Scoped run: storage.create_run() is used automatically—one session per saga,
   checkpoint commits after each step (fewer commits and sessions than the legacy path)

Requirements:
    pip install sqlalchemy[asyncio] aiosqlite
"""

import asyncio
import dataclasses
import logging
import os
import typing
import uuid

import di
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncEngine, create_async_engine

import cqrs
from cqrs import Event
from cqrs.response import Response
from cqrs.saga import bootstrap
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.sqlalchemy import Base, SqlAlchemySagaStorage

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database Configuration
# Using SQLite for this example, but can be swapped for PostgreSQL/MySQL via DATABASE_URL
DB_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./test.db")


# ============================================================================
# Domain Models (Same as in basic saga example)
# ============================================================================


@dataclasses.dataclass
class OrderContext(SagaContext):
    """Shared context passed between all saga steps."""

    order_id: str
    total_amount: float
    status: str = "PENDING"


class ProcessPaymentResponse(Response):
    transaction_id: str


# ============================================================================
# Steps
# ============================================================================


class ProcessPaymentStep(SagaStepHandler[OrderContext, ProcessPaymentResponse]):
    """A sample step that simulates payment processing."""

    @property
    def events(self) -> typing.List[Event]:
        return []

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ProcessPaymentResponse]:
        logger.info(f"Processing payment for order {context.order_id}")

        # Simulate processing time
        await asyncio.sleep(0.1)

        context.status = "PAID"

        return self._generate_step_result(
            ProcessPaymentResponse(transaction_id=f"txn_{uuid.uuid4().hex[:8]}"),
        )

    async def compensate(self, context: OrderContext) -> None:
        logger.info(f"Refunding payment for order {context.order_id}")
        context.status = "REFUNDED"


class ShipOrderStep(SagaStepHandler[OrderContext, Response]):
    """A sample step that simulates shipping."""

    @property
    def events(self) -> typing.List[Event]:
        return []

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, Response]:
        logger.info(f"Shipping order {context.order_id}")
        context.status = "SHIPPED"
        return self._generate_step_result(Response())

    async def compensate(self, context: OrderContext) -> None:
        logger.info(f"Cancelling shipment for order {context.order_id}")
        context.status = "SHIPMENT_CANCELLED"


# ============================================================================
# Saga Definition
# ============================================================================


class OrderSaga(Saga[OrderContext]):
    steps = [
        ProcessPaymentStep,
        ShipOrderStep,
    ]


# ============================================================================
# Setup & Execution
# ============================================================================


async def setup_database(engine: AsyncEngine) -> None:
    """Initialize database tables."""
    async with engine.begin() as conn:
        # For this example, we drop and recreate tables to start fresh
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


async def main() -> None:
    """
    Run a demonstration that executes an OrderSaga using an async SQLAlchemy engine and persistent SqlAlchemySagaStorage.

    Initializes a pooled async SQLAlchemy engine and schema, creates a session factory and SqlAlchemySagaStorage, bootstraps a mediator with a DI container and saga mapper, runs an OrderSaga while streaming step results to stdout, and then reloads and prints the persisted saga state and step history before disposing the engine.
    """
    # 1. Create SQLAlchemy Engine with Connection Pool
    # SQLAlchemy creates a pool by default (QueuePool for most dialects, SingletonThreadPool for SQLite)
    engine = create_async_engine(
        DB_URL,
        echo=False,  # Set to True to see SQL queries
        pool_size=5,  # Example pool configuration
        max_overflow=10,
    )

    try:
        # 2. Initialize Database Tables
        await setup_database(engine)

        # 3. Create Session Factory
        # Used by the storage; when the saga runs, create_run() yields one session per saga
        # with checkpoint commits (after each step), reducing round-trips vs legacy path.
        session_factory = async_sessionmaker(engine, expire_on_commit=False)

        # 4. Initialize SqlAlchemySagaStorage
        # Supports create_run(): execution uses one session per saga and checkpoint commits.
        saga_storage = SqlAlchemySagaStorage(session_factory)

        # 5. Setup Dependency Injection
        di_container = di.Container()

        # 6. Configure Saga Mapper
        def saga_mapper(mapper: cqrs.SagaMap) -> None:
            mapper.bind(OrderContext, OrderSaga)

        # 7. Bootstrap Mediator
        mediator = bootstrap.bootstrap(
            di_container=di_container,
            sagas_mapper=saga_mapper,
            saga_storage=saga_storage,
        )

        # 8. Execute Saga
        saga_id = uuid.uuid4()
        context = OrderContext(order_id="123", total_amount=100.0)

        print(f"Starting saga {saga_id}...")

        async for result in mediator.stream(context, saga_id=saga_id):
            print(f"Step completed: {result.step_type.__name__}")

        # 9. Verify persistence by reloading from new storage instance
        print("\nVerifying persistence...")
        status, loaded_context, _ = await saga_storage.load_saga_state(saga_id)
        history = await saga_storage.get_step_history(saga_id)

        print(f"Saga Status: {status}")
        print(f"Context Status: {loaded_context['status']}")
        print(f"History Steps: {len(history)}")
        for entry in history:
            print(f" - {entry.step_name}: {entry.status.value}")

    finally:
        # Cleanup
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
