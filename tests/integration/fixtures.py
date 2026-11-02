import contextlib
import functools
import os

import dotenv
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from cqrs.outbox import sqlalchemy

dotenv.load_dotenv()
DATABASE_DSN = os.environ.get("DATABASE_DSN", "")

# DSN for saga tests: separate MySQL and PostgreSQL variables (set in pytest-config.ini / env).
DATABASE_DSN_MYSQL = os.environ.get("DATABASE_DSN_MYSQL", "")
DATABASE_DSN_POSTGRESQL = os.environ.get("DATABASE_DSN_POSTGRESQL", "")


@pytest.fixture(scope="function")
async def init_orm():
    """Initialize outbox tables - drops and creates tables before each test."""
    engine = create_async_engine(
        DATABASE_DSN,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=30,
        echo=False,
    )
    async with engine.begin() as connect:
        await connect.run_sync(sqlalchemy.Base.metadata.drop_all)
        await connect.run_sync(sqlalchemy.Base.metadata.create_all)
        yield connect


@pytest.fixture(scope="function")
async def session(init_orm):
    engine_factory = functools.partial(
        create_async_engine,
        DATABASE_DSN,
        isolation_level="REPEATABLE READ",
    )
    session = async_sessionmaker(engine_factory())()
    async with contextlib.aclosing(session):
        yield session


# --- Outbox: PostgreSQL (DATABASE_DSN points at MySQL, so this is a separate split) ---


@pytest.fixture(scope="function")
async def init_orm_postgres():
    """Create the outbox schema on live PostgreSQL (DATABASE_DSN_POSTGRESQL)."""
    if not DATABASE_DSN_POSTGRESQL:
        pytest.skip("DATABASE_DSN_POSTGRESQL not set")
    engine = create_async_engine(
        DATABASE_DSN_POSTGRESQL,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=30,
        echo=False,
    )
    # PostgreSQL DDL is transactional, so commit the schema before yield:
    # otherwise the open transaction holds a table lock and tests hang.
    async with engine.begin() as connect:
        await connect.run_sync(sqlalchemy.Base.metadata.drop_all)
        await connect.run_sync(sqlalchemy.Base.metadata.create_all)
    async with engine.connect() as connect:
        yield connect
    await engine.dispose()


@pytest.fixture(scope="function")
async def session_postgres(init_orm_postgres):
    engine_factory = functools.partial(
        create_async_engine,
        DATABASE_DSN_POSTGRESQL,
        isolation_level="REPEATABLE READ",
    )
    engine = engine_factory()
    session = async_sessionmaker(engine)()
    async with contextlib.aclosing(session):
        yield session
    await engine.dispose()


# --- Saga storage: MySQL (separate fixtures; they create the schema and everything needed) ---


@pytest.fixture(scope="session")
async def init_saga_orm_mysql():
    """Create the saga schema for MySQL (DATABASE_DSN_MYSQL)."""
    from cqrs.saga.storage.sqlalchemy import Base

    if not DATABASE_DSN_MYSQL:
        pytest.skip("DATABASE_DSN_MYSQL not set")
    engine = create_async_engine(
        DATABASE_DSN_MYSQL,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=30,
        echo=False,
    )
    async with engine.begin() as connect:
        await connect.run_sync(Base.metadata.drop_all)
        await connect.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def saga_session_factory_mysql(init_saga_orm_mysql):
    """Session factory for saga tests on MySQL."""
    return async_sessionmaker(
        init_saga_orm_mysql,
        expire_on_commit=False,
        autocommit=False,
    )


# --- Saga storage: PostgreSQL (separate fixtures; they create the schema and everything needed) ---


@pytest.fixture(scope="session")
async def init_saga_orm_postgres():
    """Create the saga schema for PostgreSQL (DATABASE_DSN_POSTGRESQL)."""
    from cqrs.saga.storage.sqlalchemy import Base

    if not DATABASE_DSN_POSTGRESQL:
        pytest.skip("DATABASE_DSN_POSTGRESQL not set")
    engine = create_async_engine(
        DATABASE_DSN_POSTGRESQL,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=30,
        echo=False,
    )
    async with engine.begin() as connect:
        await connect.run_sync(Base.metadata.drop_all)
        await connect.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def saga_session_factory_postgres(init_saga_orm_postgres):
    """Session factory for saga tests on PostgreSQL."""
    return async_sessionmaker(
        init_saga_orm_postgres,
        expire_on_commit=False,
        autocommit=False,
    )
