"""Shared fixtures and legacy storage classes for benchmarks."""

from __future__ import annotations

import asyncio
import contextlib
import os
import typing

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from cqrs.saga.storage.memory import MemorySagaStorage
from cqrs.saga.storage.protocol import SagaStorageRun
from cqrs.saga.storage.sqlalchemy import Base, SqlAlchemySagaStorage


class MemorySagaStorageLegacy(MemorySagaStorage):
    """Memory storage without create_run: forces legacy path (commit per call)."""

    def create_run(
        self,
    ) -> contextlib.AbstractAsyncContextManager[SagaStorageRun]:
        """Raise NotImplementedError so benchmarks use the legacy commit-per-call path."""
        raise NotImplementedError("Legacy storage: create_run disabled for benchmark")


class SqlAlchemySagaStorageLegacy(SqlAlchemySagaStorage):
    """SQLAlchemy storage without create_run: forces legacy path (commit per call)."""

    def create_run(
        self,
    ) -> contextlib.AbstractAsyncContextManager[SagaStorageRun]:
        """Raise NotImplementedError so benchmarks use the legacy commit-per-call path."""
        raise NotImplementedError("Legacy storage: create_run disabled for benchmark")


@pytest.fixture(scope="session")
def database_dsn() -> typing.Optional[str]:
    """DATABASE_DSN from environment (set in CI by pytest-config.ini)."""
    return os.environ.get("DATABASE_DSN") or None


@pytest.fixture(scope="session")
def saga_benchmark_loop_and_engine(database_dsn: typing.Optional[str]):
    """
    One event loop and one async engine for the whole benchmark session.
    Used by saga SQLAlchemy benchmarks so connection setup/teardown is not measured.
    Fails (no skip) if DATABASE_DSN is unset or connection fails, so CI shows the real error.
    """
    if not database_dsn:
        pytest.fail("DATABASE_DSN not set; set it in CI (e.g. in codspeed.yml env) to run saga SQLAlchemy benchmarks")

    loop = asyncio.new_event_loop()
    engine = create_async_engine(
        database_dsn,
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=4,
        echo=False,
    )

    async def ensure_tables() -> None:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    loop.run_until_complete(ensure_tables())

    try:
        yield (loop, engine)
    finally:
        loop.run_until_complete(engine.dispose())
        loop.close()
