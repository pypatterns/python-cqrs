"""SQLModel saga storage on MySQL and PostgreSQL."""

import datetime
import os
import uuid

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from cqrs.dispatcher.exceptions import SagaConcurrencyError
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.sqlmodel import Base, SqlModelSagaStorage

pytestmark = [
    pytest.mark.sqlmodel,
    pytest.mark.filterwarnings("ignore:.*session.exec\\(\\) instead of:DeprecationWarning"),
]


def _live_dsns() -> list[tuple[str, str]]:
    candidates = [
        ("mysql", os.environ.get("DATABASE_DSN_MYSQL", "") or os.environ.get("DATABASE_DSN", "")),
        ("postgres", os.environ.get("DATABASE_DSN_POSTGRESQL", "")),
    ]
    return [(name, dsn) for name, dsn in candidates if dsn]


@pytest.fixture(params=_live_dsns() or [("missing", "")], ids=lambda item: item[0])
async def storage(request: pytest.FixtureRequest):
    _name, dsn = request.param
    if not dsn:
        pytest.skip("DATABASE_DSN_MYSQL / DATABASE_DSN_POSTGRESQL not set")
    engine = create_async_engine(dsn, pool_pre_ping=True)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    yield SqlModelSagaStorage(factory)
    await engine.dispose()


def _saga_id() -> uuid.UUID:
    return uuid.uuid4()


class TestSqlModelSagaStorage:
    async def test_full_lifecycle(self, storage: SqlModelSagaStorage) -> None:
        saga_id = _saga_id()
        context = {"order_id": "123", "amount": "100.0"}
        await storage.create_saga(saga_id, "order_saga", context)
        await storage.update_status(saga_id, SagaStatus.RUNNING)
        await storage.log_step(saga_id, "reserve_inventory", "act", SagaStepStatus.STARTED)
        await storage.log_step(
            saga_id,
            "reserve_inventory",
            "act",
            SagaStepStatus.COMPLETED,
            details="ok",
        )
        updated = {**context, "payment_id": "pay_123"}
        await storage.update_context(saga_id, updated)
        await storage.update_status(saga_id, SagaStatus.COMPLETED)

        status, stored_context, version = await storage.load_saga_state(saga_id, read_for_update=True)
        assert status == SagaStatus.COMPLETED
        assert stored_context == updated
        assert version == 4

        history = await storage.get_step_history(saga_id)
        assert [entry.step_name for entry in history] == ["reserve_inventory", "reserve_inventory"]
        assert history[0].action == "act"
        assert history[0].status == SagaStepStatus.STARTED
        assert history[1].details == "ok"
        assert history[0].timestamp.tzinfo is not None

    async def test_load_missing_saga_raises(self, storage: SqlModelSagaStorage) -> None:
        with pytest.raises(ValueError, match="not found"):
            await storage.load_saga_state(uuid.uuid4())

    async def test_optimistic_locking(self, storage: SqlModelSagaStorage) -> None:
        saga_id = _saga_id()
        await storage.create_saga(saga_id, "order_saga", {"n": 1})
        _, _, version = await storage.load_saga_state(saga_id)
        await storage.update_context(saga_id, {"n": 2}, current_version=version)
        _, stored, new_version = await storage.load_saga_state(saga_id)
        assert stored == {"n": 2}
        assert new_version == version + 1
        with pytest.raises(SagaConcurrencyError):
            await storage.update_context(saga_id, {"n": 3}, current_version=version)

    async def test_optimistic_locking_missing_saga(self, storage: SqlModelSagaStorage) -> None:
        with pytest.raises(SagaConcurrencyError):
            await storage.update_context(uuid.uuid4(), {"n": 1}, current_version=1)

    async def test_update_context_without_version_check(self, storage: SqlModelSagaStorage) -> None:
        saga_id = _saga_id()
        await storage.create_saga(saga_id, "order_saga", {"n": 1})
        await storage.update_context(saga_id, {"n": 9})
        _, stored, version = await storage.load_saga_state(saga_id)
        assert stored == {"n": 9}
        assert version == 2

    async def test_update_status_missing_saga_is_a_no_op(self, storage: SqlModelSagaStorage) -> None:
        # The immediate storage path matches SQLAlchemy: a missing id updates
        # zero rows and does not raise. The checkpoint run does raise.
        missing = uuid.uuid4()
        await storage.update_status(missing, SagaStatus.FAILED)
        with pytest.raises(ValueError, match="not found"):
            await storage.load_saga_state(missing)

    async def test_recovery_queries(self, storage: SqlModelSagaStorage) -> None:
        running = _saga_id()
        compensating = _saga_id()
        pending = _saga_id()
        completed = _saga_id()
        exhausted = _saga_id()
        other_name = _saga_id()
        await storage.create_saga(running, "order_saga", {})
        await storage.create_saga(compensating, "order_saga", {})
        await storage.create_saga(pending, "order_saga", {})
        await storage.create_saga(completed, "order_saga", {})
        await storage.create_saga(exhausted, "order_saga", {})
        await storage.create_saga(other_name, "payment_saga", {})
        await storage.update_status(running, SagaStatus.RUNNING)
        await storage.update_status(compensating, SagaStatus.COMPENSATING)
        await storage.update_status(completed, SagaStatus.COMPLETED)
        await storage.update_status(exhausted, SagaStatus.RUNNING)
        await storage.update_status(other_name, SagaStatus.RUNNING)
        await storage.set_recovery_attempts(exhausted, 5)

        found = await storage.get_sagas_for_recovery(limit=10)
        assert running in found
        assert compensating in found
        assert other_name in found
        assert pending not in found
        assert completed not in found
        assert exhausted not in found

        limited = await storage.get_sagas_for_recovery(limit=1)
        assert len([item for item in limited if item in {running, compensating, other_name}]) == 1

        named = await storage.get_sagas_for_recovery(limit=10, saga_name="payment_saga")
        assert other_name in named
        assert running not in named

        fresh = await storage.get_sagas_for_recovery(limit=10, stale_after_seconds=3600)
        assert running not in fresh

    async def test_increment_and_set_recovery_attempts(self, storage: SqlModelSagaStorage) -> None:
        saga_id = _saga_id()
        await storage.create_saga(saga_id, "order_saga", {})
        await storage.update_status(saga_id, SagaStatus.RUNNING)
        await storage.increment_recovery_attempts(saga_id, SagaStatus.FAILED)
        status, _, _ = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.FAILED
        found = await storage.get_sagas_for_recovery(limit=10, max_recovery_attempts=1)
        assert saga_id not in found

        await storage.set_recovery_attempts(saga_id, 0)
        await storage.update_status(saga_id, SagaStatus.RUNNING)
        found_again = await storage.get_sagas_for_recovery(limit=10)
        assert saga_id in found_again

        with pytest.raises(ValueError, match="not found"):
            await storage.increment_recovery_attempts(uuid.uuid4())
        with pytest.raises(ValueError, match="not found"):
            await storage.set_recovery_attempts(uuid.uuid4(), 1)

    async def test_create_run_commits_and_rolls_back(self, storage: SqlModelSagaStorage) -> None:
        committed = _saga_id()
        async with storage.create_run() as run:
            await run.create_saga(committed, "order_saga", {"from": "run"})
            await run.update_status(committed, SagaStatus.RUNNING)
            await run.log_step(committed, "ship", "act", SagaStepStatus.COMPLETED)
            await run.commit()

        status, context, version = await storage.load_saga_state(committed)
        assert status == SagaStatus.RUNNING
        assert context == {"from": "run"}
        assert version == 2
        assert len(await storage.get_step_history(committed)) == 1

        rolled_back = _saga_id()
        with pytest.raises(RuntimeError, match="boom"):
            async with storage.create_run() as run:
                await run.create_saga(rolled_back, "order_saga", {})
                raise RuntimeError("boom")

        with pytest.raises(ValueError, match="not found"):
            await storage.load_saga_state(rolled_back)

    async def test_create_run_optimistic_lock_and_history(self, storage: SqlModelSagaStorage) -> None:
        saga_id = _saga_id()
        async with storage.create_run() as run:
            await run.create_saga(saga_id, "order_saga", {"n": 1})
            await run.commit()
            await run.log_step(saga_id, "second", "compensate", SagaStepStatus.FAILED, details="later")
            await run.update_context(saga_id, {"n": 2}, current_version=1)
            await run.commit()

        _, context, version = await storage.load_saga_state(saga_id)
        assert context == {"n": 2}
        assert version == 2
        history = await storage.get_step_history(saga_id)
        assert history[0].action == "compensate"
        assert history[0].status == SagaStepStatus.FAILED
        assert history[0].timestamp <= datetime.datetime.now(datetime.timezone.utc)

        async with storage.create_run() as run:
            with pytest.raises(SagaConcurrencyError):
                await run.update_context(saga_id, {"n": 3}, current_version=1)
            with pytest.raises(SagaConcurrencyError):
                await run.update_status(uuid.uuid4(), SagaStatus.FAILED)
