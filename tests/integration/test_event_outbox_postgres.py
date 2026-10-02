"""Outbox repository against a live PostgreSQL (acceptance criteria of issue #85).

The MySQL flow is covered by `test_event_outbox.py`; here the point is that the
very same model and repository work on PostgreSQL, where `BINARY(16)` does not
exist and `event_id` is stored as a native `UUID`.
"""

import typing
import uuid

import pydantic
import sqlalchemy as sqla
from sqlalchemy.exc import IntegrityError

import cqrs
from cqrs.outbox import (
    repository as outbox_repository,
    sqlalchemy,
)


class PostgresECSTPayload(pydantic.BaseModel):
    message: typing.Text


cqrs.OutboxedEventMap.register(
    "PostgresOutboxEvent",
    cqrs.PydanticNotificationEvent[PostgresECSTPayload],
)


def make_event(message: typing.Text) -> cqrs.PydanticNotificationEvent:
    return cqrs.PydanticNotificationEvent[PostgresECSTPayload](
        event_name="PostgresOutboxEvent",
        topic="postgres_outbox_topic",
        payload=PostgresECSTPayload(message=message),
    )


class TestOutboxPostgres:
    async def test_add_commit_get_many(self, session_postgres):
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(session_postgres)
        for index in range(3):
            repository.add(make_event(f"message-{index}"))
        await repository.commit()

        produced_candidates = await repository.get_many(3)
        await session_postgres.commit()

        assert len(produced_candidates) == 3
        assert {event.event.payload.message for event in produced_candidates} == {
            "message-0",
            "message-1",
            "message-2",
        }
        assert all(event.topic == "postgres_outbox_topic" for event in produced_candidates)
        assert all(event.status == outbox_repository.EventStatus.NEW for event in produced_candidates)

    async def test_event_id_is_read_back_as_uuid(self, session_postgres):
        """PostgreSQL stores `event_id` as a native UUID, not as 16 bytes."""
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(session_postgres)
        event = make_event("uuid-roundtrip")
        repository.add(event)
        await repository.commit()

        model = (
            await session_postgres.execute(
                sqla.select(sqlalchemy.OutboxModel),
            )
        ).scalar_one()

        assert isinstance(model.event_id, uuid.UUID)
        assert model.event_id == event.event_id
        assert model.event_id_bin == event.event_id.bytes

    async def test_update_status_excludes_produced_event(self, session_postgres):
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(session_postgres)
        repository.add(make_event("produced"))
        repository.add(make_event("still-new"))
        await repository.commit()

        [produced, _] = await repository.get_many(2)
        await repository.update_status(
            produced.id,
            outbox_repository.EventStatus.PRODUCED,  # type: ignore[arg-type]
        )
        await session_postgres.commit()

        rest = await repository.get_many(2)
        await session_postgres.commit()

        assert [event.id for event in rest] != [produced.id]
        assert len(rest) == 1

    async def test_not_produced_event_increments_flush_counter(self, session_postgres):
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(session_postgres)
        repository.add(make_event("failing"))
        await repository.commit()

        [event] = await repository.get_many(1)
        for _ in range(sqlalchemy.MAX_FLUSH_COUNTER_VALUE):
            await repository.update_status(
                event.id,
                outbox_repository.EventStatus.NOT_PRODUCED,  # type: ignore[arg-type]
            )
        await session_postgres.commit()

        assert not await repository.get_many(1)

    async def test_event_status_stored_as_enum_member_name(self, session_postgres):
        """The `eventstatus` PostgreSQL enum holds member names, not their values."""
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(session_postgres)
        repository.add(make_event("enum-check"))
        await repository.commit()

        raw_status = (
            await session_postgres.execute(
                sqla.text("SELECT event_status::text FROM outbox"),
            )
        ).scalar_one()
        await session_postgres.commit()

        assert raw_status == "NEW"

    async def test_unique_constraint_on_event_id_bin(self, session_postgres):
        """`event_id_unique_index` guards idempotency on PostgreSQL as well."""
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(session_postgres)
        event = make_event("duplicate")
        repository.add(event)
        repository.add(event)

        try:
            await repository.commit()
        except IntegrityError:
            await repository.rollback()
        else:
            raise AssertionError("duplicated event_id_bin must violate the constraint")


async def test_outbox_columns_types_on_postgres(init_orm_postgres):
    """The physical schema uses portable PostgreSQL types instead of BINARY."""
    rows = await init_orm_postgres.execute(
        sqla.text(
            "SELECT column_name, data_type, udt_name FROM information_schema.columns " "WHERE table_name = 'outbox'",
        ),
    )
    types = {row.column_name: (row.data_type, row.udt_name) for row in rows}

    assert types["event_id"][1] == "uuid"
    assert types["event_id_bin"][1] == "bytea"
    assert types["payload"][1] == "bytea"
