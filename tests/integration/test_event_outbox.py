import typing

import pydantic
import sqlalchemy as sqla
from sqlalchemy.orm import registry

import cqrs
from cqrs import events, Request, RequestHandler
from cqrs.outbox import (
    repository as outbox_repository,
    repository as repository_protocol,
    sqlalchemy,
)


class OutboxRequest(Request):
    message: typing.Text
    count: int


class ECSTPayload(pydantic.BaseModel):
    message: typing.Text


cqrs.OutboxedEventMap.register(
    "OutboxRequestHandler",
    events.NotificationEvent[ECSTPayload],
)


class OutboxRequestHandler(RequestHandler[OutboxRequest, None]):
    def __init__(self, repository: cqrs.OutboxedEventRepository):
        self.repository = repository

    @property
    def events(self) -> typing.Sequence[events.IEvent]:
        return []

    async def handle(self, request: OutboxRequest) -> None:
        list(
            map(
                lambda e: self.repository.add(e),
                [
                    events.NotificationEvent[ECSTPayload](
                        event_name=OutboxRequestHandler.__name__,
                        payload=ECSTPayload(message=request.message),
                    )
                    for _ in range(request.count)
                ],
            ),
        )
        await self.repository.commit()


class TestOutbox:
    async def test_outbox_add_3_event_positive(self, session):
        """
        checks positive save events to outbox case
        """
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(session)
        request = OutboxRequest(message="test_outbox_add_3_event_positive", count=3)
        await OutboxRequestHandler(repository).handle(request)

        not_produced_events: typing.List[outbox_repository.OutboxedEvent] = await repository.get_many(3)
        await session.commit()

        assert len(not_produced_events) == 3
        assert all(
            filter(
                lambda e: e.event.payload.message == "test_outbox_add_3_event_positive",
                not_produced_events,
            ),
        )
        assert all(
            filter(
                lambda e: e.event.event_name == "OutboxRequestHandler",  # type: ignore
                not_produced_events,
            ),
        )

    async def test_get_new_events_positive(self, session):
        """
        checks getting many new events
        """
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(session)
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=3,
        )
        await OutboxRequestHandler(repository).handle(request)

        events_list = await repository.get_many(3)
        await session.commit()

        assert len(events_list) == 3

    async def test_get_new_events_negative(self, session):
        """
        checks getting many new events, but not produced
        """
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(session)
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=3,
        )
        await OutboxRequestHandler(repository).handle(request)
        events_list = await repository.get_many(3)
        await repository.update_status(
            events_list[-1].id,
            repository_protocol.EventStatus.PRODUCED,  # type: ignore[arg-type]
        )
        await session.commit()

        new_events_list = await repository.get_many(3)
        await session.commit()

        assert len(new_events_list) == 2

    async def test_get_new_event_positive(self, session):
        """
        checks getting one event positive
        """
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(session)
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=1,
        )
        await OutboxRequestHandler(repository).handle(request)
        [event_over_get_all_events_method] = await repository.get_many(1)

        event: typing.Optional[outbox_repository.OutboxedEvent] = next(
            iter(
                await repository.get_many(
                    batch_size=1,
                ),
            ),
            None,
        )
        await session.commit()

        assert event
        assert event.id == event_over_get_all_events_method.id  # noqa

    async def test_get_new_event_negative(self, session):
        """
        checks getting one event positive, but not produced
        """
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(session)
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=1,
        )
        await OutboxRequestHandler(repository).handle(request)
        [event_over_get_all_events_method] = await repository.get_many(1)
        await repository.update_status(
            event_over_get_all_events_method.id,
            repository_protocol.EventStatus.PRODUCED,  # type: ignore[arg-type]
        )
        await session.commit()

        event = await repository.get_many(
            batch_size=1,
        )
        await session.commit()

        assert not event

    async def test_mark_as_failure_positive(self, session):
        """checks reading failure produced event successfully"""

        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(session)
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=2,
        )
        await OutboxRequestHandler(repository).handle(request)
        [failure_event, success_event] = await repository.get_many(2)
        # mark FIRST event as failure
        await repository.update_status(
            failure_event.id,
            repository_protocol.EventStatus.NOT_PRODUCED,  # type: ignore[arg-type]
        )
        await session.commit()

        produce_candidates = await repository.get_many(batch_size=2)

        assert len(produce_candidates) == 2
        # check events order by status
        assert produce_candidates[0].id == success_event.id
        assert produce_candidates[1].id == failure_event.id

    async def test_mark_as_failure_negative(self, session):
        """checks reading failure produced events with flush_counter speeding"""

        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(session)
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=1,
        )
        await OutboxRequestHandler(repository).handle(request)
        [failure_event] = await repository.get_many(1)
        for _ in range(sqlalchemy.MAX_FLUSH_COUNTER_VALUE):
            await repository.update_status(
                failure_event.id,
                repository_protocol.EventStatus.NOT_PRODUCED,  # type: ignore[arg-type]
            )

        await session.commit()

        produce_candidates = await repository.get_many(batch_size=1)

        assert not len(produce_candidates)


async def test_rebind_outbox_model_positive(init_orm, session):
    custom_base = registry().generate_base()
    sqlalchemy.rebind_outbox_model(sqlalchemy.OutboxModel, custom_base, "rebind_outbox")
    await init_orm.run_sync(custom_base.metadata.create_all)

    async with session.begin():
        await session.execute(sqla.text("SELECT * FROM rebind_outbox WHERE True;"))
