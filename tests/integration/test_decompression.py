import typing

import cqrs
from cqrs.outbox import repository as outbox_repository


async def test_decompression_positive(session):
    repository = cqrs.SqlAlchemyOutboxedEventRepository(
        session,
        cqrs.ZlibCompressor(),
    )
    event = cqrs.NotificationEvent[typing.Dict](
        event_name="TestEvent",
        payload={"foo": "bar"},
    )
    cqrs.OutboxedEventMap.register("TestEvent", cqrs.NotificationEvent[typing.Dict])

    repository.add(event)
    await session.commit()

    read_event: typing.Optional[outbox_repository.OutboxedEvent] = next(
        iter(
            await repository.get_many(
                batch_size=1,
            ),
        ),
        None,
    )

    assert read_event
    assert read_event.event.event_id == event.event_id
    assert read_event.event.payload
    assert read_event.event.payload["foo"] == "bar"
