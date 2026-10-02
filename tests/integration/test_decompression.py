import typing

import cqrs
from cqrs.outbox import repository as outbox_repository


async def test_decompression_positive(session):
    repository = cqrs.SqlAlchemyOutboxedEventRepository(
        session,
        cqrs.ZlibCompressor(),
    )
    event = cqrs.PydanticNotificationEvent[typing.Dict](
        event_name="TestEvent",
        payload={"foo": "bar"},
    )
    cqrs.OutboxedEventMap.register("TestEvent", cqrs.PydanticNotificationEvent[typing.Dict])

    repository.add(event)
    await session.commit()

    read_event: outbox_repository.OutboxedEvent | None = next(
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
