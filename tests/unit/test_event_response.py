from uuid import UUID, uuid4

import pydantic
import pytest

import cqrs
from cqrs import events


class CloseMeetingRoomEvent(pydantic.BaseModel):
    meeting_room_id: UUID = pydantic.Field()


class CloseMeetingRoomEventHandler(
    events.EventHandler[cqrs.PydanticNotificationEvent[CloseMeetingRoomEvent]],
):
    def __init__(self) -> None:
        self.called = False

    async def handle(
        self,
        event: cqrs.PydanticNotificationEvent[CloseMeetingRoomEvent],
    ) -> None:
        self.called = True


class TestContainer:
    event_handler = CloseMeetingRoomEventHandler()

    async def resolve(self, type_) -> CloseMeetingRoomEventHandler:
        if type_ is CloseMeetingRoomEventHandler:
            return self.event_handler
        raise Exception(f"Handler of type {type_} not found")


@pytest.fixture
def mediator() -> cqrs.EventMediator:
    event_map = events.EventMap()
    event_map.bind(
        cqrs.PydanticNotificationEvent[CloseMeetingRoomEvent],
        CloseMeetingRoomEventHandler,
    )

    return cqrs.EventMediator(
        event_map=event_map,
        container=TestContainer(),  # type: ignore
    )


async def test_sending_event_without_response(mediator: cqrs.EventMediator) -> None:
    handler: CloseMeetingRoomEventHandler = await TestContainer().resolve(
        CloseMeetingRoomEventHandler,
    )

    await mediator.send(
        event=cqrs.PydanticNotificationEvent[CloseMeetingRoomEvent](
            event_name="CloseMeetingRoomEvent",
            payload=CloseMeetingRoomEvent(meeting_room_id=uuid4()),
        ),
    )

    assert handler
    assert handler.called
