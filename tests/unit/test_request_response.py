from uuid import UUID, uuid4
import typing

import pydantic
import pytest

import cqrs
from cqrs.events import Event, EventEmitter, EventMap
from cqrs.requests.map import RequestMap
from cqrs.requests.request import Request
from cqrs.requests.request_handler import RequestHandler
from cqrs.response import Response


class CloseMeetingRoomCommand(Request):
    meeting_room_id: UUID = pydantic.Field()


class CloseMeetingRoomCommandHandler(RequestHandler[CloseMeetingRoomCommand, None]):
    def __init__(self) -> None:
        self.called = False
        self._events: list[Event] = []

    @property
    def events(self) -> list:
        return self._events

    async def handle(self, request: CloseMeetingRoomCommand) -> None:
        self.called = True


class ReadMeetingDetailsQuery(Request):
    meeting_room_id: UUID = pydantic.Field()


class ReadMeetingDetailsQueryResult(Response):
    meeting_room_id: UUID = pydantic.Field()


class ReadMeetingDetailsQueryHandler(
    RequestHandler[ReadMeetingDetailsQuery, ReadMeetingDetailsQueryResult],  # type: ignore
):
    def __init__(self) -> None:
        self.called = False
        self._events: list[Event] = []

    @property
    def events(self) -> list:
        return self._events

    async def handle(
        self,
        request: ReadMeetingDetailsQuery,
    ) -> ReadMeetingDetailsQueryResult:
        self.called = True
        return ReadMeetingDetailsQueryResult(meeting_room_id=request.meeting_room_id)


class TestContainer:
    close_meeting_room_command_handler = CloseMeetingRoomCommandHandler()
    read_meeting_details_query_handler = ReadMeetingDetailsQueryHandler()

    async def resolve(
        self,
        type_,
    ) -> typing.Union[CloseMeetingRoomCommandHandler, ReadMeetingDetailsQueryHandler]:
        if type_ is CloseMeetingRoomCommandHandler:
            return self.close_meeting_room_command_handler
        elif type_ is ReadMeetingDetailsQueryHandler:
            return self.read_meeting_details_query_handler
        raise Exception(f"Handler of type {type_} not found")


@pytest.fixture
def mediator() -> cqrs.RequestMediator:
    event_emitter = EventEmitter(
        event_map=EventMap(),
        container=TestContainer(),  # type: ignore
    )
    request_map = RequestMap()
    request_map.bind(ReadMeetingDetailsQuery, ReadMeetingDetailsQueryHandler)
    request_map.bind(CloseMeetingRoomCommand, CloseMeetingRoomCommandHandler)
    return cqrs.RequestMediator(
        request_map=request_map,
        container=TestContainer(),  # type: ignore
        event_emitter=event_emitter,
    )


async def test_sending_request_with_response(mediator: cqrs.RequestMediator) -> None:
    handler = await TestContainer().resolve(ReadMeetingDetailsQueryHandler)
    uuid = uuid4()

    assert isinstance(handler, ReadMeetingDetailsQueryHandler)
    assert not handler.called

    response: ReadMeetingDetailsQueryResult = await mediator.send(
        ReadMeetingDetailsQuery(meeting_room_id=uuid),
    )
    assert response is not None, "Expected ReadMeetingDetailsQueryResult, got None"

    assert handler.called
    assert response
    assert isinstance(response, ReadMeetingDetailsQueryResult)
    assert response.meeting_room_id == uuid


async def test_sending_request_without_response(mediator: cqrs.RequestMediator) -> None:
    handler = await TestContainer().resolve(CloseMeetingRoomCommandHandler)

    assert isinstance(handler, CloseMeetingRoomCommandHandler)
    assert not handler.called

    await mediator.send(CloseMeetingRoomCommand(meeting_room_id=uuid4()))

    assert handler.called
