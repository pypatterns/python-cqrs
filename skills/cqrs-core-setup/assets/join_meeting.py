"""Command and query shape for one meeting.

Full demo: examples/events/request_handler.py.
"""

import di
from di import dependent

import cqrs
from cqrs.bootstrap import requests as bootstrap


class MeetingReader:
    def __init__(self, rooms: dict[str, list[str]]) -> None:
        self._rooms = rooms

    async def users(self, meeting_id: str) -> list[str]:
        return list(self._rooms.get(meeting_id, []))


class MeetingWriter:
    def __init__(self, rooms: dict[str, list[str]]) -> None:
        self._rooms = rooms

    async def join(self, user_id: str, meeting_id: str) -> None:
        self._rooms.setdefault(meeting_id, []).append(user_id)


class ReadMeetingQueryResult(cqrs.Response):
    users: list[str]


class ReadMeetingQuery(cqrs.Request[ReadMeetingQueryResult]):
    meeting_id: str


class ReadMeetingQueryHandler(cqrs.RequestHandler[ReadMeetingQuery]):
    def __init__(self, meetings: MeetingReader) -> None:
        self._meetings = meetings

    async def handle(self, request: ReadMeetingQuery):
        return ReadMeetingQueryResult(users=await self._meetings.users(request.meeting_id))


class JoinMeetingCommand(cqrs.Request[None]):
    user_id: str
    meeting_id: str


class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand]):
    def __init__(self, meetings: MeetingWriter) -> None:
        self._meetings = meetings

    async def handle(self, request: JoinMeetingCommand):
        await self._meetings.join(request.user_id, request.meeting_id)


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(JoinMeetingCommand, JoinMeetingCommandHandler)


def queries_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ReadMeetingQuery, ReadMeetingQueryHandler)


def build_mediator(rooms: dict[str, list[str]]) -> cqrs.RequestMediator:
    reader = MeetingReader(rooms)
    writer = MeetingWriter(rooms)
    container = di.Container()
    container.bind(di.bind_by_type(dependent.Dependent(lambda: reader, scope="request"), MeetingReader))
    container.bind(di.bind_by_type(dependent.Dependent(lambda: writer, scope="request"), MeetingWriter))
    return bootstrap.bootstrap(
        di_container=container,
        commands_mapper=commands_mapper,
        queries_mapper=queries_mapper,
    )
