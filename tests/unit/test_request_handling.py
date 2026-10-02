import logging
import typing
from collections import defaultdict

import di

import cqrs
from cqrs.bootstrap import requests as bootstrap

logging.basicConfig(level=logging.DEBUG)

STORAGE = defaultdict[str, typing.List[str]](lambda: [])


class JoinMeetingCommand(cqrs.Request):
    user_id: str
    meeting_id: str


class ReadMeetingQuery(cqrs.Request):
    meeting_id: str


class ReadMeetingQueryResult(cqrs.Response):
    users: list[str]


class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    @property
    def events(self):
        return []

    async def handle(self, request: JoinMeetingCommand) -> None:
        STORAGE[request.meeting_id].append(request.user_id)
        print(f"User {request.user_id} joined meeting {request.meeting_id}")


class ReadMeetingQueryHandler(
    cqrs.RequestHandler[ReadMeetingQuery, ReadMeetingQueryResult],
):
    @property
    def events(self):
        return []

    async def handle(self, request: ReadMeetingQuery) -> ReadMeetingQueryResult:
        return ReadMeetingQueryResult(users=STORAGE[request.meeting_id])


def command_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(JoinMeetingCommand, JoinMeetingCommandHandler)


def query_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ReadMeetingQuery, ReadMeetingQueryHandler)


async def test_request_handling_positive():
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        queries_mapper=query_mapper,
        commands_mapper=command_mapper,
    )
    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="2", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="3", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="4", meeting_id="1"))

    users_in_room = await mediator.send(ReadMeetingQuery(meeting_id="1"))

    assert len(users_in_room.users) == 4
