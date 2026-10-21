"""
Example: typed request handler (one type argument).

``Request[Result]`` publishes the response type. ``RequestHandler[ThatRequest]``
does not repeat it on ``handle``. An empty command is ``Request[None]``.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

   python examples/events/typed_request_handler.py

================================================================================
REQUIREMENTS
================================================================================

   - cqrs (this package)
   - di (dependency injection)

No Docker and no pydantic extra.
================================================================================
"""

import asyncio
from collections import defaultdict

import di

import cqrs
from cqrs.bootstrap import requests as bootstrap

STORAGE: dict[str, list[str]] = defaultdict(list)


class ReadMeetingQueryResult(cqrs.Response):
    users: list[str]


class ReadMeetingQuery(cqrs.Request[ReadMeetingQueryResult]):
    meeting_id: str


class JoinMeetingCommand(cqrs.Request[None]):
    user_id: str
    meeting_id: str


class ReadMeetingQueryHandler(cqrs.RequestHandler[ReadMeetingQuery]):
    async def handle(self, request: ReadMeetingQuery):
        return ReadMeetingQueryResult(users=list(STORAGE[request.meeting_id]))


class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand]):
    async def handle(self, request: JoinMeetingCommand):
        STORAGE[request.meeting_id].append(request.user_id)
        print(request.user_id, request.meeting_id)


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(JoinMeetingCommand, JoinMeetingCommandHandler)


def queries_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ReadMeetingQuery, ReadMeetingQueryHandler)


async def main() -> None:
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=commands_mapper,
        queries_mapper=queries_mapper,
    )
    assert await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="1")) is None
    assert await mediator.send(JoinMeetingCommand(user_id="2", meeting_id="1")) is None
    users = await mediator.send(ReadMeetingQuery(meeting_id="1"))
    assert users.users == ["1", "2"]
    print("users", users.users)


if __name__ == "__main__":
    asyncio.run(main())
