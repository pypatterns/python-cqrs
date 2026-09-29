"""
Example: Request Handlers (Commands and Queries)

This example demonstrates the basic CQRS pattern with separate command and query handlers.
The system shows how to separate read and write operations, allowing for different
optimizations and scaling strategies for each side.

Use case: Separating read and write operations. Commands modify state (write operations),
while queries read state (read operations). This separation enables independent scaling,
optimization, and evolution of read and write sides of the system.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/events/request_handler.py

The example will:
- Execute multiple JoinMeetingCommand commands to add users to meetings
- Execute ReadMeetingQuery to retrieve users from a meeting
- Display the number of users in the meeting
- Verify that all commands and queries executed successfully

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Command Definition and Handling:
   - Define commands as Request subclasses (JoinMeetingCommand)
   - Create command handlers that modify application state
   - Commands don't return data, only success/failure

2. Query Definition and Handling:
   - Define queries as Request subclasses (ReadMeetingQuery)
   - Create query handlers that read application state
   - Queries return Response objects with requested data

3. Handler Registration:
   - Register command handlers using commands_mapper
   - Register query handlers using queries_mapper
   - Map request types to their handlers

4. Mediator Usage:
   - Use mediator.send() to execute commands and queries
   - Commands and queries are routed to their respective handlers
   - Type-safe request/response handling

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - di (dependency injection)

================================================================================
"""

import asyncio
import logging
import typing
from collections import defaultdict

import di

import cqrs
from cqrs.requests import bootstrap

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


async def main():
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
    print("There are {} users in the room".format(len(users_in_room.users)))
    assert len(users_in_room.users) == 4


if __name__ == "__main__":
    asyncio.run(main())
