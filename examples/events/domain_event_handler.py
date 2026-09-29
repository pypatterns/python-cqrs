"""
Example: Domain Event Handling

This example demonstrates the basic pattern of domain event handling in CQRS.
The system shows how command handlers emit domain events that are automatically
dispatched to their corresponding event handlers.

Use case: Separating command execution from side effects. When a command is executed,
it emits domain events that represent what happened. These events are then processed
by event handlers that perform side effects like sending notifications, updating
read models, or triggering other workflows.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/events/domain_event_handler.py

The example will:
- Execute multiple JoinMeetingCommand commands
- Emit UserJoined domain events for each command
- Process events through UserJoinedEventHandler
- Display the number of users in the meeting and events handled

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Domain Event Definition:
   - Create domain events as frozen dataclasses (UserJoined)
   - Events represent something that happened in the domain
   - Events are immutable and contain all relevant data

2. Event Emission from Command Handlers:
   - Command handlers collect events in the events property
   - Events are emitted after command execution succeeds
   - Multiple events can be emitted from a single command

3. Event Handler Registration:
   - Register event handlers using domain_events_mapper
   - Map event types to their handlers
   - Mediator automatically dispatches events to registered handlers

4. Automatic Event Dispatching:
   - Mediator collects events from command handlers
   - Events are automatically sent to their registered handlers
   - Event handlers process events asynchronously

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
HANDLED_EVENTS = set[cqrs.DomainEvent]()


class JoinMeetingCommand(cqrs.Request):
    user_id: str
    meeting_id: str


class UserJoined(cqrs.DomainEvent, frozen=True):
    user_id: str
    meeting_id: str


class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    def __init__(self):
        self._events = []

    @property
    def events(self):
        return self._events

    async def handle(self, request: JoinMeetingCommand) -> None:
        STORAGE[request.meeting_id].append(request.user_id)
        self._events.append(
            UserJoined(user_id=request.user_id, meeting_id=request.meeting_id),
        )
        print(f"User {request.user_id} joined meeting {request.meeting_id}")


class UserJoinedEventHandler(cqrs.EventHandler[UserJoined]):
    async def handle(self, event: UserJoined) -> None:
        print(f"Handle user {event.user_id} joined meeting {event.meeting_id} event")
        HANDLED_EVENTS.add(event)


def command_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(JoinMeetingCommand, JoinMeetingCommandHandler)


def events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(UserJoined, UserJoinedEventHandler)


async def main():
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=command_mapper,
        domain_events_mapper=events_mapper,
    )
    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="2", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="3", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="4", meeting_id="1"))
    print("There are {} users in the room".format(len(STORAGE["1"])))
    print("{} events was handled".format(len(HANDLED_EVENTS)))
    assert len(HANDLED_EVENTS) == 4


if __name__ == "__main__":
    asyncio.run(main())
