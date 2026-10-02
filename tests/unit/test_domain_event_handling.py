import asyncio
import typing
from collections import defaultdict

import di

import cqrs
from cqrs.bootstrap import requests as bootstrap

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


async def test_handle_domain_events_positive():
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=command_mapper,
        domain_events_mapper=events_mapper,
    )

    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="2", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="3", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="4", meeting_id="1"))

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    assert len(HANDLED_EVENTS) == 4


async def test_request_mediator_processes_events_parallel():
    HANDLED_EVENTS.clear()

    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=command_mapper,
        domain_events_mapper=events_mapper,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
    )

    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="2", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="3", meeting_id="1"))

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    assert len(HANDLED_EVENTS) == 3


async def test_request_mediator_processes_events_sequentially():
    HANDLED_EVENTS.clear()

    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=command_mapper,
        domain_events_mapper=events_mapper,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=False,
    )

    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="2", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="3", meeting_id="1"))

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    assert len(HANDLED_EVENTS) == 3
