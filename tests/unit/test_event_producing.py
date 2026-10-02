import logging

import di
import pydantic

import cqrs
from cqrs.message_brokers import devnull
from cqrs.bootstrap import requests as bootstrap

logging.basicConfig(level=logging.DEBUG)


class UserJoinedNotificationPayload(pydantic.BaseModel, frozen=True):
    user_id: str
    meeting_id: str


class UserJoinedECSTPayload(pydantic.BaseModel, frozen=True):
    user_id: str
    meeting_id: str


class JoinMeetingCommand(cqrs.Request):
    user_id: str
    meeting_id: str


class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    def __init__(self):
        self._events = []

    @property
    def events(self):
        return self._events

    async def handle(self, request: JoinMeetingCommand) -> None:
        print(f"User {request.user_id} joined meeting {request.meeting_id}")
        self._events.append(
            cqrs.NotificationEvent[UserJoinedECSTPayload](
                event_name="user_joined_ecst",
                topic="user_ecst_events",
                payload=UserJoinedECSTPayload(
                    user_id=request.user_id,
                    meeting_id=request.meeting_id,
                ),
            ),
        )
        self._events.append(
            cqrs.NotificationEvent[UserJoinedNotificationPayload](
                event_name="user_joined_notification",
                topic="user_notification_events",
                payload=UserJoinedNotificationPayload(
                    user_id=request.user_id,
                    meeting_id=request.meeting_id,
                ),
            ),
        )


def command_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(JoinMeetingCommand, JoinMeetingCommandHandler)


async def test_event_producing_positive():
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=command_mapper,
        message_broker=devnull.DevnullMessageBroker(),
    )

    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="1"))

    assert len(devnull.MESSAGE_BUS) == 2  # type: ignore
