import functools
import typing
import uuid
from collections import defaultdict

import di
import pydantic
from di import dependent

import cqrs
from cqrs.outbox import mock
from cqrs.bootstrap import requests as bootstrap

OUTBOX_STORAGE = defaultdict[
    uuid.UUID,
    typing.List[cqrs.NotificationEvent],
](lambda: [])
mock_repository_factory = functools.partial(
    mock.MockOutboxedEventRepository,
    session_factory=functools.partial(lambda: OUTBOX_STORAGE),
)


class UserJoinedNotificationPayload(pydantic.BaseModel, frozen=True):
    user_id: str
    meeting_id: str


class UserJoinedECSTPayload(pydantic.BaseModel, frozen=True):
    user_id: str
    meeting_id: str


cqrs.OutboxedEventMap.register(
    "user_joined_notification",
    cqrs.NotificationEvent[UserJoinedNotificationPayload],
)
cqrs.OutboxedEventMap.register(
    "user_joined_ecst",
    cqrs.NotificationEvent[UserJoinedECSTPayload],
)


class JoinMeetingCommand(cqrs.Request):
    user_id: str
    meeting_id: str


class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    def __init__(self, outbox: cqrs.OutboxedEventRepository):
        self.outbox = outbox

    @property
    def events(self):
        return []

    async def handle(self, request: JoinMeetingCommand) -> None:
        print(f"User {request.user_id} joined meeting {request.meeting_id}")
        self.outbox.add(
            cqrs.NotificationEvent[UserJoinedNotificationPayload](
                event_name="user_joined_notification",
                topic="user_notification_events",
                payload=UserJoinedNotificationPayload(
                    user_id=request.user_id,
                    meeting_id=request.meeting_id,
                ),
            ),
        )
        self.outbox.add(
            cqrs.NotificationEvent[UserJoinedECSTPayload](
                event_name="user_joined_ecst",
                topic="user_ecst_events",
                payload=UserJoinedECSTPayload(
                    user_id=request.user_id,
                    meeting_id=request.meeting_id,
                ),
            ),
        )
        await self.outbox.commit()


def command_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(JoinMeetingCommand, JoinMeetingCommandHandler)


def setup_di() -> di.Container:
    """
    Initialize DI container
    """
    container = di.Container()
    bind = di.bind_by_type(
        dependent.Dependent(mock_repository_factory, scope="request"),
        cqrs.OutboxedEventRepository,
    )
    container.bind(bind)
    return container


async def test_save_events_into_outbox_positive():
    mediator = bootstrap.bootstrap(
        di_container=setup_di(),
        commands_mapper=command_mapper,
    )
    repository = mock_repository_factory()
    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="2", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="3", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="4", meeting_id="1"))

    notification_events = await repository.get_many(
        topic="user_notification_events",
    )
    ecst_events = await repository.get_many(
        topic="user_ecst_events",
    )

    assert len(OUTBOX_STORAGE) == 8
    assert len(notification_events) == 4
    assert len(ecst_events) == 4
