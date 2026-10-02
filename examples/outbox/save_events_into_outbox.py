"""
Example: Saving Events to Outbox Pattern

This example demonstrates how to save events to an outbox repository within
command handlers, implementing the Outbox pattern for reliable event publishing.
The system shows how to ensure transactional consistency when publishing events.

Use case: Guaranteeing event persistence in the same transaction as business logic.
Events are saved to an outbox repository within the command handler transaction.
If the command succeeds, events are persisted and can be published later by a
separate process. This prevents event loss and ensures eventual consistency.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/outbox/save_events_into_outbox.py

The example will:
- Execute multiple JoinMeetingCommand commands
- Save events to outbox repository within each command handler
- Commit transactions after saving events
- Query events from outbox by topic
- Display the number of events stored in the outbox

Note: This example uses MockOutboxedEventRepository for demonstration. In production,
you would use a real database-backed outbox repository (e.g., SQLAlchemyOutboxedEventRepository).

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Outbox Event Registration:
   - Register event types in OutboxedEventMap
   - Map event names to event types for deserialization
   - Support multiple event types in the same system

2. Dependency Injection:
   - Inject OutboxedEventRepository into command handlers
   - Configure DI container to provide repository instances
   - Use request scope for repository instances

3. Event Persistence:
   - Save events to outbox repository within command handlers
   - Events are persisted in the same transaction as business logic
   - Commit transactions after successful event saving

4. Event Querying:
   - Query events from outbox by topic
   - Retrieve events for publishing by separate process
   - Support filtering and batch retrieval

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - di (dependency injection)
   - pydantic (for typed payloads)

================================================================================
"""

import asyncio
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


async def main():
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

    print("There are {} users in the room".format(len(OUTBOX_STORAGE)))
    print(f"There are {len(notification_events)} notification events in the outbox")
    print(f"There are {len(ecst_events)} ecst events in the outbox")


if __name__ == "__main__":
    asyncio.run(main())
