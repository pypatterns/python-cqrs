"""
Example: Event Producing with Message Broker

This example demonstrates how to produce and publish events to a message broker
using NotificationEvent. The system shows how to publish events to message brokers
for asynchronous processing in event-driven architectures.

Use case: Decoupling event producers from consumers. Events are published to message
brokers (like Kafka, RabbitMQ) where they can be consumed by multiple subscribers
asynchronously. This enables scalable, loosely-coupled microservices.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/events/event_producing.py

The example will:
- Execute JoinMeetingCommand commands
- Emit multiple NotificationEvent instances with different topics
- Publish events to DevnullMessageBroker (in-memory for testing)
- Verify that events are sent to the message broker

Note: This example uses DevnullMessageBroker for testing. In production, you would
use a real message broker like Kafka or RabbitMQ.

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. NotificationEvent Creation:
   - Create NotificationEvent instances with typed payloads (Pydantic models)
   - Specify event names and topics for routing
   - Events can have different payload types for different purposes

2. Multiple Events from Single Command:
   - A single command can emit multiple events
   - Events can be published to different topics
   - Each event type serves a different purpose (ECST events, notifications)

3. Message Broker Integration:
   - Configure message broker in bootstrap.bootstrap()
   - Events are automatically published to the broker after command execution
   - Use DevnullMessageBroker for testing without external dependencies

4. Event Publishing:
   - Events are serialized and sent to message broker
   - Broker routes events to appropriate topics
   - Consumers can subscribe to topics to receive events

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


async def main():
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=command_mapper,
        message_broker=devnull.DevnullMessageBroker(),
    )
    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="2"))
    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="3"))

    assert len(devnull.MESSAGE_BUS) == 6  # type: ignore


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())
