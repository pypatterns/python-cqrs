"""
Example: Protobuf produce + consume with an isolated outbox map

Shows opt-in Protobuf serialization for outbox and EventEmitter without
changing the JSON default. Uses an in-memory broker so it runs without Kafka.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

   pip install -e ".[examples]"
   python examples/outbox/protobuf_outbox.py

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Isolated ``OutboxedEventMap()`` so the process-global registry is not mixed
2. ``ProtobufEventSerializer`` registered per event name
3. ``Message.payload`` stays a dict; ``payload_bytes`` carries proto wire bytes
4. Headers ``event_name`` / ``message_id`` only because the codec sets content_type
5. Consume-side ``ProtobufDeserializer`` round-trip via ``from_proto``

Schema: examples/proto/user_joined.proto (generated user_joined_pb2.py)
"""

from __future__ import annotations

import asyncio
import importlib.util
import uuid
from datetime import datetime
from pathlib import Path

import pydantic

import cqrs
from cqrs.deserializers import DeserializeProtobufError, ProtobufDeserializer
from cqrs.events import EventEmitter, EventMap
from cqrs.message_brokers.protocol import Message, MessageBroker
from cqrs.outbox import mock
from cqrs.serializers import ProtobufEventSerializer

_PB2_PATH = Path(__file__).resolve().parents[1] / "proto" / "user_joined_pb2.py"


def load_user_joined_pb2():
    spec = importlib.util.spec_from_file_location("user_joined_pb2", _PB2_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {_PB2_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


user_joined_pb2 = load_user_joined_pb2()


class UserJoinedPayload(pydantic.BaseModel, frozen=True):
    user_id: str
    meeting_id: str


class UserJoinedNotificationEvent(cqrs.NotificationEvent[UserJoinedPayload], frozen=True):
    event_name: str = "user_joined"

    def proto(self):
        msg = user_joined_pb2.UserJoinedNotification()
        msg.event_id = str(self.event_id)
        msg.event_timestamp = self.event_timestamp.isoformat()
        msg.event_name = self.event_name
        msg.payload.user_id = self.payload.user_id
        msg.payload.meeting_id = self.payload.meeting_id
        return msg

    @classmethod
    def from_proto(cls, proto):
        return cls(
            event_id=uuid.UUID(proto.event_id),
            event_timestamp=datetime.fromisoformat(proto.event_timestamp),
            event_name=proto.event_name,
            topic="user_notification_events",
            payload=UserJoinedPayload(
                user_id=proto.payload.user_id,
                meeting_id=proto.payload.meeting_id,
            ),
        )


class CapturingBroker(MessageBroker):
    def __init__(self) -> None:
        self.published: list[Message] = []

    async def send_message(self, message: Message) -> None:
        self.published.append(message)


class EmptyContainer:
    async def resolve(self, type_):
        return type_()


async def main() -> None:
    proto_codec = ProtobufEventSerializer(
        {UserJoinedNotificationEvent: user_joined_pb2.UserJoinedNotification},
    )
    events = cqrs.OutboxedEventMap()
    events.register(
        "user_joined",
        UserJoinedNotificationEvent,
        serializer=proto_codec,
    )

    event = UserJoinedNotificationEvent(
        event_name="user_joined",
        topic="user_notification_events",
        payload=UserJoinedPayload(user_id="1", meeting_id="42"),
    )

    storage: dict = {}
    repository = mock.MockOutboxedEventRepository(
        session_factory=lambda: storage,
        serializer=proto_codec,
        event_map=events,
    )
    repository.add(event)

    broker = CapturingBroker()
    producer = cqrs.EventProducer(broker, repository)
    for outboxed in await repository.get_many():
        await producer.send_message(outboxed)

    produced = broker.published[0]
    print("produced message_name:", produced.message_name)
    print("payload is dict:", isinstance(produced.payload, dict))
    print("content_type:", produced.content_type)
    print("headers:", produced.headers)

    deserializer = ProtobufDeserializer(
        UserJoinedNotificationEvent,
        user_joined_pb2.UserJoinedNotification,
    )
    consumed = deserializer(produced.payload_bytes)
    if isinstance(consumed, DeserializeProtobufError) or consumed is None:
        raise SystemExit(f"consume failed: {consumed}")
    print("consumed user_id:", consumed.payload.user_id)

    emitter_broker = CapturingBroker()
    emitter = EventEmitter(
        event_map=EventMap(),
        container=EmptyContainer(),  # type: ignore[arg-type]
        message_broker=emitter_broker,
        serializer=events.as_serializer(default=proto_codec),
    )
    await emitter.emit(event)
    emitted = emitter_broker.published[0]
    print("emitter message_name (class, unchanged):", emitted.message_name)
    print("emitter event_name header:", (emitted.headers or {}).get("event_name"))


if __name__ == "__main__":
    asyncio.run(main())
