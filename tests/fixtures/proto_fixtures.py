import importlib.util
import uuid
from datetime import datetime
from pathlib import Path

import pydantic
import pytest

import cqrs

_PB2_PATH = Path(__file__).resolve().parents[2] / "examples" / "proto" / "user_joined_pb2.py"


_user_joined_pb2 = None


def load_user_joined_pb2():
    pytest.importorskip("google.protobuf")
    global _user_joined_pb2
    if _user_joined_pb2 is None:
        spec = importlib.util.spec_from_file_location("user_joined_pb2", _PB2_PATH)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load protobuf fixture from {_PB2_PATH}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _user_joined_pb2 = module
    return _user_joined_pb2


class UserJoinedPayload(pydantic.BaseModel, frozen=True):
    user_id: str
    meeting_id: str


class UserJoinedNotificationEvent(cqrs.NotificationEvent[UserJoinedPayload], frozen=True):
    event_name: str = "user_joined"

    def proto(self):
        pb2 = load_user_joined_pb2()
        msg = pb2.UserJoinedNotification()
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


def make_user_joined_event(
    *,
    user_id: str = "u-1",
    meeting_id: str = "m-1",
    event_id: uuid.UUID | None = None,
) -> UserJoinedNotificationEvent:
    kwargs: dict = {
        "event_name": "user_joined",
        "topic": "user_notification_events",
        "payload": UserJoinedPayload(user_id=user_id, meeting_id=meeting_id),
    }
    if event_id is not None:
        kwargs["event_id"] = event_id
    return UserJoinedNotificationEvent(**kwargs)


def make_protobuf_codec():
    pb2 = load_user_joined_pb2()
    return cqrs.ProtobufEventSerializer(
        {UserJoinedNotificationEvent: pb2.UserJoinedNotification},
    )
