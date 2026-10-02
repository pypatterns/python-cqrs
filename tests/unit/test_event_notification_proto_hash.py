"""Tests for PydanticNotificationEvent proto(), from_proto(), and __hash__() methods."""

import uuid

import pytest

from cqrs.models.pydantic import PydanticNotificationEvent


class SimplePydanticNotificationEvent(PydanticNotificationEvent[dict], frozen=True):
    """Minimal Pydantic notification event for testing."""

    event_name: str = "test.event"
    payload: dict = {}


class TestPydanticNotificationEventProtoAndHash:
    """Test PydanticNotificationEvent proto(), from_proto(), and __hash__() methods."""

    def test_proto_raises_not_implemented_error(self):
        """PydanticNotificationEvent.proto() raises NotImplementedError by default."""
        event = SimplePydanticNotificationEvent(
            event_name="user.created",
            payload={"user_id": "123"},
        )
        with pytest.raises(NotImplementedError, match="Method not implemented"):
            event.proto()

    def test_from_proto_raises_not_implemented_error(self):
        """PydanticNotificationEvent.from_proto() raises NotImplementedError by default."""
        with pytest.raises(NotImplementedError, match="Method not implemented"):
            SimplePydanticNotificationEvent.from_proto(None)

    def test_hash_returns_hash_of_event_id(self):
        """PydanticNotificationEvent.__hash__() returns hash of event_id."""
        event_id = uuid.uuid4()
        event = SimplePydanticNotificationEvent(
            event_name="user.created",
            payload={"user_id": "123"},
            event_id=event_id,
        )
        assert PydanticNotificationEvent.__hash__(event) == hash(event_id)
        event2 = SimplePydanticNotificationEvent(
            event_name="other",
            payload={},
            event_id=event_id,
        )
        assert PydanticNotificationEvent.__hash__(event) == PydanticNotificationEvent.__hash__(event2)
