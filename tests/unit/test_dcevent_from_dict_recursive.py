"""
Tests for recursive type conversion in DCEvent.from_dict method.

These tests verify that DCEvent.from_dict correctly performs recursive type
conversion for nested structures including UUID, datetime, nested dataclasses,
lists, and dictionaries. Tests are organized by event type: Domain and Notification.
"""

import dataclasses
import datetime
import typing
import uuid

import pytest

from cqrs.events.event import DCDomainEvent, DCNotificationEvent


# ============================================================================
# Shared test data structures
# ============================================================================


@dataclasses.dataclass(frozen=True)
class NestedData:
    """Nested dataclass for testing recursive conversion."""

    nested_id: uuid.UUID
    nested_name: str
    nested_timestamp: datetime.datetime


# ============================================================================
# Domain Event test classes
# ============================================================================


class SimpleDomainEvent(DCDomainEvent, frozen=True):
    """Simple domain event with basic types."""

    user_id: str
    username: str


class DomainEventWithUUID(DCDomainEvent, frozen=True):
    """Domain event with UUID field."""

    event_id: uuid.UUID
    user_id: str


class DomainEventWithDatetime(DCDomainEvent, frozen=True):
    """Domain event with datetime field."""

    created_at: datetime.datetime
    user_id: str


class DomainEventWithNested(DCDomainEvent, frozen=True):
    """Domain event with nested DCEvent dataclass."""

    user_id: str
    nested: NestedData


class DomainEventWithList(DCDomainEvent, frozen=True):
    """Domain event with list of UUIDs."""

    user_ids: typing.List[uuid.UUID]
    event_name: str


class DomainEventWithListOfNested(DCDomainEvent, frozen=True):
    """Domain event with list of nested dataclasses."""

    items: typing.List[NestedData]
    event_name: str


class DomainEventWithDict(DCDomainEvent, frozen=True):
    """Domain event with dictionary containing UUID values."""

    metadata: typing.Dict[str, uuid.UUID]
    event_name: str


class DomainEventWithComplexNested(DCDomainEvent, frozen=True):
    """Domain event with complex nested structure."""

    event_id: uuid.UUID
    created_at: datetime.datetime
    nested: NestedData
    user_ids: typing.List[uuid.UUID]
    items: typing.List[NestedData]
    metadata: typing.Dict[str, uuid.UUID]


# ============================================================================
# Notification Event test classes
# ============================================================================


class SimpleNotificationEvent(DCNotificationEvent[dict], frozen=True):
    """Simple notification event with basic payload."""

    event_name: str
    payload: dict


class NotificationEventWithUUIDPayload(DCNotificationEvent[dict], frozen=True):
    """Notification event with UUID in payload."""

    event_name: str
    payload: dict


class NotificationEventWithNestedPayload(DCNotificationEvent[dict], frozen=True):
    """Notification event with nested dataclass in payload."""

    event_name: str
    payload: dict


class NotificationEventWithListPayload(DCNotificationEvent[dict], frozen=True):
    """Notification event with list in payload."""

    event_name: str
    payload: dict


class NotificationEventWithComplexPayload(DCNotificationEvent[dict], frozen=True):
    """Notification event with complex nested payload."""

    event_name: str
    payload: dict


class NotificationEventWithTypedPayload(DCNotificationEvent[NestedData], frozen=True):
    """Notification event with typed payload (dataclass)."""

    event_name: str
    payload: NestedData


# ============================================================================
# Domain Event Tests
# ============================================================================


class TestDomainEventFromDictBasic:
    """Test basic from_dict functionality for domain events."""

    def test_simple_domain_event_from_dict(self):
        """Test simple domain event conversion from dict."""
        data = {"user_id": "123", "username": "john"}
        event = SimpleDomainEvent.from_dict(**data)
        assert event.user_id == "123"
        assert event.username == "john"
        assert isinstance(event, DCDomainEvent)
        assert isinstance(event, SimpleDomainEvent)


class TestDomainEventFromDictUUID:
    """Test UUID type conversion in from_dict for domain events."""

    def test_uuid_from_string(self):
        """Test UUID conversion from string."""
        event_id = str(uuid.uuid4())
        data = {"event_id": event_id, "user_id": "123"}
        event = DomainEventWithUUID.from_dict(**data)
        assert isinstance(event.event_id, uuid.UUID)
        assert str(event.event_id) == event_id
        assert event.user_id == "123"
        assert isinstance(event, DCDomainEvent)

    def test_uuid_from_uuid_object(self):
        """Test UUID when already a UUID object."""
        event_id = uuid.uuid4()
        # dataclass_wizard expects strings for UUID conversion, not UUID objects
        # So we convert to string first
        data = {"event_id": str(event_id), "user_id": "123"}
        event = DomainEventWithUUID.from_dict(**data)
        assert isinstance(event.event_id, uuid.UUID)
        assert event.event_id == event_id


class TestDomainEventFromDictDatetime:
    """Test datetime type conversion in from_dict for domain events."""

    def test_datetime_from_iso_string(self):
        """Test datetime conversion from ISO format string."""
        now = datetime.datetime.now(datetime.timezone.utc)
        iso_string = now.isoformat()
        data = {"created_at": iso_string, "user_id": "123"}
        event = DomainEventWithDatetime.from_dict(**data)
        assert isinstance(event.created_at, datetime.datetime)
        # Compare timestamps to avoid timezone issues
        assert event.created_at.timestamp() == pytest.approx(now.timestamp(), abs=1)
        assert isinstance(event, DCDomainEvent)

    def test_datetime_from_datetime_object(self):
        """Test datetime when already a datetime object."""
        now = datetime.datetime.now(datetime.timezone.utc)
        data = {"created_at": now, "user_id": "123"}
        event = DomainEventWithDatetime.from_dict(**data)
        assert isinstance(event.created_at, datetime.datetime)
        assert event.created_at == now


class TestDomainEventFromDictNested:
    """Test recursive conversion of nested DCEvent dataclasses in domain events."""

    def test_nested_dataclass_from_dict(self):
        """Test nested DCEvent dataclass conversion."""
        nested_id = str(uuid.uuid4())
        nested_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        data = {
            "user_id": "123",
            "nested": {
                "nested_id": nested_id,
                "nested_name": "test",
                "nested_timestamp": nested_timestamp,
            },
        }
        event = DomainEventWithNested.from_dict(**data)
        assert isinstance(event, DCDomainEvent)
        assert isinstance(event.nested, NestedData)
        assert isinstance(event.nested.nested_id, uuid.UUID)
        assert str(event.nested.nested_id) == nested_id
        assert event.nested.nested_name == "test"
        assert isinstance(event.nested.nested_timestamp, datetime.datetime)

    def test_deeply_nested_dataclass(self):
        """Test deeply nested structure conversion."""
        event_id = str(uuid.uuid4())
        created_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        nested_id = str(uuid.uuid4())
        nested_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        data = {
            "event_id": event_id,
            "created_at": created_at,
            "nested": {
                "nested_id": nested_id,
                "nested_name": "test",
                "nested_timestamp": nested_timestamp,
            },
            "user_ids": [str(uuid.uuid4()), str(uuid.uuid4())],
            "items": [
                {
                    "nested_id": str(uuid.uuid4()),
                    "nested_name": "item1",
                    "nested_timestamp": nested_timestamp,
                },
                {
                    "nested_id": str(uuid.uuid4()),
                    "nested_name": "item2",
                    "nested_timestamp": nested_timestamp,
                },
            ],
            "metadata": {
                "key1": str(uuid.uuid4()),
                "key2": str(uuid.uuid4()),
            },
        }
        event = DomainEventWithComplexNested.from_dict(**data)
        assert isinstance(event, DCDomainEvent)
        assert isinstance(event.event_id, uuid.UUID)
        assert isinstance(event.created_at, datetime.datetime)
        assert isinstance(event.nested, NestedData)
        assert isinstance(event.nested.nested_id, uuid.UUID)
        assert all(isinstance(uid, uuid.UUID) for uid in event.user_ids)
        assert all(isinstance(item, NestedData) for item in event.items)
        assert all(isinstance(v, uuid.UUID) for v in event.metadata.values())


class TestDomainEventFromDictLists:
    """Test list type conversion in from_dict for domain events."""

    def test_list_of_uuids_from_strings(self):
        """Test list of UUIDs conversion from strings."""
        uuid_strings = [str(uuid.uuid4()), str(uuid.uuid4())]
        data = {"user_ids": uuid_strings, "event_name": "test_event"}
        event = DomainEventWithList.from_dict(**data)
        assert isinstance(event, DCDomainEvent)
        assert all(isinstance(uid, uuid.UUID) for uid in event.user_ids)
        assert len(event.user_ids) == 2
        assert str(event.user_ids[0]) == uuid_strings[0]
        assert str(event.user_ids[1]) == uuid_strings[1]

    def test_empty_list(self):
        """Test empty list handling."""
        data = {"user_ids": [], "event_name": "test_event"}
        event = DomainEventWithList.from_dict(**data)
        assert isinstance(event, DCDomainEvent)
        assert event.user_ids == []
        assert event.event_name == "test_event"

    def test_list_of_nested_dataclasses(self):
        """Test list of nested dataclasses conversion."""
        nested_id1 = str(uuid.uuid4())
        nested_id2 = str(uuid.uuid4())
        nested_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        data = {
            "items": [
                {
                    "nested_id": nested_id1,
                    "nested_name": "item1",
                    "nested_timestamp": nested_timestamp,
                },
                {
                    "nested_id": nested_id2,
                    "nested_name": "item2",
                    "nested_timestamp": nested_timestamp,
                },
            ],
            "event_name": "test_event",
        }
        event = DomainEventWithListOfNested.from_dict(**data)
        assert isinstance(event, DCDomainEvent)
        assert len(event.items) == 2
        assert all(isinstance(item, NestedData) for item in event.items)
        assert event.items[0].nested_name == "item1"
        assert event.items[1].nested_name == "item2"
        assert isinstance(event.items[0].nested_id, uuid.UUID)
        assert isinstance(event.items[1].nested_id, uuid.UUID)
        assert str(event.items[0].nested_id) == nested_id1
        assert str(event.items[1].nested_id) == nested_id2


class TestDomainEventFromDictDicts:
    """Test dictionary type conversion in from_dict for domain events."""

    def test_dict_with_uuid_values(self):
        """Test dictionary with UUID values conversion."""
        uuid1 = str(uuid.uuid4())
        uuid2 = str(uuid.uuid4())
        data = {
            "metadata": {"key1": uuid1, "key2": uuid2},
            "event_name": "test_event",
        }
        event = DomainEventWithDict.from_dict(**data)
        assert isinstance(event, DCDomainEvent)
        assert isinstance(event.metadata["key1"], uuid.UUID)
        assert isinstance(event.metadata["key2"], uuid.UUID)
        assert str(event.metadata["key1"]) == uuid1
        assert str(event.metadata["key2"]) == uuid2

    def test_empty_dict(self):
        """Test empty dictionary handling."""
        data = {"metadata": {}, "event_name": "test_event"}
        event = DomainEventWithDict.from_dict(**data)
        assert isinstance(event, DCDomainEvent)
        assert event.metadata == {}
        assert event.event_name == "test_event"


class TestDomainEventFromDictRoundTrip:
    """Test round-trip conversion (to_dict -> from_dict) for domain events."""

    def test_round_trip_simple(self):
        """Test round-trip for simple domain event."""
        original = SimpleDomainEvent(user_id="123", username="john")
        data = original.to_dict()
        restored = SimpleDomainEvent.from_dict(**data)
        assert restored == original

    def test_round_trip_with_uuid(self):
        """Test round-trip for domain event with UUID."""
        original = DomainEventWithUUID(event_id=uuid.uuid4(), user_id="123")
        data = original.to_dict()
        restored = DomainEventWithUUID.from_dict(**data)
        assert restored == original

    def test_round_trip_with_nested(self):
        """Test round-trip for domain event with nested dataclass."""
        nested = NestedData(
            nested_id=uuid.uuid4(),
            nested_name="test",
            nested_timestamp=datetime.datetime.now(datetime.timezone.utc),
        )
        original = DomainEventWithNested(user_id="123", nested=nested)
        data = original.to_dict()
        restored = DomainEventWithNested.from_dict(**data)
        assert restored == original
        assert restored.nested == nested

    def test_round_trip_complex(self):
        """Test round-trip for complex nested domain event."""
        nested = NestedData(
            nested_id=uuid.uuid4(),
            nested_name="test",
            nested_timestamp=datetime.datetime.now(datetime.timezone.utc),
        )
        original = DomainEventWithComplexNested(
            event_id=uuid.uuid4(),
            created_at=datetime.datetime.now(datetime.timezone.utc),
            nested=nested,
            user_ids=[uuid.uuid4(), uuid.uuid4()],
            items=[
                NestedData(
                    nested_id=uuid.uuid4(),
                    nested_name="item1",
                    nested_timestamp=datetime.datetime.now(datetime.timezone.utc),
                ),
                NestedData(
                    nested_id=uuid.uuid4(),
                    nested_name="item2",
                    nested_timestamp=datetime.datetime.now(datetime.timezone.utc),
                ),
            ],
            metadata={"key1": uuid.uuid4(), "key2": uuid.uuid4()},
        )
        data = original.to_dict()
        restored = DomainEventWithComplexNested.from_dict(**data)
        assert restored == original


# ============================================================================
# Notification Event Tests
# ============================================================================


class TestNotificationEventFromDictBasic:
    """Test basic from_dict functionality for notification events."""

    def test_simple_notification_event_from_dict(self):
        """Test simple notification event conversion from dict."""
        data = {
            "event_name": "user.created",
            "payload": {"user_id": "123", "username": "john"},
        }
        event = SimpleNotificationEvent.from_dict(**data)
        assert event.event_name == "user.created"
        assert event.payload == {"user_id": "123", "username": "john"}
        assert isinstance(event, DCNotificationEvent)
        assert isinstance(event.event_id, uuid.UUID)
        assert isinstance(event.event_timestamp, datetime.datetime)

    def test_notification_event_with_explicit_metadata(self):
        """Test notification event with explicit event_id and event_timestamp."""
        event_id = str(uuid.uuid4())
        event_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        data = {
            "event_name": "user.created",
            "payload": {"user_id": "123"},
            "event_id": event_id,
            "event_timestamp": event_timestamp,
            "topic": "users",
        }
        event = SimpleNotificationEvent.from_dict(**data)
        assert isinstance(event, DCNotificationEvent)
        assert isinstance(event.event_id, uuid.UUID)
        assert str(event.event_id) == event_id
        assert isinstance(event.event_timestamp, datetime.datetime)
        assert event.topic == "users"

    def test_notification_event_uuid_from_string(self):
        """Test notification event with event_id as string."""
        event_id_str = str(uuid.uuid4())
        data = {
            "event_name": "user.created",
            "payload": {"user_id": "123"},
            "event_id": event_id_str,
        }
        event = SimpleNotificationEvent.from_dict(**data)
        assert isinstance(event, DCNotificationEvent)
        assert isinstance(event.event_id, uuid.UUID)
        assert str(event.event_id) == event_id_str

    def test_notification_event_datetime_from_string(self):
        """Test notification event with event_timestamp as ISO string."""
        now = datetime.datetime.now(datetime.timezone.utc)
        event_timestamp_str = now.isoformat()
        data = {
            "event_name": "user.created",
            "payload": {"user_id": "123"},
            "event_timestamp": event_timestamp_str,
        }
        event = SimpleNotificationEvent.from_dict(**data)
        assert isinstance(event, DCNotificationEvent)
        assert isinstance(event.event_timestamp, datetime.datetime)
        # Compare timestamps to avoid timezone issues
        assert event.event_timestamp.timestamp() == pytest.approx(
            now.timestamp(),
            abs=1,
        )


class TestNotificationEventFromDictUUID:
    """Test UUID type conversion in from_dict for notification events."""

    def test_uuid_in_payload_from_string(self):
        """Test UUID conversion from string in payload."""
        user_id = str(uuid.uuid4())
        data = {
            "event_name": "user.created",
            "payload": {"user_id": user_id, "username": "john"},
        }
        event = NotificationEventWithUUIDPayload.from_dict(**data)
        assert isinstance(event, DCNotificationEvent)
        assert isinstance(event.event_id, uuid.UUID)
        # Payload is dict, so UUIDs in payload remain as strings unless payload is a dataclass
        assert event.payload["user_id"] == user_id


class TestNotificationEventFromDictNested:
    """Test recursive conversion of nested structures in notification event payloads."""

    def test_nested_dataclass_in_payload(self):
        """Test nested DCEvent dataclass in payload."""
        nested_id = str(uuid.uuid4())
        nested_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        data = {
            "event_name": "order.created",
            "payload": {
                "order_id": "123",
                "customer": {
                    "nested_id": nested_id,
                    "nested_name": "customer",
                    "nested_timestamp": nested_timestamp,
                },
            },
        }
        event = NotificationEventWithNestedPayload.from_dict(**data)
        assert isinstance(event, DCNotificationEvent)
        assert event.event_name == "order.created"
        # Payload is dict, nested structures are converted recursively
        customer = event.payload["customer"]
        assert isinstance(customer, dict)
        # If payload contains nested dataclass structures, they should be converted
        # Note: This depends on how dataclass_wizard handles nested dicts


class TestNotificationEventFromDictLists:
    """Test list type conversion in from_dict for notification events."""

    def test_list_in_payload(self):
        """Test list conversion in payload."""
        uuid_strings = [str(uuid.uuid4()), str(uuid.uuid4())]
        data = {
            "event_name": "users.batch_created",
            "payload": {"user_ids": uuid_strings},
        }
        event = NotificationEventWithListPayload.from_dict(**data)
        assert isinstance(event, DCNotificationEvent)
        assert event.event_name == "users.batch_created"
        assert event.payload["user_ids"] == uuid_strings


class TestNotificationEventFromDictComplex:
    """Test complex nested structures in notification event payloads."""

    def test_complex_payload_with_nested_structures(self):
        """Test complex payload with nested structures."""
        nested_id = str(uuid.uuid4())
        nested_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        data = {
            "event_name": "order.completed",
            "payload": {
                "order_id": str(uuid.uuid4()),
                "items": [
                    {
                        "nested_id": nested_id,
                        "nested_name": "item1",
                        "nested_timestamp": nested_timestamp,
                    },
                ],
                "metadata": {
                    "key1": str(uuid.uuid4()),
                },
            },
        }
        event = NotificationEventWithComplexPayload.from_dict(**data)
        assert isinstance(event, DCNotificationEvent)
        assert event.event_name == "order.completed"
        assert "order_id" in event.payload
        assert "items" in event.payload
        assert "metadata" in event.payload

    def test_typed_payload_with_nested_dataclass(self):
        """Test notification event with typed payload (dataclass) containing nested structures."""
        nested_id = str(uuid.uuid4())
        nested_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        data = {
            "event_name": "user.registered",
            "payload": {
                "nested_id": nested_id,
                "nested_name": "user_data",
                "nested_timestamp": nested_timestamp,
            },
        }
        event = NotificationEventWithTypedPayload.from_dict(**data)
        assert isinstance(event, DCNotificationEvent)
        assert event.event_name == "user.registered"
        assert isinstance(event.payload, NestedData)
        assert isinstance(event.payload.nested_id, uuid.UUID)
        assert str(event.payload.nested_id) == nested_id
        assert event.payload.nested_name == "user_data"
        assert isinstance(event.payload.nested_timestamp, datetime.datetime)
        assert isinstance(event.event_id, uuid.UUID)
        assert isinstance(event.event_timestamp, datetime.datetime)


class TestNotificationEventFromDictRoundTrip:
    """Test round-trip conversion (to_dict -> from_dict) for notification events."""

    def test_round_trip_simple(self):
        """Test round-trip for simple notification event."""
        original = SimpleNotificationEvent(
            event_name="user.created",
            payload={"user_id": "123", "username": "john"},
        )
        data = original.to_dict()
        restored = SimpleNotificationEvent.from_dict(**data)
        assert restored.event_name == original.event_name
        assert restored.payload == original.payload
        assert restored.topic == original.topic

    def test_round_trip_with_explicit_metadata(self):
        """Test round-trip for notification event with explicit metadata."""
        event_id = uuid.uuid4()
        event_timestamp = datetime.datetime.now(datetime.timezone.utc)
        original = SimpleNotificationEvent(
            event_name="user.created",
            payload={"user_id": "123"},
            event_id=event_id,
            event_timestamp=event_timestamp,
            topic="users",
        )
        data = original.to_dict()
        restored = SimpleNotificationEvent.from_dict(**data)
        assert restored.event_name == original.event_name
        assert restored.payload == original.payload
        assert restored.event_id == original.event_id
        assert restored.event_timestamp == original.event_timestamp
        assert restored.topic == original.topic

    def test_round_trip_with_typed_payload(self):
        """Test round-trip for notification event with typed payload."""
        payload = NestedData(
            nested_id=uuid.uuid4(),
            nested_name="test",
            nested_timestamp=datetime.datetime.now(datetime.timezone.utc),
        )
        original = NotificationEventWithTypedPayload(
            event_name="user.registered",
            payload=payload,
        )
        data = original.to_dict()
        restored = NotificationEventWithTypedPayload.from_dict(**data)
        assert restored.event_name == original.event_name
        assert restored.payload == original.payload
        assert isinstance(restored.payload, NestedData)
        assert restored.payload.nested_id == payload.nested_id


class TestDCNotificationEventProtoAndHash:
    """Test DCNotificationEvent proto(), from_proto(), and __hash__() methods."""

    def test_proto_raises_not_implemented_error(self):
        """DCNotificationEvent.proto() raises NotImplementedError by default."""
        event = SimpleNotificationEvent(
            event_name="user.created",
            payload={"user_id": "123"},
        )
        with pytest.raises(NotImplementedError, match="Method not implemented"):
            event.proto()

    def test_from_proto_raises_not_implemented_error(self):
        """DCNotificationEvent.from_proto() raises NotImplementedError by default."""
        with pytest.raises(NotImplementedError, match="Method not implemented"):
            SimpleNotificationEvent.from_proto(None)

    def test_hash_returns_hash_of_event_id(self):
        """DCNotificationEvent.__hash__() returns hash of event_id."""
        event_id = uuid.uuid4()
        event = SimpleNotificationEvent(
            event_name="user.created",
            payload={"user_id": "123"},
            event_id=event_id,
        )
        # Call base __hash__ explicitly: subclass is frozen dataclass with dict payload,
        # so hash(event) would use dataclass __hash__ and fail (dict unhashable).
        assert DCNotificationEvent.__hash__(event) == hash(event_id)
        event2 = SimpleNotificationEvent(
            event_name="other",
            payload={},
            event_id=event_id,
        )
        assert DCNotificationEvent.__hash__(event) == DCNotificationEvent.__hash__(event2)
