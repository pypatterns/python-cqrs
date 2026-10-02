import typing

import orjson
import pydantic

import cqrs
from cqrs.deserializers import json


class DeserializedModelPayload(pydantic.BaseModel):
    foo: typing.Text
    bar: int


# ============================================================================
# JsonDeserializer Tests
# ============================================================================


def test_json_deserializer_from_none_positive():
    deserializer = json.JsonDeserializer[cqrs.PydanticNotificationEvent](
        model=cqrs.PydanticNotificationEvent,
    )

    assert deserializer(None) is None


def test_json_deserializer_from_bytes_positive():
    deserializer = json.JsonDeserializer[cqrs.PydanticNotificationEvent](
        model=cqrs.PydanticNotificationEvent[DeserializedModelPayload],
    )

    events = cqrs.PydanticNotificationEvent[DeserializedModelPayload](
        event_name="empty_event",
        payload=DeserializedModelPayload(foo="foo", bar=1),
    )

    assert deserializer(orjson.dumps(events.model_dump(mode="json"))) == events


def test_json_deserializer_from_str_positive():
    deserializer = json.JsonDeserializer(
        model=cqrs.PydanticNotificationEvent[DeserializedModelPayload],
    )

    events = cqrs.PydanticNotificationEvent[DeserializedModelPayload](
        event_name="empty_event",
        payload=DeserializedModelPayload(foo="foo", bar=1),
    )

    assert deserializer(orjson.dumps(events.model_dump(mode="json")).decode()) == events


def test_json_deserializer_invalid_json_negative():
    deserializer = json.JsonDeserializer(
        model=cqrs.PydanticNotificationEvent[DeserializedModelPayload],
    )

    result = deserializer("not json")

    assert isinstance(result, json.DeserializeJsonError)
    assert result.error_message is not None
    assert result.error_type is not None
    assert result.message_data == "not json"


def test_json_deserializer_invalid_structure_negative():
    deserializer = json.JsonDeserializer(
        model=cqrs.PydanticNotificationEvent[DeserializedModelPayload],
    )

    # JSON with missing required field 'event_name'
    invalid_json = '{"invalid_field": "value"}'
    result = deserializer(invalid_json)

    assert isinstance(result, json.DeserializeJsonError)
    assert result.error_message is not None
    assert result.error_type is not None
    assert result.message_data == invalid_json


def test_json_deserializer_missing_required_fields_negative():
    deserializer = json.JsonDeserializer(
        model=cqrs.PydanticNotificationEvent[DeserializedModelPayload],
    )

    # JSON with payload that has wrong type for required field 'bar' (string instead of int)
    # This should cause a validation error when Pydantic tries to validate the payload
    incomplete_json = '{"event_name": "test", "payload": {"foo": "bar", "bar": "not_an_int"}}'
    result = deserializer(incomplete_json)

    assert isinstance(result, json.DeserializeJsonError)
    assert result.error_message is not None
    assert result.error_type is not None
    assert result.message_data == incomplete_json


def test_json_deserializer_empty_string_negative():
    deserializer = json.JsonDeserializer(
        model=cqrs.PydanticNotificationEvent[DeserializedModelPayload],
    )

    result = deserializer("")

    assert isinstance(result, json.DeserializeJsonError)
    assert result.error_message is not None
    assert result.error_type is not None
    assert result.message_data == ""


def test_json_deserializer_empty_json_object_negative():
    deserializer = json.JsonDeserializer(
        model=cqrs.PydanticNotificationEvent[DeserializedModelPayload],
    )

    result = deserializer("{}")

    assert isinstance(result, json.DeserializeJsonError)
    assert result.error_message is not None
    assert result.error_type is not None
    assert result.message_data == "{}"


def test_protobuf_deserializer_round_trip():
    from cqrs.deserializers.protobuf import ProtobufDeserializer
    from tests.fixtures.proto_fixtures import (
        UserJoinedNotificationEvent,
        load_user_joined_pb2,
        make_user_joined_event,
    )

    event = make_user_joined_event()
    deserializer = ProtobufDeserializer(
        UserJoinedNotificationEvent,
        load_user_joined_pb2().UserJoinedNotification,
    )

    restored = deserializer(event.proto().SerializeToString())

    assert isinstance(restored, UserJoinedNotificationEvent)
    assert restored.event_id == event.event_id
    assert restored.payload.user_id == event.payload.user_id


def test_protobuf_deserializer_from_none():
    from cqrs.deserializers.protobuf import ProtobufDeserializer
    from tests.fixtures.proto_fixtures import UserJoinedNotificationEvent, load_user_joined_pb2

    deserializer = ProtobufDeserializer(
        UserJoinedNotificationEvent,
        load_user_joined_pb2().UserJoinedNotification,
    )

    assert deserializer(None) is None


def test_protobuf_deserializer_invalid_bytes():
    from cqrs.deserializers import DeserializeProtobufError
    from cqrs.deserializers.protobuf import ProtobufDeserializer
    from tests.fixtures.proto_fixtures import UserJoinedNotificationEvent, load_user_joined_pb2

    deserializer = ProtobufDeserializer(
        UserJoinedNotificationEvent,
        load_user_joined_pb2().UserJoinedNotification,
    )

    result = deserializer(b"not-protobuf")

    assert isinstance(result, DeserializeProtobufError)
    assert result.message_data == b"not-protobuf"
