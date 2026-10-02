import uuid

import orjson
import pydantic
import pytest

import cqrs
from cqrs.message_brokers.protocol import Message
from cqrs.serializers.default import (
    encode_broker_payload,
    message_wire_bytes,
    passthrough_value_serializer,
)
from cqrs.serializers.json import JsonEventSerializer
from tests.fixtures.proto_fixtures import (
    UserJoinedNotificationEvent,
    make_protobuf_codec,
    make_user_joined_event,
)


class JsonPayload(pydantic.BaseModel, frozen=True):
    foo: str


def _json_event() -> cqrs.PydanticNotificationEvent[JsonPayload]:
    return cqrs.PydanticNotificationEvent[JsonPayload](
        event_name="json_event",
        topic="json_topic",
        payload=JsonPayload(foo="bar"),
        event_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
    )


def test_json_event_serializer_round_trip():
    event = _json_event()
    codec = JsonEventSerializer()

    payload_bytes = codec.serialize(event)

    assert payload_bytes == orjson.dumps(event.to_dict())
    restored = codec.deserialize(payload_bytes, type(event))
    assert restored.event_id == event.event_id
    assert restored.payload.foo == "bar"


def test_json_event_serializer_content_type_is_none():
    assert JsonEventSerializer.content_type is None
    assert JsonEventSerializer().content_type_for(_json_event()) is None


def test_protobuf_event_serializer_round_trip():
    event = make_user_joined_event()
    codec = make_protobuf_codec()

    payload_bytes = codec.serialize(event)

    assert payload_bytes == event.proto().SerializeToString()
    restored = codec.deserialize(payload_bytes, UserJoinedNotificationEvent)
    assert restored.event_id == event.event_id
    assert restored.payload.user_id == event.payload.user_id
    assert restored.payload.meeting_id == event.payload.meeting_id


def test_protobuf_event_serializer_content_type():
    codec = make_protobuf_codec()
    assert codec.content_type == "application/x-protobuf"
    assert codec.content_type_for(make_user_joined_event()) == "application/x-protobuf"


def test_protobuf_event_serializer_missing_type_raises_on_serialize():
    event = make_user_joined_event()
    codec = cqrs.ProtobufEventSerializer({})

    with pytest.raises(TypeError, match="No protobuf message type registered"):
        codec.serialize(event)


def test_protobuf_event_serializer_missing_proto_method_raises():
    class BareEvent:
        event_name = "bare"
        event_id = uuid.uuid4()

    event = BareEvent()
    codec = cqrs.ProtobufEventSerializer({BareEvent: object})  # type: ignore[dict-item]

    with pytest.raises(TypeError, match="has no proto\\(\\) method"):
        codec.serialize(event)  # type: ignore[arg-type]


def test_protobuf_event_serializer_missing_type_raises_on_deserialize():
    event = make_user_joined_event()
    codec = cqrs.ProtobufEventSerializer({})

    with pytest.raises(TypeError, match="No protobuf message type registered"):
        codec.deserialize(event.proto().SerializeToString(), UserJoinedNotificationEvent)


def test_protobuf_event_serializer_missing_from_proto_method_raises():
    class BareEvent:
        pass

    codec = cqrs.ProtobufEventSerializer({BareEvent: object})  # type: ignore[dict-item]

    with pytest.raises(TypeError, match="has no from_proto\\(\\) method"):
        codec.deserialize(b"", BareEvent)  # type: ignore[arg-type]


def test_encode_broker_payload_passthrough_bytes():
    raw = b'{"already":"encoded"}'
    assert encode_broker_payload(raw) == raw
    assert encode_broker_payload(bytearray(raw)) == raw


def test_encode_broker_payload_encodes_dict():
    payload = {"foo": "bar"}
    assert encode_broker_payload(payload) == orjson.dumps(payload)


def test_message_wire_bytes_prefers_payload_bytes():
    message = Message(
        message_name="n",
        topic="t",
        payload={"foo": "from-dict"},
        payload_bytes=b"from-bytes",
    )

    assert message_wire_bytes(message) == b"from-bytes"


def test_message_wire_bytes_encodes_payload_when_bytes_missing():
    payload = {"foo": "bar"}
    message = Message(message_name="n", topic="t", payload=payload)

    assert message_wire_bytes(message) == orjson.dumps(payload)


def test_passthrough_value_serializer_skips_bytes():
    wrapped = passthrough_value_serializer(lambda value: orjson.dumps(value))

    assert wrapped(b"already-bytes") == b"already-bytes"
    assert wrapped({"a": 1}) == orjson.dumps({"a": 1})


def test_json_bytes_match_orjson_dumps_of_payload():
    event = _json_event()
    payload_bytes = JsonEventSerializer().serialize(event)
    message = Message(
        message_name=type(event).__name__,
        topic=event.topic,
        payload=event.to_dict(),
        payload_bytes=payload_bytes,
    )

    assert message_wire_bytes(message) == orjson.dumps(event.to_dict())
    assert message_wire_bytes(message) == payload_bytes


def test_json_codec_round_trip_with_zlib_compressor():
    event = _json_event()
    codec = JsonEventSerializer()
    compressor = cqrs.ZlibCompressor()

    compressed = compressor.compress(codec.serialize(event))
    restored = codec.deserialize(compressor.decompress(compressed), type(event))

    assert restored.event_id == event.event_id
    assert restored.payload.foo == "bar"
