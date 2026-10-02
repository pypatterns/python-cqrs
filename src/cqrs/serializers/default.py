import typing

import orjson

from cqrs.models.event import INotificationEvent

if typing.TYPE_CHECKING:
    from cqrs.message_brokers.protocol import Message


Serializer = typing.Callable[[typing.Any], typing.ByteString | None]


def default_serializer(message: typing.Any) -> typing.ByteString:
    """
    Default serializer for messages.

    Works with any object that has a to_dict() method (interface-based approach).
    Falls back to model_dump() if available, otherwise serializes as-is.

    Args:
        message: Object to serialize. Should implement to_dict() method.

    Returns:
        Serialized message as bytes.
    """
    if hasattr(message, "to_dict"):
        return orjson.dumps(message.to_dict())
    elif hasattr(message, "model_dump"):
        return orjson.dumps(message.model_dump(mode="json"))
    else:
        return orjson.dumps(message)


def encode_broker_payload(payload: typing.Any) -> bytes:
    """
    Encode a broker payload to bytes without double-encoding wire bytes.

    ``bytes`` / ``bytearray`` / ``memoryview`` are returned as ``bytes``.
    Anything else is encoded with :func:`default_serializer`.
    """
    if isinstance(payload, (bytes, bytearray)):
        return bytes(payload)
    if isinstance(payload, memoryview):
        return payload.tobytes()
    return bytes(default_serializer(payload))


def message_wire_bytes(message: "Message") -> bytes:
    """Prefer ``message.payload_bytes`` when set; otherwise encode ``payload``."""
    if message.payload_bytes is not None:
        return encode_broker_payload(message.payload_bytes)
    return encode_broker_payload(message.payload)


def passthrough_value_serializer(
    value_serializer: Serializer | None = None,
) -> Serializer:
    """
    Wrap a Kafka ``value_serializer`` so ready-made wire bytes are not JSON-encoded again.
    """
    inner = value_serializer or default_serializer

    def _serialize(message: typing.Any) -> typing.ByteString | None:
        if isinstance(message, (bytes, bytearray, memoryview)):
            return bytes(message)
        return inner(message)

    return _serialize


def headers_for_content_type(
    content_type: str | None,
    event: INotificationEvent,
) -> tuple[str | None, dict[str, str] | None]:
    """
    Return ``content_type`` and broker headers when a typed codec set a MIME type.

    JSON codecs leave both as ``None`` so the Kafka/AMQP frame stays unchanged.
    """
    if content_type is None:
        return None, None
    return content_type, {
        "event_name": event.event_name,
        "message_id": str(event.event_id),
    }
