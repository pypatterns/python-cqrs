import abc
import base64
import dataclasses
import typing
import uuid
from dataclass_wizard import asdict


@dataclasses.dataclass
class Message:
    """
    Internal message structure for message broker communication.

    Args:
        message_name: Name of the message type
        message_id: Unique identifier for the message (auto-generated if not provided)
        topic: Message broker topic where the message should be sent
        payload: Structured message payload (typically ``event.to_dict()``)
        payload_bytes: Optional codec wire-bytes for the transport
        content_type: Optional MIME type (set only by typed codecs such as Protobuf)
        headers: Optional broker headers (set only when ``content_type`` is set)
    """

    message_name: typing.Text
    topic: typing.Text
    payload: typing.Any
    message_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)
    payload_bytes: bytes | None = None
    content_type: str | None = None
    headers: dict[str, str] | None = None

    def to_dict(self) -> dict[str, typing.Any]:
        """
        Convert the message instance to a dictionary representation.

        Core fields use ``dataclass_wizard.asdict`` camelCase (``messageName``,
        ``messageId``, …) for JSON-path ``GET /published`` stability. Codec
        extras (``content_type`` / ``payload_bytes`` / ``headers``) are added
        only when ``content_type`` is set, in snake_case.
        """
        # Rebuild with only the original fields so asdict matches the historical
        # JSON-path shape (camelCase messageName/messageId). Extra codec fields
        # still appear as None camelCase keys from defaults — drop them.
        data = asdict(
            Message(
                message_name=self.message_name,
                topic=self.topic,
                payload=self.payload,
                message_id=self.message_id,
            ),
        )
        data.pop("payloadBytes", None)
        data.pop("contentType", None)
        data.pop("headers", None)
        if self.content_type is not None:
            data["content_type"] = self.content_type
            if self.payload_bytes is not None:
                data["payload_bytes"] = base64.b64encode(self.payload_bytes).decode("ascii")
                data["payload_bytes_encoding"] = "base64"
            if self.headers is not None:
                data["headers"] = self.headers
        return data


class MessageBroker(abc.ABC):
    """
    The interface over a message broker.

    Used for sending messages to message brokers (currently only redis supported).
    """

    @abc.abstractmethod
    async def send_message(self, message: Message) -> None:
        raise NotImplementedError
