import dataclasses
import typing


@dataclasses.dataclass(frozen=True)
class DeserializeProtobufError:
    """
    Error that occurred during protobuf deserialization.

    Args:
        error_message: Human-readable error message
        error_type: Type of the exception that occurred
        message_data: The original message data that failed to deserialize
    """

    error_message: str
    error_type: typing.Type[Exception]
    message_data: str | bytes | None


@dataclasses.dataclass(frozen=True)
class DeserializeJsonError:
    """
    Error that occurred during JSON deserialization.

    Args:
        error_message: Human-readable error message
        error_type: Type of the exception that occurred
        message_data: The original message data that failed to deserialize
    """

    error_message: str
    error_type: typing.Type[Exception]
    message_data: str | bytes | None
