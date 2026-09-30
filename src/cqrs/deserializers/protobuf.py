import logging
import typing
import sys

from cqrs.deserializers.exceptions import DeserializeProtobufError

if sys.version_info >= (3, 11):
    from typing import Self  # novm
else:
    from typing_extensions import Self

logger = logging.getLogger("cqrs")


class ProtoDeserializable(typing.Protocol):
    """Objects that can be constructed from a protobuf message via ``from_proto``."""

    @classmethod
    def from_proto(cls, proto: typing.Any) -> Self: ...


_T = typing.TypeVar("_T", bound=ProtoDeserializable)


class ProtobufDeserializer(typing.Generic[_T]):
    """
    Deserializer for protobuf messages.

    Converts protobuf wire bytes into Python objects using the ``from_proto``
    classmethod of the target model. Duck-typed: this module does not import
    ``google.protobuf``.
    """

    def __init__(self, model: type[_T], proto_type: typing.Any) -> None:
        self._model = model
        self._proto_type = proto_type

    def __call__(self, data: bytes | None) -> _T | None | DeserializeProtobufError:
        if data is None:
            return None
        try:
            return self._model.from_proto(self._proto_type.FromString(data))
        except Exception as e:
            logger.error(
                f"Error while deserializing protobuf message: {e}",
            )
            return DeserializeProtobufError(
                error_message=str(e),
                error_type=type(e),
                message_data=data,
            )
