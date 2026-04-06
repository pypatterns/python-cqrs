import logging
import typing
import sys

import orjson

from cqrs.deserializers.exceptions import DeserializeJsonError

if sys.version_info >= (3, 11):
    from typing import Self  # novm
else:
    from typing_extensions import Self

logger = logging.getLogger("cqrs")


class Deserializable(typing.Protocol):
    """
    Protocol for objects that can be deserialized from a dictionary.

    Objects implementing this protocol must have a classmethod `from_dict`
    that creates an instance from keyword arguments.
    """

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        """
        Create an instance from keyword arguments.

        Args:
            **kwargs: Keyword arguments matching the object fields.

        Returns:
            A new instance of the class.
        """
        ...


_T = typing.TypeVar("_T", bound=Deserializable)


class JsonDeserializer(typing.Generic[_T]):
    """
    Deserializer for JSON messages.

    Converts JSON strings or bytes into Python objects using the `from_dict`
    classmethod of the target model.

    Example::

        deserializer = JsonDeserializer(MyEvent)
        result = deserializer('{"field": "value"}')
        if isinstance(result, DeserializeJsonError):
            # Handle error
        else:
            # Use result
    """

    def __init__(self, model: typing.Type[typing.Any]):
        """
        Initialize JSON deserializer.

        Args:
            model: Class that implements Deserializable protocol (has a from_dict classmethod).
                   Can be a regular type or a parameterized generic type
                   (e.g., NotificationEvent[PayloadType]).

        Note:
            The model type must implement the Deserializable protocol (have a from_dict
            classmethod). This is verified at runtime. For proper type inference,
            specify the generic parameter: JsonDeserializer[ConcreteType](model=...)
        """
        # Runtime check: verify that model implements Deserializable protocol
        if not hasattr(model, "from_dict") or not callable(
            getattr(model, "from_dict", None),
        ):
            raise TypeError(
                f"Model {model} does not implement Deserializable protocol: " "missing 'from_dict' classmethod",
            )
        # Store model - type is preserved through generic parameter _T for return type
        self._model: typing.Type[typing.Any] = model

    def __call__(
        self,
        data: typing.Union[str, typing.Optional[bytes]],
    ) -> typing.Union[typing.Optional[_T], DeserializeJsonError]:
        """
        Deserialize JSON data into model instance.

        Args:
            data: JSON string, bytes, or None

        Returns:
            Instance of the model, None if data is None, or DeserializeJsonError on failure.
        """
        if data is None:
            return None
        try:
            json_dict = orjson.loads(data)
            # Safe cast: model is Type[_T] where _T bound=Deserializable,
            # so from_dict is guaranteed to return _T
            result = self._model.from_dict(**json_dict)
            return typing.cast(_T, result)
        except Exception as e:
            logger.error(
                f"Error while deserializing json message: {e}",
            )
            return DeserializeJsonError(
                error_message=str(e),
                error_type=type(e),
                message_data=data,
            )
