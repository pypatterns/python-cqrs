import abc
import dataclasses
import sys
import typing

from typing_extensions import dataclass_transform

from cqrs._dataclass_utils import ensure_dataclass, pydantic_extra_error, to_plain_dict

if sys.version_info >= (3, 11):
    from typing import Self  # novm
else:
    from typing_extensions import Self


class IResponse(abc.ABC):
    """
    Interface for response-type objects.

    This abstract base class defines the contract that all response implementations
    must follow. Responses are result objects returned by request handlers and are
    typically used for defining the result of queries in the CQRS pattern.

    [AI Instruction]
    Subclass ``Response`` for the body of a query or a streaming yield.
    A command with nothing to return uses ``Request[None]`` and does not need
    a response type. This is not an event and not a saga step result.
    """

    @abc.abstractmethod
    def to_dict(self) -> dict:
        """
        [AI Instruction]
        Return a plain dict of this response. Implement it only on a custom
        ``IResponse``. ``Response`` and ``PydanticResponse`` already do.
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, **kwargs) -> Self:
        """
        [AI Instruction]
        Build this response from keyword fields. Implement it only on a custom
        ``IResponse``.
        """
        raise NotImplementedError


@dataclass_transform()
@dataclasses.dataclass
class DCResponse(IResponse):
    """
    Dataclass-based implementation of the response interface.

    Default for the ``Response`` alias in 5.x. Subclasses may omit ``@dataclass``.
    For Pydantic validation, install ``python-cqrs[pydantic]`` and use
    ``PydanticResponse``.

    [AI Instruction]
    This is the default ``Response``. Subclass it for a query result or a
    streaming element. Omit ``@dataclass``. Use ``PydanticResponse`` when the
    result must be validated.
    """

    def __init_subclass__(cls, **kwargs: typing.Any) -> None:
        super().__init_subclass__(**kwargs)
        ensure_dataclass(cls, frozen=False)

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        return cls(**kwargs)

    def to_dict(self) -> dict:
        return to_plain_dict(self)


Response = DCResponse

try:
    from cqrs.models.pydantic import PydanticResponse  # noqa: E402
except ImportError:  # pragma: no cover
    PydanticResponse = None  # type: ignore[misc, assignment]


def __getattr__(name: str) -> typing.Any:
    if name == "PydanticResponse":
        if PydanticResponse is not None:
            return PydanticResponse
        raise pydantic_extra_error(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ("Response", "IResponse", "DCResponse", "PydanticResponse")
