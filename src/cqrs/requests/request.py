import abc
import dataclasses
import sys
import typing

from typing_extensions import dataclass_transform

from cqrs._dataclass_utils import ensure_dataclass, pydantic_extra_error
from cqrs.response import IResponse

if sys.version_info >= (3, 11):
    from typing import Self  # novm
else:
    from typing_extensions import Self


class IRequest(abc.ABC):
    """
    Interface for request-type objects.

    This abstract base class defines the contract that all request implementations
    must follow. Requests are input objects passed to request handlers and are used
    for defining queries or commands in the CQRS pattern.
    """

    @abc.abstractmethod
    def to_dict(self) -> dict:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, **kwargs) -> Self:
        raise NotImplementedError


# Type variables for request/response (defined here to avoid circular import with
# cqrs.types <-> cqrs.requests.request_handler). Re-exported from cqrs.types for compatibility.
ReqT = typing.TypeVar("ReqT", bound=IRequest, contravariant=True)
ResT = typing.TypeVar("ResT", bound=IResponse | None, covariant=True)


@dataclass_transform()
@dataclasses.dataclass
class DCRequest(IRequest):
    """
    Dataclass-based implementation of the request interface.

    Default for the ``Request`` alias in 5.x. Subclasses may omit ``@dataclass``.
    For Pydantic validation, install ``python-cqrs[pydantic]`` and use
    ``PydanticRequest``.
    """

    def __init_subclass__(cls, **kwargs: typing.Any) -> None:
        super().__init_subclass__(**kwargs)
        ensure_dataclass(cls, frozen=False)

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        return cls(**kwargs)

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


Request = DCRequest

try:
    from cqrs.requests.pydantic import PydanticRequest  # noqa: E402
except ImportError:  # pragma: no cover
    PydanticRequest = None  # type: ignore[misc, assignment]


def __getattr__(name: str) -> typing.Any:
    if name == "PydanticRequest":
        if PydanticRequest is not None:
            return PydanticRequest
        raise pydantic_extra_error(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ("Request", "IRequest", "DCRequest", "PydanticRequest", "ReqT", "ResT")
