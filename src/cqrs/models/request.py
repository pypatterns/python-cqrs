import abc
import dataclasses
import sys
import typing

from typing_extensions import TypeVar, dataclass_transform

from cqrs._dataclass_utils import ensure_dataclass, pydantic_extra_error, to_plain_dict
from cqrs.models.response import IResponse

if sys.version_info >= (3, 11):
    from typing import Self  # novm
else:
    from typing_extensions import Self


# Default ``Any`` keeps an unparameterized ``Request`` typed as ``Request[Any]``
# so ``send`` still allows reading fields of the response. ``Request[None]`` is
# the explicit empty command result. Not the handler's ``ResT``: a default there
# would hide the request's own parameter.
RequestResT = TypeVar("RequestResT", covariant=True, default=typing.Any)


class IRequest(abc.ABC, typing.Generic[RequestResT]):
    """
    Interface for request-type objects.

    This abstract base class defines the contract that all request implementations
    must follow. Requests are input objects passed to request handlers and are used
    for defining queries or commands in the CQRS pattern.

    [AI Instruction]
    Subclass ``Request`` for both commands and queries. A command with no
    result is ``Request[None]``. A query that returns data is
    ``Request[ThatResult]``, and ``send`` / ``stream`` use that parameter.
    A subclass written as ``Request`` with no parameter stays ``Request[Any]``.
    There is no ``Query`` or ``Command`` base. Put fields on the subclass;
    do not pass a separate payload object into the mediator.
    """

    @abc.abstractmethod
    def to_dict(self) -> dict:
        """
        [AI Instruction]
        Return a plain dict of this request. Implement it only on a custom
        ``IRequest``. ``Request`` and ``PydanticRequest`` already do.
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, **kwargs) -> Self:
        """
        [AI Instruction]
        Build this request from keyword fields. Implement it only on a custom
        ``IRequest``. Prefer ``Request`` or ``PydanticRequest`` so this stays
        consistent with ``to_dict``.
        """
        raise NotImplementedError


# Type variables for request/response (defined here to avoid circular import with
# cqrs.types <-> cqrs.requests.request_handler). Re-exported from cqrs.types for compatibility.
ReqT = typing.TypeVar("ReqT", bound=IRequest, contravariant=True)
ResT = TypeVar("ResT", bound=IResponse | None, covariant=True, default=typing.Any)


@dataclass_transform()
@dataclasses.dataclass
class DCRequest(IRequest[RequestResT]):
    """
    Dataclass-based implementation of the request interface.

    Default for the ``Request`` alias in 5.x. Subclasses may omit ``@dataclass``.
    For Pydantic validation, install ``python-cqrs[pydantic]`` and use
    ``PydanticRequest``.

    [AI Instruction]
    This is the default ``Request``. Subclass it directly. Omit
    ``@dataclass``; the base applies it. Use ``PydanticRequest`` when the
    payload must be validated, after installing ``python-cqrs[pydantic]``.
    ``Request[Result]`` publishes ``Result`` to ``send``, ``stream``, and
    ``RequestHandler[ThatRequest]``. An unparameterized ``class Command(Request)``
    stays ``Request[Any]``.
    """

    def __init_subclass__(cls, **kwargs: typing.Any) -> None:
        super().__init_subclass__(**kwargs)
        ensure_dataclass(cls, frozen=False)

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        return cls(**kwargs)

    def to_dict(self) -> dict:
        return to_plain_dict(self)


Request = DCRequest

try:
    from cqrs.models.pydantic import PydanticRequest  # noqa: E402
except ImportError:  # pragma: no cover
    PydanticRequest = None  # type: ignore[misc, assignment]


def __getattr__(name: str) -> typing.Any:
    if name == "PydanticRequest":
        if PydanticRequest is not None:
            return PydanticRequest
        raise pydantic_extra_error(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ("Request", "IRequest", "DCRequest", "PydanticRequest", "ReqT", "ResT")
