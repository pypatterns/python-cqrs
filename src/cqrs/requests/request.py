import abc
import dataclasses
import sys
import typing

import pydantic

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

    All request implementations must provide:
    - `to_dict()`: Convert the request instance to a dictionary representation
    - `from_dict()`: Create a request instance from a dictionary
    """

    @abc.abstractmethod
    def to_dict(self) -> dict:
        """
        Convert the request instance to a dictionary representation.

        Returns:
            A dictionary containing all fields of the request instance.
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, **kwargs) -> Self:
        """
        Create a request instance from keyword arguments.

        Args:
            **kwargs: Keyword arguments matching the request fields.

        Returns:
            A new instance of the request class.
        """
        raise NotImplementedError


# Type variables for request/response (defined here to avoid circular import with
# cqrs.types <-> cqrs.requests.request_handler). Re-exported from cqrs.types for compatibility.
ReqT = typing.TypeVar("ReqT", bound=IRequest, contravariant=True)
ResT = typing.TypeVar("ResT", bound=typing.Optional[IResponse], covariant=True)


@dataclasses.dataclass
class DCRequest(IRequest):
    """
    Dataclass-based implementation of the request interface.

    This class provides a request implementation using Python's dataclasses.
    It's useful when you want to avoid pydantic dependency or prefer dataclasses
    for request definitions.

    Example::

        @dataclasses.dataclass
        class GetUserQuery(DCRequest):
            user_id: str

        query = GetUserQuery(user_id="123")
        data = query.to_dict()  # {"user_id": "123"}
        restored = GetUserQuery.from_dict(**data)
    """

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        """
        Create a request instance from keyword arguments.

        Args:
            **kwargs: Keyword arguments matching the dataclass fields.

        Returns:
            A new instance of the request class.
        """
        return cls(**kwargs)

    def to_dict(self) -> dict:
        """
        Convert the request instance to a dictionary representation.

        Returns:
            A dictionary containing all fields of the dataclass instance.
        """
        return dataclasses.asdict(self)


class PydanticRequest(pydantic.BaseModel, IRequest):
    """
    Pydantic-based implementation of the request interface.

    This class provides a request implementation using Pydantic models.
    It offers data validation, serialization, and other Pydantic features.
    This is the default request implementation used by the library.

    The request is an input of the request handler.
    Often Request is used for defining queries or commands.

    Example::

        class CreateUserCommand(PydanticRequest):
            username: str
            email: str

        command = CreateUserCommand(username="john", email="john@example.com")
        data = command.to_dict()  # {"username": "john", "email": "john@example.com"}
        restored = CreateUserCommand.from_dict(**data)
    """

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        """
        Create a request instance from keyword arguments.

        Validates and converts types, ensuring required fields are present.

        Args:
            **kwargs: Keyword arguments matching the request fields.

        Returns:
            A new instance of the request class.
        """
        return cls.model_validate(kwargs)

    def to_dict(self) -> dict:
        """
        Convert the request instance to a dictionary representation.

        Returns:
            A dictionary containing all fields of the request instance.
        """
        return self.model_dump(mode="python")


Request = PydanticRequest

__all__ = ("Request", "IRequest", "DCRequest", "PydanticRequest", "ReqT", "ResT")
