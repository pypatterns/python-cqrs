"""Pydantic model implementations (optional ``python-cqrs[pydantic]`` extra)."""

from __future__ import annotations

import datetime
import sys
import typing
import uuid

import pydantic

from cqrs.models.event import (
    DEFAULT_OUTPUT_TOPIC,
    IDomainEvent,
    IEvent,
    INotificationEvent,
    PayloadT,
)
from cqrs.models.request import IRequest
from cqrs.models.response import IResponse

if sys.version_info >= (3, 11):
    from typing import Self  # novm
else:
    from typing_extensions import Self


class PydanticEvent(pydantic.BaseModel, IEvent, frozen=True):
    """
    Pydantic-based implementation of the event interface.

    Requires ``pip install python-cqrs[pydantic]``. Default aliases
    ``Event`` / ``DomainEvent`` / ``NotificationEvent`` point at the
    dataclass implementations; use these classes when you need Pydantic
    validation.
    """

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        return cls.model_validate(kwargs)

    def to_dict(self) -> dict:
        return self.model_dump(mode="python")


class PydanticDomainEvent(PydanticEvent, IDomainEvent, frozen=True):
    """Pydantic-based domain event (optional pydantic extra)."""


class PydanticNotificationEvent(
    PydanticEvent,
    INotificationEvent[PayloadT],
    typing.Generic[PayloadT],
    frozen=True,
):
    """Pydantic-based notification event (optional pydantic extra)."""

    payload: PayloadT

    event_id: uuid.UUID = pydantic.Field(default_factory=uuid.uuid4)
    event_timestamp: datetime.datetime = pydantic.Field(
        default_factory=datetime.datetime.now,
    )
    event_name: typing.Text
    topic: typing.Text = pydantic.Field(default=DEFAULT_OUTPUT_TOPIC)

    model_config = pydantic.ConfigDict(from_attributes=True)

    def proto(self) -> typing.Any:
        raise NotImplementedError("Method not implemented")

    @classmethod
    def from_proto(cls, proto: typing.Any) -> Self:
        raise NotImplementedError("Method not implemented")

    def __hash__(self) -> int:
        return hash(self.event_id)


__all__ = (
    "PydanticEvent",
    "PydanticDomainEvent",
    "PydanticNotificationEvent",
)


class PydanticRequest(pydantic.BaseModel, IRequest):
    """
    Pydantic-based request implementation.

    Requires ``pip install python-cqrs[pydantic]``. The default ``Request``
    alias points at ``DCRequest``.
    """

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        return cls.model_validate(kwargs)

    def to_dict(self) -> dict:
        return self.model_dump(mode="python")


__all__ = ("PydanticRequest",)


class PydanticResponse(pydantic.BaseModel, IResponse):
    """
    Pydantic-based response implementation.

    Requires ``pip install python-cqrs[pydantic]``. The default ``Response``
    alias points at ``DCResponse``.
    """

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        return cls.model_validate(kwargs)

    def to_dict(self) -> dict:
        return self.model_dump(mode="python")


__all__ = ("PydanticResponse",)

__all__ = (
    "PydanticEvent",
    "PydanticDomainEvent",
    "PydanticNotificationEvent",
    "PydanticRequest",
    "PydanticResponse",
)
