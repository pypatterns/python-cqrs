import abc
import dataclasses
import datetime
import os
import sys
import typing
import uuid

import dotenv
from dataclass_wizard import asdict, fromdict

from typing_extensions import dataclass_transform

from cqrs._dataclass_utils import ensure_dataclass

if sys.version_info >= (3, 11):
    from typing import Self  # novm
else:
    from typing_extensions import Self

dotenv.load_dotenv()
DEFAULT_OUTPUT_TOPIC = os.getenv("DEFAULT_OUTPUT_TOPIC", "output_topic")

# Type variable for generic payload types
PayloadT = typing.TypeVar("PayloadT", bound=typing.Any)


class IEvent(abc.ABC):
    """
    Interface for event-type objects.

    This abstract base class defines the contract that all event implementations
    must follow. Events represent domain events or notification events in the
    CQRS pattern and are used for communication between different parts of the system.

    All event implementations must provide:
    - `to_dict()`: Convert the event instance to a dictionary representation
    - `from_dict()`: Create an event instance from a dictionary
    """

    @abc.abstractmethod
    def to_dict(self) -> dict:
        """
        Convert the event instance to a dictionary representation.

        Returns:
            A dictionary containing all fields of the event instance.
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, **kwargs) -> Self:
        """
        Create an event instance from keyword arguments.

        Args:
            **kwargs: Keyword arguments matching the event fields.

        Returns:
            A new instance of the event class.
        """
        raise NotImplementedError


_LIBRARY_EVENT_BASES = frozenset(
    {"DCEvent", "DCDomainEvent", "DCNotificationEvent"},
)


@dataclass_transform(frozen_default=True)
@dataclasses.dataclass(frozen=True)
class DCEvent(IEvent):
    """
    Dataclass-based implementation of the event interface.

    Subclasses may omit ``@dataclass``; ``__init_subclass__`` applies it.
    Pass ``frozen=True`` (default) for immutable subclasses::

        class UserCreatedEvent(DCEvent, frozen=True):
            user_id: str
            username: str
    """

    def __init_subclass__(
        cls,
        frozen: bool = True,
        kw_only: bool = False,
        **kwargs: typing.Any,
    ) -> None:
        super().__init_subclass__(**kwargs)
        # Intermediate library bases are wrapped by their own @dataclass decorator.
        if cls.__name__ in _LIBRARY_EVENT_BASES:
            return
        ensure_dataclass(cls, frozen=frozen, kw_only=kw_only)

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        return fromdict(cls, kwargs)

    def to_dict(self) -> dict:
        return asdict(self)


class IDomainEvent(IEvent):
    """
    Interface for domain event objects.

    Domain events represent something that happened in the domain that domain experts
    care about. They are typically used for in-process event handling within the
    same bounded context.
    """


@dataclass_transform(frozen_default=True)
@dataclasses.dataclass(frozen=True)
class DCDomainEvent(DCEvent, IDomainEvent):
    """
    Dataclass-based implementation of domain events.

    Default for the ``DomainEvent`` alias in 5.x. For Pydantic validation,
    install ``python-cqrs[pydantic]`` and use ``PydanticDomainEvent``.
    """

    def __init_subclass__(cls, frozen: bool = True, **kwargs: typing.Any) -> None:
        super().__init_subclass__(frozen=frozen, **kwargs)


class INotificationEvent(IEvent, typing.Generic[PayloadT]):
    """
    Interface for notification event objects.

    Notification events are used for cross-service communication and are typically
    published to message brokers (Kafka, RabbitMQ, etc.).
    """

    if typing.TYPE_CHECKING:
        event_id: uuid.UUID
        event_timestamp: datetime.datetime
        event_name: str
        topic: str
        payload: PayloadT

        def proto(self) -> typing.Any: ...

        @classmethod
        def from_proto(cls, proto: typing.Any) -> Self: ...


@dataclass_transform(frozen_default=True, kw_only_default=True)
@dataclasses.dataclass(frozen=True, kw_only=True)
class DCNotificationEvent(
    DCEvent,
    INotificationEvent[PayloadT],
    typing.Generic[PayloadT],
):
    """
    Dataclass-based implementation of notification events.

    Default for the ``NotificationEvent`` alias in 5.x. Fields are keyword-only
    so subclasses may override ``event_name`` with a default without breaking
    field ordering. For Pydantic validation, install ``python-cqrs[pydantic]``
    and use ``PydanticNotificationEvent``.
    """

    event_name: str
    payload: PayloadT

    event_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)
    event_timestamp: datetime.datetime = dataclasses.field(
        default_factory=datetime.datetime.now,
    )
    topic: str = dataclasses.field(default=DEFAULT_OUTPUT_TOPIC)

    def __init_subclass__(cls, frozen: bool = True, **kwargs: typing.Any) -> None:
        super().__init_subclass__(frozen=frozen, kw_only=True, **kwargs)

    def proto(self) -> typing.Any:
        raise NotImplementedError("Method not implemented")

    @classmethod
    def from_proto(cls, proto: typing.Any) -> Self:
        raise NotImplementedError("Method not implemented")

    def __hash__(self) -> int:
        return hash(self.event_id)


# Defaults are dataclass-based (no pydantic required).
# Pydantic* types live in cqrs.events.pydantic (optional extra).
Event = DCEvent
DomainEvent = DCDomainEvent
NotificationEvent = DCNotificationEvent


__all__ = (
    "IEvent",
    "IDomainEvent",
    "INotificationEvent",
    "DCEvent",
    "DCDomainEvent",
    "DCNotificationEvent",
    "Event",
    "DomainEvent",
    "NotificationEvent",
    "PayloadT",
    "DEFAULT_OUTPUT_TOPIC",
)
