import abc
import dataclasses
import enum
import sys
import typing

import cqrs
from cqrs.events.event import INotificationEvent

if sys.version_info >= (3, 11):
    StrEnum = enum.StrEnum  # novm
else:
    # For Python 3.10 compatibility, use regular Enum with string values
    class StrEnum(str, enum.Enum):  # type: ignore[misc]
        """Compatible StrEnum for Python 3.10."""

        def __str__(self) -> str:
            return self.value


class EventStatus(StrEnum):
    NEW = "new"
    PRODUCED = "produced"
    NOT_PRODUCED = "not_produced"


@dataclasses.dataclass(frozen=True)
class OutboxedEvent:
    """
    Outboxed event dataclass.

    Outboxed events represent notification events that are stored in an outbox
    pattern for reliable message delivery. They include metadata about the event
    and its processing status.

    This is an internal data structure used by the outbox pattern implementation.

    Args:
        id: Unique identifier for the outboxed event
        event: The notification event being stored
        topic: Message broker topic where the event should be published
        status: Current processing status of the event

    Example::

        outboxed_event = OutboxedEvent(
            id=1,
            event=notification_event,
            topic="user.events",
            status=EventStatus.NEW
        )
    """

    id: int
    event: cqrs.INotificationEvent
    topic: str
    status: EventStatus


class OutboxedEventRepository(abc.ABC):
    @abc.abstractmethod
    def add(
        self,
        event: INotificationEvent,
    ) -> None:
        """Add an event to the repository."""

    @abc.abstractmethod
    async def get_many(
        self,
        batch_size: int = 100,
        topic: typing.Optional[typing.Text] = None,
    ) -> typing.List[OutboxedEvent]:
        """Get many events from the repository."""

    @abc.abstractmethod
    async def update_status(
        self,
        outboxed_event_id: int,
        new_status: EventStatus,
    ):
        """Update the event status"""

    @abc.abstractmethod
    async def commit(self):
        pass

    @abc.abstractmethod
    async def rollback(self):
        pass


__all__ = (
    "EventStatus",
    "OutboxedEvent",
    "OutboxedEventRepository",
)
