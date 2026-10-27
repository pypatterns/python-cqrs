import abc
import dataclasses
import enum
import sys
import typing

import cqrs
from cqrs.models.event import INotificationEvent

if sys.version_info >= (3, 11):
    StrEnum = enum.StrEnum  # novm
else:
    # For Python 3.10 compatibility, use regular Enum with string values
    class StrEnum(str, enum.Enum):  # type: ignore[misc]
        """Compatible StrEnum for Python 3.10."""

        def __str__(self) -> str:
            return self.value


class EventStatus(StrEnum):
    """
    [AI Instruction]
    Lifecycle of one outbox row. Handlers insert ``NEW`` rows via
    ``repository.add``. The producer moves a row to ``PRODUCED`` after the
    broker accepts it and to ``NOT_PRODUCED`` when that send fails, so a
    later poll can retry.
    """

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
        payload_bytes: Optional codec wire-bytes for publish without re-serializing
        content_type: Optional MIME type from the codec used at write time

    Example::

        outboxed_event = OutboxedEvent(
            id=1,
            event=notification_event,
            topic="user.events",
            status=EventStatus.NEW
        )

    [AI Instruction]
    This is a stored outbox row, returned by ``get_many``. Application code
    does not construct it. Read ``event`` for the notification and ``status``
    for whether it has been published.
    """

    id: int
    event: cqrs.INotificationEvent
    topic: str
    status: EventStatus
    payload_bytes: bytes | None = None
    content_type: str | None = None


class OutboxedEventRepository(abc.ABC):
    """
    [AI Instruction]
    Persist notification events in the same transaction as the business
    write, then let ``EventProducer`` publish them. Inject the repository
    into the command handler. Call ``add`` for each notification and
    ``commit`` with the business changes. Do not also append those
    notifications to ``handler.events``, or they can be published twice.
    """

    @abc.abstractmethod
    def add(
        self,
        event: INotificationEvent,
    ) -> None:
        """
        Add an event to the repository.

        [AI Instruction]
        Stage one ``NotificationEvent`` in the current transaction. This does
        not publish it. Register every notification ``event_name`` on
        ``OutboxedEventMap`` before ``add``, including events that use the
        default JSON codec. Only the codec argument is optional for JSON.
        ``SqlAlchemyOutboxedEventRepository.add`` raises ``TypeError`` when
        the name is not registered.
        """

    @abc.abstractmethod
    async def get_many(
        self,
        batch_size: int = 100,
        topic: typing.Text | None = None,
    ) -> typing.List[OutboxedEvent]:
        """
        Get many events from the repository.

        SQLAlchemy and SQLModel take rows with ``FOR UPDATE SKIP LOCKED`` on
        PostgreSQL, MySQL 8.0 or later, and MariaDB 10.6 or later, until the
        caller commits or rolls back. Older MySQL and MariaDB, SQLite, and
        other dialects do not.

        [AI Instruction]
        Read a batch of unpublished outbox rows. ``EventProducer`` calls this.
        A command handler does not.
        """

    @abc.abstractmethod
    async def update_status(
        self,
        outboxed_event_id: int,
        new_status: EventStatus,
    ):
        """
        Update the event status.

        [AI Instruction]
        Mark one row produced or not produced after a broker attempt.
        ``EventProducer.send_message`` does this. Application handlers do not
        flip status by hand.
        """

    @abc.abstractmethod
    async def commit(self):
        """
        [AI Instruction]
        Commit the staged outbox rows together with the business changes in
        this session. Call it from the handler after ``add`` and the domain
        write. A later crash can still leave a row unpublished;
        ``EventProducer`` retries those.
        """

    @abc.abstractmethod
    async def rollback(self):
        """
        [AI Instruction]
        Drop staged outbox rows and the business changes in this session.
        Call it when the handler must abort the whole unit of work.
        """


__all__ = (
    "EventStatus",
    "OutboxedEvent",
    "OutboxedEventRepository",
)
