"""
Transactional inbox repository protocol.

[AI Instruction]
Presentation calls ``add`` with a batch of ``(message_id, message)`` and
does not commit inside ``add``. A separate process uses ``get_many`` and
``InboxProcessor``. There is no handler and no middleware whose only job
is to insert the inbox row.
"""

import abc
import contextlib
import dataclasses
import datetime
import enum
import sys
import typing

from cqrs.models.event import IEvent
from cqrs.models.request import IRequest

if sys.version_info >= (3, 11):
    StrEnum = enum.StrEnum  # novm
else:

    class StrEnum(str, enum.Enum):  # type: ignore[misc]
        """Compatible StrEnum for Python 3.10."""

        def __str__(self) -> str:
            return self.value


class InboxStatus(StrEnum):
    """
    Lifecycle of one inbox row.

    Presentation inserts ``NEW``. ``InboxProcessor`` moves a row to
    ``PROCESSED`` when the mediator finishes and to ``NOT_PROCESSED`` when
    that attempt raises. ``NOT_PROCESSED`` increments ``flush_counter``.
    After five failures the row leaves ``get_many``.
    """

    NEW = "new"
    PROCESSED = "processed"
    NOT_PROCESSED = "not_processed"


MAX_FLUSH_COUNTER_VALUE = 5

InboxEntry = typing.Union[IRequest, IEvent]


@dataclasses.dataclass(frozen=True)
class InboxMessage:
    """
    One stored inbox row, returned by ``get_many``.

    Application code does not construct it. ``payload`` is the bytes written
    by ``add``. ``InboxProcessor`` deserializes them through ``InboxMessageMap``.
    """

    id: int
    message_id: str
    message_name: str
    status: InboxStatus
    flush_counter: int
    created_at: datetime.datetime
    payload: bytes


class InboxRepository(abc.ABC):
    """
    [AI Instruction]
    Store a command or an event before a worker runs it. ``add`` always takes
    a batch and returns ``list[bool]`` of the same length. ``True`` means the
    row was inserted. ``False`` means ``(message_id, message_name)`` is already
    stored, including earlier in this batch. An empty batch returns ``[]``.
    ``add`` does not commit. The caller commits that session. ``get_many`` and
    ``InboxProcessor.process`` run later, on their own session.
    """

    @abc.abstractmethod
    async def add(
        self,
        messages: typing.Sequence[typing.Tuple[str, InboxEntry]],
    ) -> typing.List[bool]:
        """
        Insert a batch. One message is a batch of one element.

        [AI Instruction]
        Pass ``[(message_id, request_or_event)]``. Register every message name
        on the ``InboxMessageMap`` this repository uses before ``add``. A
        duplicate key returns ``False`` and does not abort the outer
        transaction. Do not commit here.
        """

    @abc.abstractmethod
    async def get_many(self, batch_size: int = 100) -> typing.List[InboxMessage]:
        """
        Return up to ``batch_size`` rows that still need work.

        Rows have status ``new`` or ``not_processed`` and ``flush_counter``
        below five, ordered by ``id`` ascending. SQLAlchemy and SQLModel lock
        those rows with ``FOR UPDATE SKIP LOCKED``. SQLite does not.

        [AI Instruction]
        Call this from the worker, not from the HTTP route or the subscriber.
        """

    @abc.abstractmethod
    async def update_status(self, row_id: int, new_status: InboxStatus) -> None:
        """
        Set one row's status.

        ``NOT_PROCESSED`` increments ``flush_counter``. ``InboxProcessor``
        calls this. Application handlers do not.
        """

    @abc.abstractmethod
    def nested(self) -> contextlib.AbstractAsyncContextManager[None]:
        """
        Open a savepoint.

        [AI Instruction]
        ``InboxProcessor`` runs ``send`` and the ``processed`` status update
        inside this block. An exception rolls back only that savepoint.
        """

    @abc.abstractmethod
    async def commit(self) -> None:
        """
        Commit the current session.

        [AI Instruction]
        The caller commits the session. ``add`` and ``process`` do not.
        """

    @abc.abstractmethod
    async def rollback(self) -> None:
        """Drop the current session's uncommitted inbox and business rows."""


__all__ = (
    "MAX_FLUSH_COUNTER_VALUE",
    "InboxEntry",
    "InboxMessage",
    "InboxRepository",
    "InboxStatus",
)
