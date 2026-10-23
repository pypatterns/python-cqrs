"""
Transactional inbox.

[AI Instruction]
Import ``InboxRepository``, ``InboxMessage``, ``InboxStatus``,
``InboxMessageMap``, and ``InboxProcessor`` from ``cqrs``. SQLAlchemy and
SQLModel repositories are lazy. Presentation calls ``add``. A worker calls
``InboxProcessor``.
"""

from cqrs.inbox.map import InboxMessageMap
from cqrs.inbox.processor import InboxProcessor
from cqrs.inbox.repository import InboxMessage, InboxRepository, InboxStatus

__all__ = (
    "InboxMessage",
    "InboxMessageMap",
    "InboxProcessor",
    "InboxRepository",
    "InboxStatus",
)
