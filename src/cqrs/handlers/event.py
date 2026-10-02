import abc
from collections.abc import Sequence
import typing

from cqrs.models.event import IEvent

E = typing.TypeVar("E", bound=IEvent, contravariant=True)


class EventHandler(abc.ABC, typing.Generic[E]):
    """
    The event handler interface.

    Subclasses must implement :meth:`handle`. Optionally override :attr:`events`
    to return follow-up events emitted after handling (e.g. for multi-level
    event chains).

    Example::

        class UserJoinedEvent(DomainEvent):
            meeting_id: str
            user_id: str

        class UserJoinedEventHandler(EventHandler[UserJoinedEvent]):
            def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
                self._meetings_api = meetings_api

            async def handle(self, event: UserJoinedEvent) -> None:
                await self._meetings_api.notify_room(
                    event.meeting_id, "New user joined!"
                )
    """

    @property
    def events(self) -> Sequence[IEvent]:
        """
        Events produced by this handler after :meth:`handle` was called.

        Override in subclasses to return follow-up events that should be
        processed by the same pipeline (e.g. domain events to emit). By default
        returns an empty sequence.

        Returns:
            Sequence of follow-up events (e.g. new domain events) to process.
        """
        return ()

    @abc.abstractmethod
    async def handle(self, event: E) -> None:
        """
        Handle the given event.

        Args:
            event: The event instance to handle.

        Raises:
            NotImplementedError: Must be implemented by subclasses.
        """
        raise NotImplementedError
