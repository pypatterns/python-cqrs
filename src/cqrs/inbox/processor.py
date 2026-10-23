"""
Read inbox rows and hand them to the mediator that is already registered.

[AI Instruction]
Run ``InboxProcessor`` in a worker process. It does not replace
``repository.add`` in a route or a subscriber. ``process`` does not commit.
The worker commits the business rows and the status together.
"""

import functools
import logging
import typing

from cqrs.inbox.map import InboxMessageMap
from cqrs.inbox.repository import InboxMessage, InboxRepository, InboxStatus
from cqrs.models.event import IEvent
from cqrs.models.request import IRequest

logger = logging.getLogger("cqrs")


class InboxProcessor:
    """
    Deserialize one inbox row and send it through an existing mediator.

    A ``Request`` goes to ``RequestMediator.send``. An event goes to
    ``EventMediator.send``. Each row has its own savepoint: success stores
    ``processed`` inside that savepoint, and a failure stores ``not_processed``
    on the outer transaction after the savepoint rolls back.

    [AI Instruction]
    Construct it with the repository for this batch's session, the same
    ``InboxMessageMap`` that repository uses, and the mediators bootstrap
    already built. The caller owns the session and the commit. Do not add a
    handler whose only job is to write the inbox row. The handler that
    ``send`` runs must not call ``session.commit``.
    """

    def __init__(
        self,
        repository: InboxRepository,
        message_map: InboxMessageMap,
        request_mediator: typing.Any | None = None,
        event_mediator: typing.Any | None = None,
    ) -> None:
        self.repository = repository
        self._map = message_map
        self._request_mediator = request_mediator
        self._event_mediator = event_mediator

    async def batch_generator(
        self,
        batch_size: int = 100,
    ) -> typing.AsyncIterator[typing.List[InboxMessage]]:
        """
        Yield batches until the caller stops the loop.

        [AI Instruction]
        Poll forever. Each yield is one ``get_many`` result from the
        repository passed to ``__init__``. Process every row, then let the
        caller commit. This generator does not commit and does not end on
        an empty batch.
        """

        while True:
            yield await self.repository.get_many(batch_size=batch_size)

    async def process(self, row: InboxMessage) -> None:
        """
        Run one row.

        [AI Instruction]
        Call this for each row from ``get_many``. The repository is the one
        passed to ``__init__``. A handler exception rolls back that row's
        business writes and leaves neighbors in the batch untouched.
        ``not_processed`` increments ``flush_counter``. The row stays
        selectable until the counter reaches five.
        """

        try:
            message = self._map.deserialize(row.message_name, row.payload)
            async with self.repository.nested():
                await self._dispatch(message)
                await self.repository.update_status(row.id, InboxStatus.PROCESSED)
        except Exception:
            logger.exception(
                "inbox row %s (%s) failed",
                row.id,
                row.message_name,
            )
            await self.repository.update_status(row.id, InboxStatus.NOT_PROCESSED)

    @typing.overload
    async def _dispatch(self, message: IRequest) -> None: ...

    @typing.overload
    async def _dispatch(self, message: IEvent) -> None: ...

    @functools.singledispatchmethod
    async def _dispatch(self, message: typing.Any) -> None:
        raise TypeError(f"Unsupported inbox message type {type(message)}")

    @_dispatch.register(IRequest)  # pyright: ignore[reportFunctionMemberAccess]
    async def _dispatch_request(self, message: IRequest) -> None:
        if self._request_mediator is None:
            raise RuntimeError("RequestMediator is not configured")
        await self._request_mediator.send(message)

    @_dispatch.register(IEvent)  # pyright: ignore[reportFunctionMemberAccess]
    async def _dispatch_event(self, message: IEvent) -> None:
        if self._event_mediator is None:
            raise RuntimeError("EventMediator is not configured")
        await self._event_mediator.send(message)


__all__ = ("InboxProcessor",)
