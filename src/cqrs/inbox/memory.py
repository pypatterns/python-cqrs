"""In-memory inbox repository for unit tests."""

import contextlib
import dataclasses
import datetime
import typing

from cqrs.inbox.map import InboxMessageMap, resolve_message_map
from cqrs.inbox.repository import (
    MAX_FLUSH_COUNTER_VALUE,
    InboxEntry,
    InboxMessage,
    InboxRepository,
    InboxStatus,
)


class MemoryInboxRepository(InboxRepository):
    """
    Process-local inbox.

    ``add`` is visible to ``get_many`` on this instance before ``commit``,
    matching a session that has flushed and not committed. ``rollback``
    restores the last ``commit``. ``nested`` snapshots the rows and restores
    them when the block raises.
    """

    def __init__(self, message_map: InboxMessageMap | None = None) -> None:
        self._message_map = resolve_message_map(message_map)
        self._rows: typing.List[InboxMessage] = []
        self._checkpoint: typing.List[InboxMessage] = []
        self._next_id = 1

    async def add(
        self,
        messages: typing.Sequence[typing.Tuple[str, InboxEntry]],
    ) -> typing.List[bool]:
        if not messages:
            return []
        stored = {(row.message_id, row.message_name) for row in self._rows}
        results: typing.List[bool] = []
        for message_id, message in messages:
            name, payload = self._message_map.payload_for(message)
            key = (message_id, name)
            if key in stored:
                results.append(False)
                continue
            self._rows.append(
                InboxMessage(
                    id=self._next_id,
                    message_id=message_id,
                    message_name=name,
                    status=InboxStatus.NEW,
                    flush_counter=0,
                    created_at=datetime.datetime.now(),
                    payload=payload,
                ),
            )
            self._next_id += 1
            stored.add(key)
            results.append(True)
        return results

    async def get_many(self, batch_size: int = 100) -> typing.List[InboxMessage]:
        selectable = [
            row
            for row in self._rows
            if row.status in (InboxStatus.NEW, InboxStatus.NOT_PROCESSED)
            and row.flush_counter < MAX_FLUSH_COUNTER_VALUE
        ]
        selectable.sort(key=lambda row: row.id)
        return selectable[:batch_size]

    async def update_status(self, row_id: int, new_status: InboxStatus) -> None:
        for index, row in enumerate(self._rows):
            if row.id != row_id:
                continue
            flush_counter = row.flush_counter
            if new_status is InboxStatus.NOT_PROCESSED:
                flush_counter += 1
            self._rows[index] = dataclasses.replace(
                row,
                status=new_status,
                flush_counter=flush_counter,
            )
            return

    def nested(self) -> contextlib.AbstractAsyncContextManager[None]:
        return self._nested()

    @contextlib.asynccontextmanager
    async def _nested(self) -> typing.AsyncIterator[None]:
        snapshot = [dataclasses.replace(row) for row in self._rows]
        next_id = self._next_id
        try:
            yield
        except BaseException:
            self._rows = snapshot
            self._next_id = next_id
            raise

    async def commit(self) -> None:
        self._checkpoint = [dataclasses.replace(row) for row in self._rows]

    async def rollback(self) -> None:
        self._rows = [dataclasses.replace(row) for row in self._checkpoint]
        self._next_id = max((row.id for row in self._rows), default=0) + 1


__all__ = ("MemoryInboxRepository",)
