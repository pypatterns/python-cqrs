"""In-memory outbox repository.

[AI Instruction]
Import ``MemoryOutboxRepository`` from this module. It is not an attribute of
``cqrs``. Each instance has its own id counter. ``PRODUCED`` updates the row
status and keeps the row. ``cqrs.outbox.mock.MockOutboxedEventRepository`` is
a different class and is unchanged.
"""

import dataclasses
import typing

from cqrs.mapping import outbox as map
from cqrs.models.event import INotificationEvent
from cqrs.outbox import repository
from cqrs.serializers.json import JsonEventSerializer
from cqrs.serializers.protocol import EventCodec


class MemoryOutboxRepository(repository.OutboxedEventRepository):
    """
    Process-local outbox.

    The id counter and the rows belong to the instance. A row marked
    ``PRODUCED`` stays in the repository with that status. ``get_many``
    returns ``NEW`` and ``NOT_PRODUCED`` rows only. ``rollback`` drops rows
    added since the last ``commit``.
    """

    def __init__(
        self,
        *,
        serializer: EventCodec | None = None,
        event_map: map.OutboxedEventMap | None = None,
    ) -> None:
        self._counter = 0
        self._rows: dict[int, repository.OutboxedEvent] = {}
        self._staged: list[int] = []
        self._serializer = serializer or JsonEventSerializer()
        self._event_map = event_map

    def _resolve_codec(self, event_name: str) -> EventCodec:
        if self._event_map is not None:
            registered = self._event_map.get_serializer(event_name)
            if registered is not None:
                return registered
        return self._serializer

    def add(self, event: INotificationEvent) -> None:
        self._counter += 1
        row_id = self._counter
        codec = self._resolve_codec(event.event_name)
        self._rows[row_id] = repository.OutboxedEvent(
            id=row_id,
            event=event,
            topic=event.topic,
            status=repository.EventStatus.NEW,
            payload_bytes=codec.serialize(event),
            content_type=codec.content_type_for(event),
        )
        self._staged.append(row_id)

    async def get_many(
        self,
        batch_size: int = 100,
        topic: typing.Text | None = None,
    ) -> typing.List[repository.OutboxedEvent]:
        selectable = [
            row
            for row in self._rows.values()
            if row.status in (repository.EventStatus.NEW, repository.EventStatus.NOT_PRODUCED)
            and (topic is None or row.topic == topic)
        ]
        selectable.sort(key=lambda row: row.id)
        return selectable[:batch_size]

    async def update_status(
        self,
        outboxed_event_id: int,
        new_status: repository.EventStatus,
    ) -> None:
        current = self._rows.get(outboxed_event_id)
        if current is None:
            return
        self._rows[outboxed_event_id] = dataclasses.replace(current, status=new_status)

    async def commit(self) -> None:
        self._staged.clear()

    async def rollback(self) -> None:
        for row_id in self._staged:
            self._rows.pop(row_id, None)
        self._staged.clear()


__all__ = ("MemoryOutboxRepository",)
