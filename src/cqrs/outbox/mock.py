import typing

import cqrs
from cqrs.outbox import map, repository
from cqrs.serializers.json import JsonEventSerializer
from cqrs.serializers.protocol import EventCodec


class MockOutboxedEventRepository(repository.OutboxedEventRepository):
    COUNTER: typing.ClassVar = 0

    def __init__(
        self,
        session_factory: typing.Callable[[], typing.Dict],
        *,
        serializer: EventCodec | None = None,
        event_map: map.OutboxedEventMap | None = None,
    ):
        self.session = session_factory()
        self._serializer = serializer or JsonEventSerializer()
        self._event_map = event_map if event_map is not None else map.OutboxedEventMap

    async def __aenter__(self) -> typing.Dict:
        return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    def _resolve_codec(self, event_name: str) -> EventCodec:
        return self._event_map.get_serializer(event_name) or self._serializer

    def add(self, event: cqrs.INotificationEvent) -> None:
        MockOutboxedEventRepository.COUNTER += 1
        codec = self._resolve_codec(event.event_name)
        self.session[MockOutboxedEventRepository.COUNTER] = repository.OutboxedEvent(
            id=MockOutboxedEventRepository.COUNTER,
            event=event,
            topic=event.topic,
            status=repository.EventStatus.NEW,
            payload_bytes=codec.serialize(event),
            content_type=codec.content_type_for(event),
        )

    async def get_many(
        self,
        batch_size: int = 100,
        topic: typing.Text | None = None,
    ) -> typing.List[repository.OutboxedEvent]:
        return list(
            filter(lambda e: topic == e.topic, self.session.values()) if topic else list(self.session.values()),
        )

    async def update_status(
        self,
        outboxed_event_id: int,
        new_status: repository.EventStatus,
    ):
        if outboxed_event_id not in self.session:
            return
        if new_status is repository.EventStatus.PRODUCED:
            del self.session[outboxed_event_id]

    async def commit(self):
        pass

    async def rollback(self):
        pass
