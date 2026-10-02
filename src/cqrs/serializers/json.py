import orjson

from cqrs.models.event import INotificationEvent


class JsonEventSerializer:
    """JSON codec for notification events. Wire format matches ``orjson.dumps(event.to_dict())``."""

    content_type: str | None = None

    def serialize(self, event: INotificationEvent) -> bytes:
        return orjson.dumps(event.to_dict())

    def content_type_for(self, event: INotificationEvent) -> str | None:
        return None

    def deserialize(
        self,
        payload: bytes,
        event_type: type[INotificationEvent],
    ) -> INotificationEvent:
        return event_type.from_dict(**orjson.loads(payload))
