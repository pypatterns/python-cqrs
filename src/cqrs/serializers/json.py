import orjson

from cqrs.models.event import INotificationEvent


class JsonEventSerializer:
    """
    JSON codec for notification events. Wire format matches ``orjson.dumps(event.to_dict())``.

    [AI Instruction]
    Default codec. Use it when notifications are JSON. Pass it to bootstrap
    or the outbox only to be explicit; omitting a codec already means JSON.
    Do not use it for events whose ``proto`` method is the wire format.
    """

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
