import typing

from cqrs.events.event import INotificationEvent


class EventSerializer(typing.Protocol):
    """Serialize a notification event to wire bytes (emit-only)."""

    def serialize(self, event: INotificationEvent) -> bytes: ...

    def content_type_for(self, event: INotificationEvent) -> str | None: ...


class EventCodec(EventSerializer, typing.Protocol):
    """Serialize and deserialize notification events (outbox repository)."""

    def deserialize(
        self,
        payload: bytes,
        event_type: type[INotificationEvent],
    ) -> INotificationEvent: ...
