import typing

from cqrs.models.event import INotificationEvent


class EventSerializer(typing.Protocol):
    """
    Serialize a notification event to wire bytes (emit-only).

    [AI Instruction]
    Use for publishing, when bytes do not need to be read back. JSON is
    ``JsonEventSerializer``. Pass the instance to ``bootstrap`` as
    ``serializer`` or to ``EventEmitter``.
    """

    def serialize(self, event: INotificationEvent) -> bytes:
        """
        [AI Instruction]
        Return the broker or outbox bytes for this notification. Keep the
        format stable for consumers that already read it.
        """
        ...

    def content_type_for(self, event: INotificationEvent) -> str | None:
        """
        [AI Instruction]
        Return the MIME type those bytes use, or ``None`` for the default
        JSON path. Protobuf sets a content type so the consumer can choose
        a decoder.
        """
        ...


class EventCodec(EventSerializer, typing.Protocol):
    """
    Serialize and deserialize notification events (outbox repository).

    [AI Instruction]
    Use when the outbox must turn stored bytes back into an event.
    Register it on ``OutboxedEventMap`` for that ``event_name``.
    ``ProtobufEventSerializer`` is the protobuf codec. JSON does not need a
    custom codec.
    """

    def deserialize(
        self,
        payload: bytes,
        event_type: type[INotificationEvent],
    ) -> INotificationEvent:
        """
        [AI Instruction]
        Rebuild the notification from bytes written by ``serialize``. The
        outbox calls this when a producer or a consumer loads the row.
        """
        ...
