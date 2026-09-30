import typing

from cqrs.events.event import INotificationEvent


class ProtobufEventSerializer:
    """Duck-typed protobuf codec. Does not import ``google.protobuf``."""

    content_type: str | None = "application/x-protobuf"

    def __init__(
        self,
        proto_types: typing.Mapping[type[INotificationEvent], typing.Any],
    ) -> None:
        self._proto_types = dict(proto_types)

    def serialize(self, event: INotificationEvent) -> bytes:
        event_type = type(event)
        if event_type not in self._proto_types:
            raise TypeError(
                f"No protobuf message type registered for {event_type!r}",
            )
        proto = getattr(event, "proto", None)
        if not callable(proto):
            raise TypeError(f"{event_type!r} has no proto() method")
        message = typing.cast(typing.Any, proto())
        return message.SerializeToString()

    def content_type_for(self, event: INotificationEvent) -> str | None:
        return self.content_type

    def deserialize(
        self,
        payload: bytes,
        event_type: type[INotificationEvent],
    ) -> INotificationEvent:
        proto_cls = self._proto_types.get(event_type)
        if proto_cls is None:
            raise TypeError(
                f"No protobuf message type registered for {event_type!r}",
            )
        from_proto = getattr(event_type, "from_proto", None)
        if not callable(from_proto):
            raise TypeError(f"{event_type!r} has no from_proto() method")
        return typing.cast(
            INotificationEvent,
            from_proto(proto_cls.FromString(payload)),
        )
