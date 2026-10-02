import functools
import typing

from cqrs.models.event import INotificationEvent
from cqrs.serializers.json import JsonEventSerializer
from cqrs.serializers.protocol import EventCodec, EventSerializer


class _bind_self_or_singleton:
    """
    Descriptor for dual class/instance API.

    Class access binds to the process-global singleton; instance access binds to
    that map instance.
    """

    def __init__(self, func: typing.Callable) -> None:
        self.func = func

    def __get__(
        self,
        obj: typing.Any,
        objtype: type | None = None,
    ) -> typing.Callable:
        target = obj if obj is not None else objtype._singleton()  # type: ignore[union-attr]
        return functools.partial(self.func, target)


class OutboxedEventMap:
    """
    Registry of outbox event names to types and optional codecs.

    Class-level ``register`` / ``get`` use a process-global singleton (existing
    behaviour). ``OutboxedEventMap()`` creates an isolated registry.

    Warning: class-level methods mutate the process singleton; prefer an
    instance map when tests or apps need isolation.
    """

    _instance: typing.ClassVar["OutboxedEventMap | None"] = None

    def __init__(self) -> None:
        self._entries: typing.Dict[
            typing.Text,
            typing.Tuple[typing.Type[INotificationEvent], EventCodec | None],
        ] = {}

    @classmethod
    def _singleton(cls) -> "OutboxedEventMap":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @_bind_self_or_singleton
    def register(
        self,
        event_name: typing.Text,
        event_type: typing.Type[INotificationEvent],
        serializer: EventCodec | None = None,
    ) -> None:
        if event_name in self._entries:
            raise KeyError(f"Event with {event_name} already registered")
        self._entries[event_name] = (event_type, serializer)

    @_bind_self_or_singleton
    def get(
        self,
        event_name: typing.Text,
    ) -> typing.Type[INotificationEvent] | None:
        entry = self._entries.get(event_name)
        if entry is None:
            return None
        return entry[0]

    @_bind_self_or_singleton
    def get_serializer(
        self,
        event_name: typing.Text,
    ) -> EventCodec | None:
        entry = self._entries.get(event_name)
        if entry is None:
            return None
        return entry[1]

    @_bind_self_or_singleton
    def as_serializer(
        self,
        default: EventSerializer | None = None,
    ) -> EventSerializer:
        fallback = default or JsonEventSerializer()
        event_map = self

        class _Dispatcher:
            def resolve_codec(self, event: INotificationEvent) -> EventSerializer:
                return event_map.get_serializer(event.event_name) or fallback

            def serialize(self, event: INotificationEvent) -> bytes:
                return self.resolve_codec(event).serialize(event)

            def content_type_for(self, event: INotificationEvent) -> str | None:
                return self.resolve_codec(event).content_type_for(event)

        return _Dispatcher()
