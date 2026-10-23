"""
Name registry for inbox messages.

[AI Instruction]
Register each request or event name before ``InboxRepository.add``. JSON is
the default codec. A second ``register`` of the same name raises ``KeyError``.
Prefer an instance when tests must not share the process-global map.
"""

import functools
import typing

import orjson

from cqrs.inbox.repository import InboxEntry
from cqrs.models.event import IEvent
from cqrs.models.request import IRequest

_KT = typing.TypeVar("_KT")


class _JsonMessageSerializer:
    """JSON codec. Wire format is ``orjson.dumps(message.to_dict())``."""

    def serialize(self, message: InboxEntry) -> bytes:
        return orjson.dumps(message.to_dict())

    def deserialize(self, payload: bytes, message_type: typing.Type[_KT]) -> _KT:
        data = orjson.loads(payload)
        if not isinstance(data, dict):
            raise TypeError("inbox payload must be a JSON object")
        return message_type.from_dict(**data)  # type: ignore[attr-defined]


class MessageCodec(typing.Protocol):
    """Serialize and deserialize one inbox message type."""

    def serialize(self, message: InboxEntry) -> bytes: ...

    def deserialize(self, payload: bytes, message_type: typing.Type[typing.Any]) -> typing.Any: ...


class _bind_self_or_singleton:
    """Class access binds the process singleton; instance access binds that map."""

    def __init__(self, func: typing.Callable) -> None:
        self.func = func

    def __get__(
        self,
        obj: typing.Any,
        objtype: type | None = None,
    ) -> typing.Callable:
        target = obj if obj is not None else objtype._singleton()  # type: ignore[union-attr]
        return functools.partial(self.func, target)


def message_name(message: InboxEntry) -> str:
    """
    Name stored with the row.

    Notification events use ``event_name``, the same string the outbox stores.
    Requests and domain events use the class name.
    """

    event_name = getattr(message, "event_name", None)
    if isinstance(event_name, str) and event_name:
        return event_name
    return type(message).__name__


def _type_matches(message_type: type, registered: typing.Any) -> bool:
    registered_origin = typing.get_origin(registered) or registered
    if message_type is registered:
        return True
    if typing.get_origin(registered) is not None and message_type is registered_origin:
        return True
    if isinstance(registered, type) and issubclass(message_type, registered):
        return True
    return False


class InboxMessageMap:
    """
    Registry of inbox message names to request or event types.

    Class-level ``register`` / ``get`` use a process-global singleton.
    ``InboxMessageMap()`` creates an isolated registry.

    [AI Instruction]
    Tell the inbox which Python type belongs to a message name. Register
    before the repository reads or writes that message. JSON is the default.
    A second registration of the same name raises ``KeyError``. Requests and
    domain events are registered under the class name. Notification events
    are registered under ``event_name``.
    """

    _instance: typing.ClassVar["InboxMessageMap | None"] = None

    def __init__(self) -> None:
        self._entries: typing.Dict[str, typing.Tuple[typing.Any, MessageCodec | None]] = {}

    @classmethod
    def _singleton(cls) -> "InboxMessageMap":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @_bind_self_or_singleton
    def register(
        self,
        message_name_: str,
        message_type: typing.Type[InboxEntry],
        serializer: MessageCodec | None = None,
    ) -> None:
        """
        Bind ``message_name_`` to a request or event class.

        [AI Instruction]
        Call once at startup. A second registration of the same name raises
        ``KeyError``. Omit ``serializer`` for JSON.
        """

        if message_name_ in self._entries:
            raise KeyError(f"Message {message_name_} already registered")
        self._entries[message_name_] = (message_type, serializer)

    @_bind_self_or_singleton
    def get(self, message_name_: str) -> typing.Any | None:
        """Return the registered type, or ``None`` when the name is unknown."""

        entry = self._entries.get(message_name_)
        if entry is None:
            return None
        return entry[0]

    @_bind_self_or_singleton
    def get_serializer(self, message_name_: str) -> MessageCodec | None:
        """Return the codec registered for this name, or ``None`` for JSON."""

        entry = self._entries.get(message_name_)
        if entry is None:
            return None
        return entry[1]

    @_bind_self_or_singleton
    def deserialize(self, message_name_: str, payload: bytes) -> InboxEntry:
        """
        Rebuild a request or event from stored bytes.

        [AI Instruction]
        ``InboxProcessor`` calls this. An unknown name raises ``KeyError``
        and does not return a message.
        """

        entry = self._entries.get(message_name_)
        if entry is None:
            raise KeyError(f"Unknown message name {message_name_}")
        message_type, serializer = entry
        codec = serializer or _JsonMessageSerializer()
        return codec.deserialize(payload, message_type)

    @_bind_self_or_singleton
    def payload_for(self, message: InboxEntry) -> typing.Tuple[str, bytes]:
        """
        Return ``(message_name, payload)`` for a registered message.

        Raises ``TypeError`` when the name is missing or the instance type
        does not match the registration.
        """

        if not isinstance(message, (IRequest, IEvent)):
            raise TypeError(f"Unsupported inbox message type {type(message)}")
        name = message_name(message)
        registered = self.get(name)
        if registered is None:
            raise TypeError(f"Unknown message name for {name}")
        if not _type_matches(type(message), registered):
            raise TypeError(
                f"Message type {type(message)} does not match registered type {registered}",
            )
        codec = self.get_serializer(name) or _JsonMessageSerializer()
        return name, codec.serialize(message)


def resolve_message_map(
    message_map: "InboxMessageMap | None",
) -> InboxMessageMap:
    """Use the given instance, or the process singleton when ``message_map`` is omitted."""

    if message_map is None or message_map is InboxMessageMap:
        return InboxMessageMap._singleton()
    return message_map


__all__ = (
    "InboxMessageMap",
    "MessageCodec",
    "message_name",
    "resolve_message_map",
)
