"""Journals and the middleware wrapper that records requests and middleware calls.

[AI Instruction]
These types are the journals on ``TestHarness``. Tests import ``test_harness``
from ``cqrs.testing`` and read the journals from that object. A boolean
``contains`` does not raise. ``of`` returns matches. ``one``, ``assert_contains``,
``assert_one``, ``assert_empty``, ``assert_not_contains``, ``assert_entered``,
and ``assert_not_entered`` raise ``AssertionError`` with a full harness report.
``where=`` is a predicate on the journal item. Keyword filters such as
``topic=``, ``method=``, and ``called_next=`` stay. ``yields`` is a list.
"""

import contextlib
import dataclasses
import re
import typing
import uuid
from contextvars import ContextVar

from cqrs.events.event_emitter import EventEmitterListener
from cqrs.inbox.map import message_name
from cqrs.inbox.repository import InboxMessage, InboxStatus
from cqrs.message_brokers.protocol import Message
from cqrs.outbox.repository import EventStatus
from cqrs.saga.recovery import recover_saga
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus

_RECORDING = "_cqrs_testing_recording"
_SAGA_DEPTH: ContextVar[int] = ContextVar("cqrs_testing_saga_depth", default=0)
_CAMEL_BOUNDARY = re.compile(r"(?<!^)(?=[A-Z])")


def _snake_key(key: str) -> str:
    return _CAMEL_BOUNDARY.sub("_", key).lower()


def _payload_view(payload: typing.Any) -> typing.Any:
    """Expose camelCase broker keys under their snake_case names as well."""

    if isinstance(payload, list):
        return [_payload_view(item) for item in payload]
    if not isinstance(payload, dict):
        return payload
    viewed: dict[typing.Any, typing.Any] = {}
    for key, value in payload.items():
        converted = _payload_view(value)
        viewed[key] = converted
        if isinstance(key, str):
            snake = _snake_key(key)
            if snake not in viewed:
                viewed[snake] = converted
    return viewed


def _event_names(event_type: type) -> set[str]:
    names = {event_type.__name__}
    event_name = getattr(event_type, "event_name", None)
    if isinstance(event_name, str):
        names.add(event_name)
    model_fields = getattr(event_type, "model_fields", None)
    if isinstance(model_fields, dict) and "event_name" in model_fields:
        default = getattr(model_fields["event_name"], "default", None)
        if isinstance(default, str):
            names.add(default)
    return names


_T = typing.TypeVar("_T")


def _apply_where(
    items: list[_T],
    where: typing.Callable[[_T], bool] | None,
) -> list[_T]:
    if where is None:
        return list(items)
    return [item for item in items if where(item)]


def _query_filters(
    *,
    topic: str | None = None,
    method: str | None = None,
    called_next: bool | None = None,
) -> dict[str, typing.Any]:
    filters: dict[str, typing.Any] = {}
    if topic is not None:
        filters["topic"] = topic
    if method is not None:
        filters["method"] = method
    if called_next is not None:
        filters["called_next"] = called_next
    return filters


class _Reported:
    """Journals that raise ``AssertionError`` with a full harness dump."""

    _journal_name: str = ""
    _report: typing.Any | None = None

    def bind_report(self, report: typing.Any) -> None:
        self._report = report

    def _raise_report(
        self,
        method: str,
        *,
        looked_for: typing.Any = None,
        filters: dict[str, typing.Any] | None = None,
        has_where: bool = False,
        matches: int = 0,
    ) -> typing.NoReturn:
        from cqrs.testing.report import render_failure

        raise AssertionError(
            render_failure(
                self._report,
                journal=self._journal_name,
                method=method,
                looked_for=looked_for,
                filters=filters or {},
                has_where=has_where,
                matches=matches,
            ),
        )


class _JournalQuery(_Reported, typing.Generic[_T]):
    """contains / of / assert_* over ``_matching`` and ``where=``."""

    def _matching(self, item_type: type, **filters: typing.Any) -> list[_T]:
        raise NotImplementedError

    def _is_empty(self) -> bool:
        return not len(self)  # type: ignore[arg-type]

    def of(
        self,
        item_type: type,
        *,
        where: typing.Callable[[_T], bool] | None = None,
        topic: str | None = None,
        method: str | None = None,
        called_next: bool | None = None,
    ) -> list[_T]:
        return _apply_where(
            self._matching(item_type, **_query_filters(topic=topic, method=method, called_next=called_next)),
            where,
        )

    def contains(
        self,
        item_type: type,
        *,
        where: typing.Callable[[_T], bool] | None = None,
        topic: str | None = None,
        method: str | None = None,
        called_next: bool | None = None,
    ) -> bool:
        return bool(self.of(item_type, where=where, topic=topic, method=method, called_next=called_next))

    def _one(
        self,
        item_type: type,
        *,
        where: typing.Callable[[_T], bool] | None = None,
        topic: str | None = None,
        method: str | None = None,
        called_next: bool | None = None,
    ) -> _T:
        matches = self.of(item_type, where=where, topic=topic, method=method, called_next=called_next)
        if len(matches) != 1:
            self._fail_query(
                "one",
                item_type,
                matches,
                where=where,
                topic=topic,
                method=method,
                called_next=called_next,
            )
        return matches[0]

    def assert_contains(
        self,
        item_type: type,
        *,
        where: typing.Callable[[_T], bool] | None = None,
        topic: str | None = None,
        method: str | None = None,
        called_next: bool | None = None,
    ) -> None:
        matches = self.of(item_type, where=where, topic=topic, method=method, called_next=called_next)
        if matches:
            return
        self._fail_query(
            "assert_contains",
            item_type,
            matches,
            where=where,
            topic=topic,
            method=method,
            called_next=called_next,
        )

    def assert_one(
        self,
        item_type: type,
        *,
        where: typing.Callable[[_T], bool] | None = None,
        topic: str | None = None,
        method: str | None = None,
        called_next: bool | None = None,
    ) -> None:
        matches = self.of(item_type, where=where, topic=topic, method=method, called_next=called_next)
        if len(matches) == 1:
            return
        self._fail_query(
            "assert_one",
            item_type,
            matches,
            where=where,
            topic=topic,
            method=method,
            called_next=called_next,
        )

    def assert_not_contains(
        self,
        item_type: type,
        *,
        where: typing.Callable[[_T], bool] | None = None,
        topic: str | None = None,
        method: str | None = None,
        called_next: bool | None = None,
    ) -> None:
        matches = self.of(item_type, where=where, topic=topic, method=method, called_next=called_next)
        if not matches:
            return
        self._fail_query(
            "assert_not_contains",
            item_type,
            matches,
            where=where,
            topic=topic,
            method=method,
            called_next=called_next,
        )

    def assert_empty(self) -> None:
        if self._is_empty():
            return
        try:
            count = len(self)  # type: ignore[arg-type]
        except TypeError:
            count = 1
        self._raise_report("assert_empty", matches=count)

    def _fail_query(
        self,
        method_name: str,
        item_type: type,
        matches: list[_T],
        *,
        where: typing.Callable[[_T], bool] | None,
        topic: str | None,
        method: str | None,
        called_next: bool | None,
    ) -> typing.NoReturn:
        self._raise_report(
            method_name,
            looked_for=item_type,
            filters=_query_filters(topic=topic, method=method, called_next=called_next),
            has_where=where is not None,
            matches=len(matches),
        )


class _TypedJournal(_JournalQuery[_T]):
    def one(
        self,
        item_type: type,
        *,
        where: typing.Callable[[_T], bool] | None = None,
        topic: str | None = None,
        method: str | None = None,
        called_next: bool | None = None,
    ) -> _T:
        return self._one(item_type, where=where, topic=topic, method=method, called_next=called_next)


@dataclasses.dataclass(frozen=True)
class Invocation:
    """One entry into ``handle``, ``act``, or ``compensate``."""

    handler: type
    method: str
    argument: typing.Any
    argument_type: type
    handler_instance: typing.Any


class InvocationJournal(_TypedJournal[Invocation]):
    """Handlers that actually entered ``handle``, ``act``, or ``compensate``."""

    _journal_name = "invocations"

    def __init__(self) -> None:
        self._items: list[Invocation] = []

    def record(
        self,
        handler: type,
        method: str,
        argument: typing.Any,
        handler_instance: typing.Any,
    ) -> None:
        self._items.append(
            Invocation(
                handler=handler,
                method=method,
                argument=argument,
                argument_type=type(argument),
                handler_instance=handler_instance,
            ),
        )

    def __iter__(self) -> typing.Iterator[Invocation]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def _matching(self, item_type: type, **filters: typing.Any) -> list[Invocation]:
        found = [item for item in self._items if item.handler is item_type]
        method = filters.get("method")
        if method is not None:
            found = [item for item in found if item.method == method]
        return found

    def assert_entered(
        self,
        handler_type: type,
        *,
        method: str | None = None,
        where: typing.Callable[[Invocation], bool] | None = None,
    ) -> None:
        self.assert_contains(handler_type, method=method, where=where)

    def assert_not_entered(
        self,
        handler_type: type,
        *,
        method: str | None = None,
        where: typing.Callable[[Invocation], bool] | None = None,
    ) -> None:
        self.assert_not_contains(handler_type, method=method, where=where)

    def contains_argument(self, event_type: type) -> bool:
        return any(item.argument_type is event_type for item in self._items)


@dataclasses.dataclass(frozen=True)
class RequestCall:
    """One ``RequestMediator.send`` call."""

    request: typing.Any
    response: typing.Any = None
    exception: BaseException | None = None


class RequestJournal(_TypedJournal[RequestCall]):
    """Every ``send``: the request, and the response or the exception."""

    _journal_name = "requests"

    def __init__(self) -> None:
        self._items: list[RequestCall] = []

    def record(
        self,
        request: typing.Any,
        *,
        response: typing.Any = None,
        exception: BaseException | None = None,
    ) -> None:
        self._items.append(RequestCall(request=request, response=response, exception=exception))

    def _matching(self, item_type: type, **filters: typing.Any) -> list[RequestCall]:
        return [item for item in self._items if type(item.request) is item_type]

    def __iter__(self) -> typing.Iterator[RequestCall]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)


class EventJournal(_TypedJournal[typing.Any]):
    """Domain events delivered to handlers, or events that had no handler."""

    def __init__(self, name: str = "handled") -> None:
        self._journal_name = name
        self._events: list[typing.Any] = []

    def add(self, event: typing.Any) -> None:
        self._events.append(event)

    def _matching(self, item_type: type, **filters: typing.Any) -> list[typing.Any]:
        return [event for event in self._events if type(event) is item_type]

    def __iter__(self) -> typing.Iterator[typing.Any]:
        return iter(self._events)

    def __len__(self) -> int:
        return len(self._events)


@dataclasses.dataclass(frozen=True)
class PublishedMessage:
    """One broker message, with snake_case aliases added to ``payload``."""

    message_name: str
    message_id: uuid.UUID
    topic: str
    payload: typing.Any
    payload_bytes: bytes | None
    content_type: str | None


class PublishedJournal(_TypedJournal[PublishedMessage]):
    """Messages that reached ``MessageBroker.send_message``."""

    _journal_name = "published"

    def __init__(self, messages: list[Message]) -> None:
        self._messages = messages

    def _view(self, message: Message) -> PublishedMessage:
        return PublishedMessage(
            message_name=message.message_name,
            message_id=message.message_id,
            topic=message.topic,
            payload=_payload_view(message.payload),
            payload_bytes=message.payload_bytes,
            content_type=message.content_type,
        )

    def _matches(self, message: Message, event_type: type, topic: str | None) -> bool:
        names = _event_names(event_type)
        payload = message.payload if isinstance(message.payload, dict) else {}
        payload_names = {
            payload.get("event_name"),
            payload.get("eventName"),
            message.message_name,
        }
        if names.isdisjoint(payload_names):
            return False
        if topic is not None and message.topic != topic:
            return False
        return True

    def _matching(self, item_type: type, **filters: typing.Any) -> list[PublishedMessage]:
        topic = filters.get("topic")
        return [self._view(message) for message in self._messages if self._matches(message, item_type, topic)]

    def __iter__(self) -> typing.Iterator[PublishedMessage]:
        return (self._view(message) for message in self._messages)

    def __len__(self) -> int:
        return len(self._messages)


class OutboxJournal(_TypedJournal[typing.Any]):
    """Rows a handler added, and the batch ``produce_available`` publishes."""

    _journal_name = "outbox"

    def __init__(self, repository: typing.Any, broker: typing.Any) -> None:
        self._repository = repository
        self._broker = broker
        self._pending = 0
        self._produced = 0
        self._not_produced = 0
        self._events: list[typing.Any] = []
        self._restorers: list[typing.Callable[[], None]] = []
        if repository is not None:
            self._install()

    def _install(self) -> None:
        repository = self._repository
        original_add = repository.add

        def add(event: typing.Any) -> None:
            original_add(event)
            self._pending += 1
            self._events.append(event)

        repository.add = add
        self._restorers.append(lambda: setattr(repository, "add", original_add))

        original_update = repository.update_status

        async def update_status(outboxed_event_id: int, new_status: EventStatus) -> None:
            await original_update(outboxed_event_id, new_status)
            if new_status is EventStatus.PRODUCED:
                if self._pending:
                    self._pending -= 1
                self._produced += 1
            elif new_status is EventStatus.NOT_PRODUCED:
                self._not_produced += 1

        repository.update_status = update_status
        self._restorers.append(lambda: setattr(repository, "update_status", original_update))

    def restore(self) -> None:
        for restore in reversed(self._restorers):
            restore()
        self._restorers.clear()

    @property
    def pending_count(self) -> int:
        return self._pending

    @property
    def produced_count(self) -> int:
        return self._produced

    @property
    def not_produced_count(self) -> int:
        return self._not_produced

    @property
    def events(self) -> list[typing.Any]:
        return list(self._events)

    def _matching(self, item_type: type, **filters: typing.Any) -> list[typing.Any]:
        return [event for event in self._events if type(event) is item_type]

    def _is_empty(self) -> bool:
        return self._pending == 0 and self._produced == 0 and self._not_produced == 0

    def __iter__(self) -> typing.Iterator[typing.Any]:
        return iter(self._events)

    def __len__(self) -> int:
        return len(self._events)

    def assert_empty(self) -> None:
        if self._is_empty():
            return
        self._raise_report(
            "assert_empty",
            matches=self._pending + self._produced + self._not_produced,
        )

    async def produce_available(self) -> None:
        """Read one batch, publish each row, then commit."""

        if self._repository is None or self._broker is None:
            raise RuntimeError("outbox repository is not configured")
        from cqrs.message_brokers.producer import EventProducer

        producer = EventProducer(self._broker, self._repository)
        batch = await self._repository.get_many()
        for event in batch:
            await producer.send_message(event)
        await self._repository.commit()


class InboxJournal(_JournalQuery[InboxMessage]):
    """Insert and process inbox rows through the real repository and processor."""

    _journal_name = "inbox"

    def __init__(
        self,
        repository: typing.Any,
        message_map: typing.Any,
        processor: typing.Any,
    ) -> None:
        self._repository = repository
        self._map = message_map if message_map is not None else getattr(repository, "_message_map", None)
        self._processor = processor
        self._rows: dict[str, InboxMessage] = {}
        self._ids: dict[int, str] = {}
        self._types: dict[str, type] = {}
        self._restorers: list[typing.Callable[[], None]] = []
        if repository is not None:
            self._install()

    def _install(self) -> None:
        repository = self._repository
        original_add = repository.add

        async def add(
            messages: typing.Sequence[typing.Tuple[str, typing.Any]],
        ) -> list[bool]:
            self._register(messages)
            result = await original_add(messages)
            await self._index_pending()
            return result

        repository.add = add
        self._restorers.append(lambda: setattr(repository, "add", original_add))

        original_update = repository.update_status

        async def update_status(row_id: int, new_status: InboxStatus) -> None:
            await original_update(row_id, new_status)
            message_id = self._ids.get(row_id)
            if message_id is None or message_id not in self._rows:
                return
            row = self._rows[message_id]
            flush_counter = row.flush_counter
            if new_status is InboxStatus.NOT_PROCESSED:
                flush_counter += 1
            self._rows[message_id] = dataclasses.replace(
                row,
                status=new_status,
                flush_counter=flush_counter,
            )

        repository.update_status = update_status
        self._restorers.append(lambda: setattr(repository, "update_status", original_update))

    def restore(self) -> None:
        for restore in reversed(self._restorers):
            restore()
        self._restorers.clear()

    def _register(self, messages: typing.Sequence[typing.Tuple[str, typing.Any]]) -> None:
        for message_id, message in messages:
            self._types[message_id] = type(message)
            if self._map is None:
                continue
            name = message_name(message)
            if self._map.get(name) is None:
                self._map.register(name, type(message))

    async def _index_pending(self) -> None:
        if self._repository is None:
            return
        pending = await self._repository.get_many(batch_size=10_000)
        for row in pending:
            self._rows[row.message_id] = row
            self._ids[row.id] = row.message_id

    async def add(
        self,
        messages: typing.Sequence[typing.Tuple[str, typing.Any]],
    ) -> list[bool]:
        if self._repository is None:
            raise RuntimeError("inbox repository is not configured")
        return await self._repository.add(messages)

    async def process_available(self) -> None:
        """Process the current batch. The caller still owns ``commit``."""

        if self._repository is None or self._processor is None:
            raise RuntimeError("inbox repository is not configured")
        rows = await self._repository.get_many()
        for row in rows:
            self._rows[row.message_id] = row
            self._ids[row.id] = row.message_id
            await self._processor.process(row)

    def one(self, message_id: str) -> InboxMessage:
        if message_id not in self._rows:
            self._raise_report(
                "one",
                looked_for=message_id,
                filters={"message_id": message_id},
                matches=0,
            )
        return self._rows[message_id]

    def _matching(self, item_type: type, **filters: typing.Any) -> list[InboxMessage]:
        names = _event_names(item_type)
        found: list[InboxMessage] = []
        for row in self._rows.values():
            recorded = self._types.get(row.message_id)
            if recorded is item_type or row.message_name in names:
                found.append(row)
        return found

    def __iter__(self) -> typing.Iterator[InboxMessage]:
        return iter(self._rows.values())

    def __len__(self) -> int:
        return len(self._rows)

    @property
    def pending_count(self) -> int:
        return sum(
            1
            for row in self._rows.values()
            if row.status in (InboxStatus.NEW, InboxStatus.NOT_PROCESSED) and row.flush_counter < 5
        )


@dataclasses.dataclass
class MiddlewareCall:
    """One middleware invocation and whether it called ``handle``."""

    middleware: type
    request: typing.Any
    called_next: bool


class MiddlewareJournal(_TypedJournal[MiddlewareCall]):
    """Middleware that saw a request, and whether it called the next handle."""

    _journal_name = "middlewares"

    def __init__(self) -> None:
        self._items: list[MiddlewareCall] = []

    def record(self, middleware_type: type, request: typing.Any, called_next: bool) -> None:
        self._items.append(
            MiddlewareCall(
                middleware=middleware_type,
                request=request,
                called_next=called_next,
            ),
        )

    def _matching(self, item_type: type, **filters: typing.Any) -> list[MiddlewareCall]:
        found = [item for item in self._items if item.middleware is item_type]
        called_next = filters.get("called_next")
        if called_next is not None:
            found = [item for item in found if item.called_next is called_next]
        return found

    def __iter__(self) -> typing.Iterator[MiddlewareCall]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)


@dataclasses.dataclass(frozen=True)
class HandlerErrorRecord:
    event: typing.Any
    handler: type
    error: BaseException


class HandlerErrorJournal(_Reported):
    """Domain handler errors that ``send`` logged and swallowed."""

    _journal_name = "handler_errors"

    def __init__(self) -> None:
        self._items: list[HandlerErrorRecord] = []

    def add(self, event: typing.Any, handler: type, error: BaseException) -> None:
        self._items.append(HandlerErrorRecord(event=event, handler=handler, error=error))

    def of(
        self,
        event_type: type,
        handler_type: type,
        *,
        where: typing.Callable[[HandlerErrorRecord], bool] | None = None,
    ) -> list[HandlerErrorRecord]:
        found = [item for item in self._items if type(item.event) is event_type and item.handler is handler_type]
        return _apply_where(found, where)

    def contains(
        self,
        event_type: type,
        handler_type: type,
        *,
        where: typing.Callable[[HandlerErrorRecord], bool] | None = None,
    ) -> bool:
        return bool(self.of(event_type, handler_type, where=where))

    def one(
        self,
        event_type: type,
        handler_type: type,
        *,
        where: typing.Callable[[HandlerErrorRecord], bool] | None = None,
    ) -> HandlerErrorRecord:
        matches = self.of(event_type, handler_type, where=where)
        if len(matches) != 1:
            self._fail_pair("one", event_type, handler_type, matches, where)
        return matches[0]

    def assert_contains(
        self,
        event_type: type,
        handler_type: type,
        *,
        where: typing.Callable[[HandlerErrorRecord], bool] | None = None,
    ) -> None:
        matches = self.of(event_type, handler_type, where=where)
        if matches:
            return
        self._fail_pair("assert_contains", event_type, handler_type, matches, where)

    def assert_one(
        self,
        event_type: type,
        handler_type: type,
        *,
        where: typing.Callable[[HandlerErrorRecord], bool] | None = None,
    ) -> None:
        matches = self.of(event_type, handler_type, where=where)
        if len(matches) == 1:
            return
        self._fail_pair("assert_one", event_type, handler_type, matches, where)

    def assert_not_contains(
        self,
        event_type: type,
        handler_type: type,
        *,
        where: typing.Callable[[HandlerErrorRecord], bool] | None = None,
    ) -> None:
        matches = self.of(event_type, handler_type, where=where)
        if not matches:
            return
        self._fail_pair("assert_not_contains", event_type, handler_type, matches, where)

    def assert_empty(self) -> None:
        if not self._items:
            return
        self._raise_report("assert_empty", matches=len(self._items))

    def _fail_pair(
        self,
        method: str,
        event_type: type,
        handler_type: type,
        matches: list[HandlerErrorRecord],
        where: typing.Callable[[HandlerErrorRecord], bool] | None,
    ) -> typing.NoReturn:
        self._raise_report(
            method,
            looked_for=(event_type, handler_type),
            has_where=where is not None,
            matches=len(matches),
        )

    def __iter__(self) -> typing.Iterator[HandlerErrorRecord]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)


class _HarnessListener:
    def __init__(self, errors: HandlerErrorJournal, unhandled: EventJournal) -> None:
        self._errors = errors
        self._unhandled = unhandled

    def on_domain_handler_error(
        self,
        event: typing.Any,
        handler: type,
        error: BaseException,
    ) -> None:
        self._errors.add(event, handler, error)

    def on_unhandled(self, event: typing.Any) -> None:
        self._unhandled.add(event)


class _ChainedListener:
    def __init__(self, harness: _HarnessListener, previous: EventEmitterListener | None) -> None:
        self._harness = harness
        self._previous = previous

    def on_domain_handler_error(
        self,
        event: typing.Any,
        handler: type,
        error: BaseException,
    ) -> None:
        self._harness.on_domain_handler_error(event, handler, error)
        previous = self._previous
        if previous is not None:
            previous.on_domain_handler_error(event, handler, error)

    def on_unhandled(self, event: typing.Any) -> None:
        self._harness.on_unhandled(event)
        previous = self._previous
        if previous is not None:
            previous.on_unhandled(event)


@dataclasses.dataclass
class _SagaState:
    saga_id: uuid.UUID
    name: str
    status: SagaStatus
    context: dict[str, typing.Any]
    history: list[tuple[str, str, SagaStepStatus]]


class _SagaCache:
    def __init__(self) -> None:
        self.by_id: dict[uuid.UUID, _SagaState] = {}

    def create(self, saga_id: uuid.UUID, name: str, context: dict[str, typing.Any]) -> None:
        self.by_id[saga_id] = _SagaState(
            saga_id=saga_id,
            name=name,
            status=SagaStatus.PENDING,
            context=dict(context),
            history=[],
        )

    def set_status(self, saga_id: uuid.UUID, status: SagaStatus) -> None:
        state = self.by_id.get(saga_id)
        if state is None:
            self.by_id[saga_id] = _SagaState(
                saga_id=saga_id,
                name="",
                status=status,
                context={},
                history=[],
            )
            return
        state.status = status

    def log(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: str,
        status: SagaStepStatus,
    ) -> None:
        state = self.by_id.get(saga_id)
        if state is None:
            return
        state.history.append((step_name, action, status))


class SagaSnapshot:
    """Status, context, and step history for one saga id."""

    def __init__(self, state: _SagaState) -> None:
        self.saga_id = state.saga_id
        self.name = state.name
        self.status = state.status
        self.context = state.context
        self._history = list(state.history)

    @property
    def completed_steps(self) -> list[str]:
        names: list[str] = []
        seen: set[str] = set()
        for step_name, action, status in self._history:
            if action == "act" and status is SagaStepStatus.COMPLETED and step_name not in seen:
                seen.add(step_name)
                names.append(step_name)
        return names

    def step_status(self, step_name: str) -> SagaStepStatus:
        matches = [entry for entry in self._history if entry[0] == step_name]
        if not matches:
            raise AssertionError(f"saga {self.name} has no step {step_name}")
        _, action, status = matches[-1]
        if action == "compensate" and status is SagaStepStatus.COMPLETED:
            return SagaStepStatus.COMPENSATED
        return status


class SagaJournal(_TypedJournal[SagaSnapshot]):
    """Saga status read from storage calls made during the block."""

    _journal_name = "sagas"

    def __init__(
        self,
        storage: typing.Any,
        container: typing.Any,
        scope_strategy: typing.Any,
    ) -> None:
        self._storage = storage
        self._container = container
        self._scope_strategy = scope_strategy
        self._cache = _SagaCache()
        self._restorers: list[typing.Callable[[], None]] = []
        if storage is not None:
            self._install(storage)
            self._restorers.extend(_watch_create_run(storage, self._cache))

    def restore(self) -> None:
        for restore in reversed(self._restorers):
            restore()
        self._restorers.clear()

    def _install(self, target: typing.Any) -> None:
        self._restorers.append(_patch(target, "create_saga", _wrap_create(target.create_saga, self._cache)))
        self._restorers.append(_patch(target, "update_status", _wrap_status(target.update_status, self._cache)))
        self._restorers.append(_patch(target, "log_step", _wrap_log(target.log_step, self._cache)))

    @property
    def running_count(self) -> int:
        return sum(1 for state in self._cache.by_id.values() if state.status is SagaStatus.RUNNING)

    @property
    def completed_count(self) -> int:
        return sum(1 for state in self._cache.by_id.values() if state.status is SagaStatus.COMPLETED)

    def _matching(self, item_type: type, **filters: typing.Any) -> list[SagaSnapshot]:
        name = item_type.__name__
        return [SagaSnapshot(state) for state in self._cache.by_id.values() if state.name == name]

    def __iter__(self) -> typing.Iterator[SagaSnapshot]:
        return (SagaSnapshot(state) for state in self._cache.by_id.values())

    def __len__(self) -> int:
        return len(self._cache.by_id)

    async def recover(
        self,
        saga: typing.Any,
        saga_id: uuid.UUID,
        context_builder: typing.Any,
    ) -> None:
        """Resume ``saga_id`` with ``cqrs.saga.recovery.recover_saga``."""

        if self._storage is None or self._container is None:
            raise RuntimeError("saga storage is not configured")
        await recover_saga(
            saga,
            saga_id,
            context_builder,
            container=self._container,
            storage=self._storage,
            scope_strategy=self._scope_strategy,
        )


def _patch(target: typing.Any, name: str, new: typing.Callable[..., typing.Any]) -> typing.Callable[[], None]:
    previous = getattr(target, name)
    setattr(target, name, new)

    def restore() -> None:
        setattr(target, name, previous)

    return restore


def _wrap_create(
    original: typing.Callable[..., typing.Awaitable[None]],
    cache: _SagaCache,
) -> typing.Callable[..., typing.Awaitable[None]]:
    async def wrapped(saga_id: uuid.UUID, name: str, context: dict[str, typing.Any]) -> None:
        if _SAGA_DEPTH.get():
            await original(saga_id, name, context)
            return
        token = _SAGA_DEPTH.set(_SAGA_DEPTH.get() + 1)
        try:
            await original(saga_id, name, context)
            cache.create(saga_id, name, context)
        finally:
            _SAGA_DEPTH.reset(token)

    return wrapped


def _wrap_status(
    original: typing.Callable[..., typing.Awaitable[None]],
    cache: _SagaCache,
) -> typing.Callable[..., typing.Awaitable[None]]:
    async def wrapped(saga_id: uuid.UUID, status: SagaStatus) -> None:
        if _SAGA_DEPTH.get():
            await original(saga_id, status)
            return
        token = _SAGA_DEPTH.set(_SAGA_DEPTH.get() + 1)
        try:
            await original(saga_id, status)
            cache.set_status(saga_id, status)
        finally:
            _SAGA_DEPTH.reset(token)

    return wrapped


def _wrap_log(
    original: typing.Callable[..., typing.Awaitable[None]],
    cache: _SagaCache,
) -> typing.Callable[..., typing.Awaitable[None]]:
    async def wrapped(
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: str | None = None,
    ) -> None:
        if _SAGA_DEPTH.get():
            await original(saga_id, step_name, action, status, details)
            return
        token = _SAGA_DEPTH.set(_SAGA_DEPTH.get() + 1)
        try:
            await original(saga_id, step_name, action, status, details)
            cache.log(saga_id, step_name, action, status)
        finally:
            _SAGA_DEPTH.reset(token)

    return wrapped


def _watch_create_run(storage: typing.Any, cache: _SagaCache) -> list[typing.Callable[[], None]]:
    if not hasattr(storage, "create_run"):
        return []
    original_create_run = storage.create_run

    def create_run() -> typing.AsyncContextManager[typing.Any]:
        @contextlib.asynccontextmanager
        async def _cm() -> typing.AsyncIterator[typing.Any]:
            async with original_create_run() as run:
                restorers = [
                    _patch(run, "create_saga", _wrap_create(run.create_saga, cache)),
                    _patch(run, "update_status", _wrap_status(run.update_status, cache)),
                    _patch(run, "log_step", _wrap_log(run.log_step, cache)),
                ]
                try:
                    yield run
                finally:
                    for restore in reversed(restorers):
                        restore()

        return _cm()

    storage.create_run = create_run
    return [lambda: setattr(storage, "create_run", original_create_run)]


def wrap_send(mediator: typing.Any, journal: RequestJournal) -> typing.Callable[[], None]:
    original = mediator.send

    async def send(request: typing.Any) -> typing.Any:
        try:
            response = await original(request)
        except Exception as exc:
            journal.record(request, exception=exc)
            raise
        else:
            journal.record(request, response=response)
            return response

    mediator.send = send
    return lambda: setattr(mediator, "send", original)


def wrap_stream(mediator: typing.Any, yields: list[typing.Any]) -> typing.Callable[[], None]:
    original = mediator.stream

    def stream(request: typing.Any) -> typing.AsyncIterator[typing.Any]:
        return _iterate(original(request))

    async def _iterate(source: typing.AsyncIterator[typing.Any]) -> typing.AsyncIterator[typing.Any]:
        async for item in source:
            yields.append(item)
            yield item

    mediator.stream = stream
    return lambda: setattr(mediator, "stream", original)


def wrap_emit(emitter: typing.Any, handled: EventJournal) -> typing.Callable[[], None]:
    from cqrs.models.event import IDomainEvent

    original = emitter.emit

    async def emit(event: typing.Any) -> typing.Any:
        if isinstance(event, IDomainEvent):
            handlers = emitter._event_map.get(type(event), [])
            if handlers:
                handled.add(event)
        return await original(event)

    emitter.emit = emit
    return lambda: setattr(emitter, "emit", original)


def wrap_middleware(middleware: typing.Any, journal: MiddlewareJournal) -> typing.Callable[[], None]:
    current = middleware.__call__
    if getattr(current, _RECORDING, False):
        return lambda: None

    async def __call__(request: typing.Any, handle: typing.Callable[..., typing.Any]) -> typing.Any:
        called = False

        async def tracking(req: typing.Any) -> typing.Any:
            nonlocal called
            called = True
            return await handle(req)

        try:
            return await current(request, tracking)
        finally:
            journal.record(type(middleware), request, called)

    setattr(__call__, _RECORDING, True)
    middleware.__call__ = __call__
    return lambda: setattr(middleware, "__call__", current)


def listener_for(
    errors: HandlerErrorJournal,
    unhandled: EventJournal,
    previous: EventEmitterListener | None,
) -> _ChainedListener:
    return _ChainedListener(_HarnessListener(errors, unhandled), previous)
