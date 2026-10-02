import logging
import uuid
from unittest import mock

import orjson
import pydantic
import pytest

import cqrs
from cqrs.outbox import repository as outbox_repository
from cqrs.outbox.mock import MockOutboxedEventRepository
from cqrs.outbox.sqlalchemy import SqlAlchemyOutboxedEventRepository
from cqrs.serializers.json import JsonEventSerializer
from tests.fixtures.proto_fixtures import (
    UserJoinedNotificationEvent,
    make_protobuf_codec,
    make_user_joined_event,
)


class JsonPayload(pydantic.BaseModel, frozen=True):
    foo: str


class JsonNotification(cqrs.PydanticNotificationEvent[JsonPayload], frozen=True):
    event_name: str = "json_mixed_event"


def test_outboxed_event_four_arg_constructor():
    event = cqrs.NotificationEvent[dict](event_name="e", payload={})

    outboxed = outbox_repository.OutboxedEvent(
        1,
        event,
        "topic",
        outbox_repository.EventStatus.NEW,
    )

    assert outboxed.payload_bytes is None
    assert outboxed.content_type is None
    assert outboxed.event is event


def test_isolated_maps_do_not_share_registrations():
    name = f"isolated_{uuid.uuid4().hex}"
    map_a = cqrs.OutboxedEventMap()
    map_b = cqrs.OutboxedEventMap()
    event_type = cqrs.NotificationEvent[JsonPayload]

    map_a.register(name, event_type)

    assert map_a.get(name) is event_type
    assert map_b.get(name) is None
    assert cqrs.OutboxedEventMap.get(name) is None


def test_class_level_register_is_global():
    name = f"global_{uuid.uuid4().hex}"
    event_type = cqrs.NotificationEvent[JsonPayload]

    cqrs.OutboxedEventMap.register(name, event_type)
    try:
        assert cqrs.OutboxedEventMap.get(name) is event_type
        isolated = cqrs.OutboxedEventMap()
        assert isolated.get(name) is None
    finally:
        cqrs.OutboxedEventMap._singleton()._entries.pop(name, None)


def test_register_duplicate_raises_key_error():
    events = cqrs.OutboxedEventMap()
    events.register("dup", cqrs.NotificationEvent[JsonPayload])

    with pytest.raises(KeyError, match="already registered"):
        events.register("dup", cqrs.NotificationEvent[JsonPayload])


def test_get_serializer_per_event_codec():
    events = cqrs.OutboxedEventMap()
    proto = make_protobuf_codec()
    events.register("user_joined", UserJoinedNotificationEvent, serializer=proto)

    assert events.get_serializer("user_joined") is proto
    assert events.get_serializer("missing") is None


def test_as_serializer_uses_per_event_codec_and_default():
    events = cqrs.OutboxedEventMap()
    proto = make_protobuf_codec()
    json_codec = JsonEventSerializer()
    events.register("user_joined", UserJoinedNotificationEvent, serializer=proto)
    events.register("json_mixed_event", JsonNotification)

    dispatcher = events.as_serializer(default=json_codec)
    proto_event = make_user_joined_event()
    json_event = JsonNotification(
        event_name="json_mixed_event",
        payload=JsonPayload(foo="bar"),
    )

    assert dispatcher.serialize(proto_event) == proto.serialize(proto_event)
    assert dispatcher.serialize(json_event) == json_codec.serialize(json_event)
    assert dispatcher.content_type_for(proto_event) == "application/x-protobuf"
    assert dispatcher.content_type_for(json_event) is None


def test_sqlalchemy_add_without_protobuf_type_raises():
    events = cqrs.OutboxedEventMap()
    events.register(
        "user_joined",
        UserJoinedNotificationEvent,
        serializer=cqrs.ProtobufEventSerializer({}),
    )
    repository = SqlAlchemyOutboxedEventRepository(
        session=mock.Mock(),
        event_map=events,
    )

    with pytest.raises(TypeError, match="No protobuf message type registered"):
        repository.add(make_user_joined_event())


def test_process_events_skips_and_logs_deserialize_errors(caplog):
    events = cqrs.OutboxedEventMap()
    events.register(
        "user_joined",
        UserJoinedNotificationEvent,
        serializer=make_protobuf_codec(),
    )
    repository = SqlAlchemyOutboxedEventRepository(
        session=mock.Mock(),
        event_map=events,
    )

    class FakeRow:
        def row_to_dict(self):
            return {
                "id": 42,
                "event_name": "user_joined",
                "topic": "t",
                "event_status": outbox_repository.EventStatus.NEW,
                "payload": b"not-a-protobuf-payload",
            }

    with caplog.at_level(logging.WARNING):
        result = repository._process_events(FakeRow())  # type: ignore[arg-type]

    assert result is None
    assert "user_joined" in caplog.text
    assert "42" in caplog.text
    assert "ProtobufEventSerializer" in caplog.text


def test_process_events_unknown_name_skips():
    repository = SqlAlchemyOutboxedEventRepository(
        session=mock.Mock(),
        event_map=cqrs.OutboxedEventMap(),
    )

    class FakeRow:
        def row_to_dict(self):
            return {
                "id": 1,
                "event_name": "never_registered",
                "topic": "t",
                "event_status": outbox_repository.EventStatus.NEW,
                "payload": b"{}",
            }

    assert repository._process_events(FakeRow()) is None  # type: ignore[arg-type]


async def _get_many_with_rows(
    repository: SqlAlchemyOutboxedEventRepository,
    rows: list,
) -> tuple[list[outbox_repository.OutboxedEvent], mock.AsyncMock]:
    scalars_result = mock.Mock()
    scalars_result.all.return_value = rows
    execute_result = mock.Mock()
    execute_result.scalars.return_value = scalars_result
    repository.session.execute = mock.AsyncMock(return_value=execute_result)
    update_status = mock.AsyncMock()
    repository.update_status = update_status
    result = await repository.get_many(batch_size=len(rows) or 1)
    return result, update_status


@pytest.mark.asyncio
async def test_get_many_marks_undecodable_as_not_produced(caplog):
    events = cqrs.OutboxedEventMap()
    events.register(
        "user_joined",
        UserJoinedNotificationEvent,
        serializer=make_protobuf_codec(),
    )
    repository = SqlAlchemyOutboxedEventRepository(
        session=mock.Mock(),
        event_map=events,
    )
    row = mock.Mock()
    row.id = 42
    row.row_to_dict.return_value = {
        "id": 42,
        "event_name": "user_joined",
        "topic": "t",
        "event_status": outbox_repository.EventStatus.NEW,
        "payload": b"not-a-protobuf-payload",
    }

    with caplog.at_level(logging.WARNING):
        result, update_status = await _get_many_with_rows(repository, [row])

    assert result == []
    update_status.assert_awaited_once_with(
        42,
        outbox_repository.EventStatus.NOT_PRODUCED,
    )
    assert "user_joined" in caplog.text


@pytest.mark.asyncio
async def test_get_many_marks_unknown_name_as_not_produced():
    repository = SqlAlchemyOutboxedEventRepository(
        session=mock.Mock(),
        event_map=cqrs.OutboxedEventMap(),
    )
    row = mock.Mock()
    row.id = 7
    row.row_to_dict.return_value = {
        "id": 7,
        "event_name": "never_registered",
        "topic": "t",
        "event_status": outbox_repository.EventStatus.NEW,
        "payload": b"{}",
    }

    result, update_status = await _get_many_with_rows(repository, [row])

    assert result == []
    update_status.assert_awaited_once_with(
        7,
        outbox_repository.EventStatus.NOT_PRODUCED,
    )


@pytest.mark.asyncio
async def test_get_many_keeps_decodable_and_marks_only_failures():
    events = cqrs.OutboxedEventMap()
    proto = make_protobuf_codec()
    events.register("user_joined", UserJoinedNotificationEvent, serializer=proto)
    events.register("json_mixed_event", JsonNotification)
    repository = SqlAlchemyOutboxedEventRepository(
        session=mock.Mock(),
        event_map=events,
    )
    good_event = JsonNotification(
        event_name="json_mixed_event",
        payload=JsonPayload(foo="ok"),
    )
    good = mock.Mock()
    good.id = 1
    good.row_to_dict.return_value = {
        "id": 1,
        "event_name": "json_mixed_event",
        "topic": good_event.topic,
        "event_status": outbox_repository.EventStatus.NEW,
        "payload": JsonEventSerializer().serialize(good_event),
    }
    bad = mock.Mock()
    bad.id = 2
    bad.row_to_dict.return_value = {
        "id": 2,
        "event_name": "user_joined",
        "topic": "t",
        "event_status": outbox_repository.EventStatus.NEW,
        "payload": b"broken",
    }

    result, update_status = await _get_many_with_rows(repository, [good, bad])

    assert len(result) == 1
    assert result[0].id == 1
    update_status.assert_awaited_once_with(
        2,
        outbox_repository.EventStatus.NOT_PRODUCED,
    )


def test_mock_repository_sets_payload_bytes():
    storage: dict = {}
    repository = MockOutboxedEventRepository(session_factory=lambda: storage)
    event = cqrs.PydanticNotificationEvent[JsonPayload](
        event_name="json_event",
        payload=JsonPayload(foo="bar"),
    )

    repository.add(event)

    stored = next(iter(storage.values()))
    assert stored.payload_bytes == orjson.dumps(event.to_dict())
    assert stored.content_type is None
    assert stored.event is event


def test_mock_repository_sets_protobuf_content_type():
    events = cqrs.OutboxedEventMap()
    codec = make_protobuf_codec()
    events.register("user_joined", UserJoinedNotificationEvent, serializer=codec)
    storage: dict = {}
    repository = MockOutboxedEventRepository(
        session_factory=lambda: storage,
        event_map=events,
    )
    event = make_user_joined_event()

    repository.add(event)

    stored = next(iter(storage.values()))
    assert stored.payload_bytes == codec.serialize(event)
    assert stored.content_type == "application/x-protobuf"


def test_as_serializer_default_proto():
    events = cqrs.OutboxedEventMap()
    proto = make_protobuf_codec()
    event = make_user_joined_event()

    dispatcher = events.as_serializer(default=proto)

    assert dispatcher.serialize(event) == proto.serialize(event)


def test_mixed_outbox_isolated_map_deserializes_per_event_codec():
    events = cqrs.OutboxedEventMap()
    proto = make_protobuf_codec()
    json_codec = JsonEventSerializer()
    events.register("user_joined", UserJoinedNotificationEvent, serializer=proto)
    events.register("json_mixed_event", JsonNotification)
    repository = SqlAlchemyOutboxedEventRepository(
        session=mock.Mock(),
        event_map=events,
    )
    proto_event = make_user_joined_event()
    json_event = JsonNotification(
        event_name="json_mixed_event",
        payload=JsonPayload(foo="bar"),
    )

    class ProtoRow:
        def row_to_dict(self):
            return {
                "id": 1,
                "event_name": "user_joined",
                "topic": proto_event.topic,
                "event_status": outbox_repository.EventStatus.NEW,
                "payload": proto.serialize(proto_event),
            }

    class JsonRow:
        def row_to_dict(self):
            return {
                "id": 2,
                "event_name": "json_mixed_event",
                "topic": json_event.topic,
                "event_status": outbox_repository.EventStatus.NEW,
                "payload": json_codec.serialize(json_event),
            }

    proto_outboxed = repository._process_events(ProtoRow())  # type: ignore[arg-type]
    json_outboxed = repository._process_events(JsonRow())  # type: ignore[arg-type]

    assert proto_outboxed is not None
    assert json_outboxed is not None
    assert proto_outboxed.event.event_id == proto_event.event_id
    assert proto_outboxed.payload_bytes == proto.serialize(proto_event)
    assert proto_outboxed.content_type == "application/x-protobuf"
    assert json_outboxed.event.payload.foo == "bar"
    assert json_outboxed.payload_bytes == json_codec.serialize(json_event)
    assert json_outboxed.content_type is None
