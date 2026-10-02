import uuid
from unittest import mock

import aio_pika
import pydantic

import cqrs
from cqrs.events import EventEmitter, EventMap
from cqrs.message_brokers import amqp, kafka
from cqrs.message_brokers.protocol import Message, MessageBroker
from cqrs.outbox import repository as outbox_repository
from cqrs.outbox.mock import MockOutboxedEventRepository
from cqrs.message_brokers.producer import EventProducer
from cqrs.serializers.json import JsonEventSerializer
from tests.fixtures.proto_fixtures import (
    make_protobuf_codec,
    make_user_joined_event,
)


class CapturingBroker(MessageBroker):
    def __init__(self) -> None:
        self.messages: list[Message] = []

    async def send_message(self, message: Message) -> None:
        self.messages.append(message)


class JsonPayload(pydantic.BaseModel, frozen=True):
    foo: str


class EmptyContainer:
    async def resolve(self, type_):
        return type_()


def _json_event() -> cqrs.NotificationEvent[JsonPayload]:
    return cqrs.NotificationEvent[JsonPayload](
        event_name="json_event",
        topic="json_topic",
        payload=JsonPayload(foo="bar"),
        event_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
    )


async def test_event_emitter_json_path_keeps_payload_dict_and_class_message_name():
    broker = CapturingBroker()
    emitter = EventEmitter(
        event_map=EventMap(),
        container=EmptyContainer(),  # type: ignore[arg-type]
        message_broker=broker,
    )
    event = _json_event()

    await emitter.emit(event)

    message = broker.messages[0]
    assert message.message_name == type(event).__name__
    assert message.payload == event.to_dict()
    assert isinstance(message.payload, dict)
    assert message.payload_bytes == JsonEventSerializer().serialize(event)
    assert message.content_type is None
    assert message.headers is None
    encoded = message.to_dict()
    assert "messageName" in encoded
    assert "messageId" in encoded
    assert "payload_bytes" not in encoded
    assert "content_type" not in encoded
    assert "headers" not in encoded
    assert "payloadBytes" not in encoded
    assert "contentType" not in encoded


async def test_event_emitter_protobuf_sets_headers_and_keeps_payload_dict():
    broker = CapturingBroker()
    codec = make_protobuf_codec()
    emitter = EventEmitter(
        event_map=EventMap(),
        container=EmptyContainer(),  # type: ignore[arg-type]
        message_broker=broker,
        serializer=codec,
    )
    event = make_user_joined_event()

    await emitter.emit(event)

    message = broker.messages[0]
    assert message.message_name == type(event).__name__
    assert message.payload == event.to_dict()
    assert isinstance(message.payload, dict)
    assert message.payload_bytes == codec.serialize(event)
    assert message.content_type == "application/x-protobuf"
    assert message.headers == {
        "event_name": event.event_name,
        "message_id": str(event.event_id),
    }
    encoded = message.to_dict()
    assert encoded["content_type"] == "application/x-protobuf"
    assert encoded["headers"]["event_name"] == event.event_name
    assert "payload_bytes" in encoded
    assert encoded["payload_bytes_encoding"] == "base64"
    assert "payloadBytes" not in encoded
    assert "contentType" not in encoded
    assert "messageName" in encoded
    assert "messageId" in encoded


async def test_producer_puts_dict_payload_and_outboxed_payload_bytes():
    broker = CapturingBroker()
    storage: dict = {}
    repository = MockOutboxedEventRepository(session_factory=lambda: storage)
    event = _json_event()
    repository.add(event)
    producer = EventProducer(broker, repository)
    outboxed = next(iter(storage.values()))

    await producer.send_message(outboxed)

    message = broker.messages[0]
    assert message.message_name == event.event_name
    assert message.payload == event.to_dict()
    assert message.payload_bytes == outboxed.payload_bytes
    assert message.content_type is None
    assert message.headers is None


async def test_producer_protobuf_headers_from_repository_codec():
    broker = CapturingBroker()
    events = cqrs.OutboxedEventMap()
    codec = make_protobuf_codec()
    events.register("user_joined", type(make_user_joined_event()), serializer=codec)
    storage: dict = {}
    repository = MockOutboxedEventRepository(
        session_factory=lambda: storage,
        event_map=events,
        serializer=codec,
    )
    event = make_user_joined_event()
    repository.add(event)
    producer = EventProducer(broker, repository)

    await producer.send_message(next(iter(storage.values())))

    message = broker.messages[0]
    assert message.payload == event.to_dict()
    assert message.payload_bytes == codec.serialize(event)
    assert message.content_type == "application/x-protobuf"
    assert message.headers == {
        "event_name": event.event_name,
        "message_id": str(event.event_id),
    }


class DummyRepo(outbox_repository.OutboxedEventRepository):
    def add(self, event) -> None:
        pass

    async def get_many(self, batch_size: int = 100, topic=None):
        return []

    async def update_status(self, *args, **kwargs):
        pass

    async def commit(self):
        pass

    async def rollback(self):
        pass


async def test_producer_none_payload_bytes_falls_back_to_payload_dict():
    broker = CapturingBroker()
    event = _json_event()
    outboxed = outbox_repository.OutboxedEvent(
        id=1,
        event=event,
        topic=event.topic,
        status=outbox_repository.EventStatus.NEW,
    )
    producer = EventProducer(broker, DummyRepo())

    await producer.send_message(outboxed)

    message = broker.messages[0]
    assert message.payload == event.to_dict()
    assert message.payload_bytes is None


async def test_producer_uses_outboxed_content_type_without_repository_codec():
    broker = CapturingBroker()
    event = make_user_joined_event()
    payload_bytes = make_protobuf_codec().serialize(event)
    outboxed = outbox_repository.OutboxedEvent(
        id=1,
        event=event,
        topic=event.topic,
        status=outbox_repository.EventStatus.NEW,
        payload_bytes=payload_bytes,
        content_type="application/x-protobuf",
    )
    producer = EventProducer(broker, DummyRepo())

    await producer.send_message(outboxed)

    message = broker.messages[0]
    assert message.payload_bytes == payload_bytes
    assert message.content_type == "application/x-protobuf"
    assert message.headers == {
        "event_name": event.event_name,
        "message_id": str(event.event_id),
    }


async def test_kafka_broker_json_sends_bytes_without_headers():
    producer = mock.AsyncMock()
    producer.produce = mock.AsyncMock()
    broker = kafka.KafkaMessageBroker(producer)
    event = _json_event()
    payload_bytes = JsonEventSerializer().serialize(event)
    message = Message(
        message_name=type(event).__name__,
        topic=event.topic,
        payload=event.to_dict(),
        payload_bytes=payload_bytes,
    )

    await broker.send_message(message)

    producer.produce.assert_awaited_once_with(event.topic, payload_bytes)


async def test_kafka_broker_legacy_value_serializer_forwards_payload_dict():
    producer = mock.AsyncMock()
    producer.produce = mock.AsyncMock()
    producer.legacy_value_serializer = True
    broker = kafka.KafkaMessageBroker(producer)
    event = _json_event()
    payload = event.to_dict()
    message = Message(
        message_name=type(event).__name__,
        topic=event.topic,
        payload=payload,
    )

    await broker.send_message(message)

    producer.produce.assert_awaited_once_with(event.topic, payload)


async def test_kafka_broker_legacy_value_serializer_keeps_outbox_bytes():
    producer = mock.AsyncMock()
    producer.produce = mock.AsyncMock()
    producer.legacy_value_serializer = True
    broker = kafka.KafkaMessageBroker(producer)
    event = _json_event()
    payload_bytes = JsonEventSerializer().serialize(event)
    message = Message(
        message_name=type(event).__name__,
        topic=event.topic,
        payload=event.to_dict(),
        payload_bytes=payload_bytes,
    )

    await broker.send_message(message)

    producer.produce.assert_awaited_once_with(event.topic, payload_bytes)


async def test_kafka_broker_legacy_value_serializer_keeps_protobuf_bytes():
    producer = mock.AsyncMock()
    producer.produce = mock.AsyncMock()
    producer.legacy_value_serializer = True
    broker = kafka.KafkaMessageBroker(producer)
    event = make_user_joined_event()
    payload_bytes = make_protobuf_codec().serialize(event)
    headers = {"event_name": event.event_name, "message_id": str(event.event_id)}
    message = Message(
        message_name=type(event).__name__,
        topic=event.topic,
        payload=event.to_dict(),
        payload_bytes=payload_bytes,
        content_type="application/x-protobuf",
        headers=headers,
    )

    await broker.send_message(message)

    producer.produce.assert_awaited_once_with(
        event.topic,
        payload_bytes,
        headers=headers,
    )


async def test_kafka_broker_protobuf_passes_headers():
    producer = mock.AsyncMock()
    producer.produce = mock.AsyncMock()
    broker = kafka.KafkaMessageBroker(producer)
    event = make_user_joined_event()
    payload_bytes = make_protobuf_codec().serialize(event)
    headers = {"event_name": event.event_name, "message_id": str(event.event_id)}
    message = Message(
        message_name=type(event).__name__,
        topic=event.topic,
        payload=event.to_dict(),
        payload_bytes=payload_bytes,
        content_type="application/x-protobuf",
        headers=headers,
    )

    await broker.send_message(message)

    producer.produce.assert_awaited_once_with(
        event.topic,
        payload_bytes,
        headers=headers,
    )


async def test_kafka_broker_stub_produce_accepts_headers():
    calls: list[tuple] = []

    class StubProducer:
        async def produce(self, topic, message, headers=None):
            calls.append((topic, message, headers))

    broker = kafka.KafkaMessageBroker(StubProducer())  # type: ignore[arg-type]
    event = make_user_joined_event()
    payload_bytes = make_protobuf_codec().serialize(event)
    headers = {"event_name": event.event_name, "message_id": str(event.event_id)}
    message = Message(
        message_name=type(event).__name__,
        topic=event.topic,
        payload=event.to_dict(),
        payload_bytes=payload_bytes,
        content_type="application/x-protobuf",
        headers=headers,
    )

    await broker.send_message(message)

    assert calls == [(event.topic, payload_bytes, headers)]


async def test_amqp_broker_json_has_no_content_type_or_headers():
    publisher = mock.AsyncMock()
    broker = amqp.AMQPMessageBroker(publisher, exchange_name="ex")
    event = _json_event()
    payload_bytes = JsonEventSerializer().serialize(event)
    message = Message(
        message_name=type(event).__name__,
        topic=event.topic,
        payload=event.to_dict(),
        payload_bytes=payload_bytes,
    )

    await broker.send_message(message)

    published = publisher.publish.await_args.kwargs["message"]
    assert isinstance(published, aio_pika.Message)
    assert published.body == payload_bytes
    assert published.content_type is None
    assert not published.headers


async def test_amqp_broker_protobuf_sets_content_type_and_headers():
    publisher = mock.AsyncMock()
    broker = amqp.AMQPMessageBroker(publisher, exchange_name="ex")
    event = make_user_joined_event()
    payload_bytes = make_protobuf_codec().serialize(event)
    headers = {"event_name": event.event_name, "message_id": str(event.event_id)}
    message = Message(
        message_name=type(event).__name__,
        topic=event.topic,
        payload=event.to_dict(),
        payload_bytes=payload_bytes,
        content_type="application/x-protobuf",
        headers=headers,
    )

    await broker.send_message(message)

    published = publisher.publish.await_args.kwargs["message"]
    assert published.body == payload_bytes
    assert published.content_type == "application/x-protobuf"
    assert published.headers["event_name"] == event.event_name
