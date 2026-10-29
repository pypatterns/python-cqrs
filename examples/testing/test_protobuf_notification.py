"""
Protobuf notification. The harness does not import protobuf.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_protobuf_notification.py
"""

import importlib.util
from pathlib import Path

import di
import pytest

import cqrs
from cqrs.testing import test_harness

pytestmark = pytest.mark.protobuf


def _load_user_joined_pb2():
    path = Path(__file__).resolve().parents[1] / "proto" / "user_joined_pb2.py"
    spec = importlib.util.spec_from_file_location("user_joined_pb2", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


user_joined_pb2 = _load_user_joined_pb2()


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreatedNotice(cqrs.NotificationEvent):
    event_name: str = "order_created"
    topic: str = "order_events"

    def proto(self):
        message = user_joined_pb2.UserJoinedNotification()
        message.event_id = str(self.event_id)
        message.event_timestamp = self.event_timestamp.isoformat()
        message.event_name = self.event_name
        message.payload.user_id = "1"
        message.payload.meeting_id = "1"
        return message


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder):
        self._events.append(OrderCreatedNotice(payload={"order_id": "1"}))
        return CreatedOrder(order_id="1")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


async def test_notification_is_published_as_protobuf() -> None:
    container = di.Container()
    serializer = cqrs.ProtobufEventSerializer(
        {OrderCreatedNotice: user_joined_pb2.UserJoinedNotification},
    )
    async with test_harness(
        container,
        commands_mapper=commands_mapper,
        serializer=serializer,
    ) as harness:
        await harness.mediator.send(CreateOrder(sku="book"))

        message = harness.published.one(OrderCreatedNotice)
        assert message.content_type == "application/x-protobuf"
        assert message.payload_bytes
