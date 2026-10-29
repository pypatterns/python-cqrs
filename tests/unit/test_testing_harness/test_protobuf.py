import importlib.util
from pathlib import Path

import pytest

import cqrs
from cqrs.testing import TestContainer, test_harness

pytestmark = pytest.mark.protobuf

_PB2_PATH = Path(__file__).resolve().parents[3] / "examples" / "proto" / "user_joined_pb2.py"


def _load_pb2():
    pytest.importorskip("google.protobuf")
    spec = importlib.util.spec_from_file_location("user_joined_pb2", _PB2_PATH)
    if spec is None or spec.loader is None:
        pytest.skip(f"protobuf fixture is missing at {_PB2_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreatedNotice(cqrs.NotificationEvent):
    event_name: str = "order_created"
    topic: str = "order_events"
    payload: str = ""

    def proto(self):
        pb2 = _load_pb2()
        message = pb2.UserJoinedNotification()
        message.event_id = str(self.event_id)
        message.event_timestamp = self.event_timestamp.isoformat()
        message.event_name = self.event_name
        message.payload.user_id = "u-1"
        message.payload.meeting_id = "m-1"
        return message


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder) -> CreatedOrder:
        self._events.append(OrderCreatedNotice())
        return CreatedOrder(order_id="1")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


async def test_protobuf_published_message_keeps_bytes_and_content_type() -> None:
    pb2 = _load_pb2()
    async with test_harness(
        TestContainer(),
        commands_mapper=commands_mapper,
        serializer=cqrs.ProtobufEventSerializer({OrderCreatedNotice: pb2.UserJoinedNotification}),
    ) as harness:
        assert harness.mediator is not None
        await harness.mediator.send(CreateOrder(sku="book"))
        message = harness.published.one(OrderCreatedNotice)
        assert message.content_type == "application/x-protobuf"
        assert message.payload_bytes
