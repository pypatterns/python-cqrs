"""
SQS transport against LocalStack.

The fixture targets ``http://127.0.0.1:4566`` and skips when that port is
closed. This module does not start Docker.

Contract cases from ``tests.unit.transport.test_contract`` run through the
fixture's ``queue_name`` override. That override gives each test its own
prefix and turns the contract's ``orders.dlq`` topic into a legal queue
name. The library itself does not rewrite other dotted topics.
``test_two_instances_do_not_share_messages`` stays on ``InMemoryTransport``
and is not repeated here.
"""

from __future__ import annotations

import asyncio
import socket
import uuid
import urllib.parse
from collections.abc import AsyncIterator, Callable

import pytest

pytest.importorskip("aioboto3")

from cqrs.message_brokers.protocol import Message  # noqa: E402
from cqrs.message_brokers.sqs import (  # noqa: E402
    SQSTransport,
    default_sqs_queue_name,
    physical_queue_name,
)
from cqrs.serializers.default import message_wire_bytes  # noqa: E402
from cqrs.transport import (  # noqa: E402
    IncomingMessage,
    SubscribeOptions,
    TransportClosed,
    create_transport,
)
from cqrs.transport.errors import TransportError  # noqa: E402
from cqrs.transport.protocol import Transport  # noqa: E402
import tests.unit.transport.test_contract as contract  # noqa: E402

_HOST = "127.0.0.1"
_PORT = 4566
_URL = f"sqs://test:test@{_HOST}:{_PORT}?region=us-east-1"


def _port_open() -> bool:
    try:
        with socket.create_connection((_HOST, _PORT), timeout=0.3):
            return True
    except OSError:
        return False


def _require_localstack() -> None:
    if not _port_open():
        pytest.skip("LocalStack SQS is not accepting connections on localhost:4566")


def _isolate(prefix: str) -> Callable[[str], str]:
    """Prefix a topic and make the contract's dotted DLQ name legal for SQS."""

    def queue_name(topic: str) -> str:
        fifo = topic.endswith(".fifo")
        stem = topic[: -len(".fifo")] if fifo else topic
        if stem.endswith(".dlq"):
            stem = f"{stem[: -len('.dlq')]}-dlq"
        stem = stem.replace(".", "-")
        if fifo:
            return f"{prefix}-{stem}.fifo"
        return f"{prefix}-{stem}"

    return queue_name


def _open(*, queue_name: Callable[[str], str] | None = None) -> SQSTransport:
    return SQSTransport(
        _URL,
        queue_name=queue_name,
        wait_time_seconds=0,
        visibility_timeout=30,
        redelivery_delay=0,
    )


@pytest.fixture
async def transport() -> AsyncIterator[SQSTransport]:
    """One LocalStack transport. Dotted contract topics are translated here."""

    _require_localstack()
    prefix = uuid.uuid4().hex[:12]
    broker = _open(queue_name=_isolate(prefix))
    try:
        yield broker
    finally:
        await broker.close()


def test_default_queue_name_keeps_dots_and_hyphenates_only_dlq() -> None:
    assert default_sqs_queue_name("orders") == "orders"
    assert default_sqs_queue_name("order.paid") == "order.paid"
    assert default_sqs_queue_name("orders.dlq") == "orders-dlq"
    assert default_sqs_queue_name("jobs.fifo") == "jobs.fifo"
    assert default_sqs_queue_name("jobs.fifo.dlq") == "jobs-dlq.fifo"
    assert physical_queue_name("orders", "billing") == "orders-billing"
    assert physical_queue_name("jobs.fifo", "billing") == "jobs-billing.fifo"
    assert physical_queue_name("orders-dlq", "dead") == "orders-dlq-dead"


def test_create_transport_reads_endpoint_region_and_credentials() -> None:
    parsed = create_transport(
        "sqs://?endpoint_url=http%3A%2F%2F127.0.0.1%3A4566&region=eu-central-1"
        "&aws_access_key_id=key&aws_secret_access_key=secret&aws_session_token=token",
    )
    assert isinstance(parsed, SQSTransport)
    assert parsed.endpoint_url == "http://127.0.0.1:4566"
    assert parsed.region_name == "eu-central-1"
    assert parsed._access_key == "key"
    assert parsed._secret_key == "secret"
    assert parsed._session_token == "token"
    assert parsed.capabilities.ordering_per_key is False
    assert parsed.capabilities.native_dlq is False
    assert parsed.capabilities.delayed_redelivery is True
    assert parsed.capabilities.headers is True
    assert not hasattr(SQSTransport, "__bool__")
    assert not hasattr(SQSTransport, "__len__")
    assert bool(parsed) is True


def test_host_port_url_defaults_localstack_credentials() -> None:
    transport = create_transport("sqs://localhost:4566?region=us-east-1")
    assert isinstance(transport, SQSTransport)
    assert transport.endpoint_url == "http://localhost:4566"
    assert transport.region_name == "us-east-1"
    assert transport._access_key == "test"
    assert transport._secret_key == "test"


def test_amazonaws_host_uses_the_region_and_the_default_credential_chain() -> None:
    transport = SQSTransport("sqs://sqs.eu-west-1.amazonaws.com")
    assert transport.endpoint_url is None
    assert transport.region_name == "eu-west-1"
    assert transport._access_key is None
    assert transport._secret_key is None


def test_keyword_arguments_override_the_url() -> None:
    transport = SQSTransport(
        urllib.parse.urlparse("sqs://localhost:4566?region=us-east-1"),
        endpoint_url="http://127.0.0.1:4566",
        region_name="ap-southeast-2",
        aws_access_key_id="override",
        aws_secret_access_key="override-secret",
        visibility_timeout=15,
        wait_time_seconds=0,
        redelivery_delay=1,
    )
    assert transport.endpoint_url == "http://127.0.0.1:4566"
    assert transport.region_name == "ap-southeast-2"
    assert transport._access_key == "override"
    assert transport.visibility_timeout == 15
    assert transport.wait_time_seconds == 0
    assert transport.redelivery_delay == 1


async def test_send_and_subscribe_after_close_raise() -> None:
    transport = create_transport("sqs://localhost:4566")
    assert isinstance(transport, SQSTransport)
    await transport.close()
    message = Message(message_name="closed", topic="orders", payload={"n": 1})
    with pytest.raises(TransportClosed):
        await transport.send_message(message)
    with pytest.raises(TransportClosed):
        await transport.subscribe("orders", _noop, SubscribeOptions(group="billing"))


async def test_contract_round_trip_body_headers_content_type_and_key(transport: Transport) -> None:
    await contract.test_round_trip_body_headers_content_type_and_key(transport)


async def test_contract_groups_broadcast_and_compete(transport: Transport) -> None:
    await contract.test_groups_broadcast_and_compete(transport)


async def test_contract_same_key_stays_ordered_when_concurrency_is_high(transport: Transport) -> None:
    await contract.test_same_key_stays_ordered_when_concurrency_is_high(transport)


async def test_contract_different_keys_run_side_by_side(transport: Transport) -> None:
    await contract.test_different_keys_run_side_by_side(transport)


async def test_contract_ack_does_not_redeliver(transport: Transport) -> None:
    await contract.test_ack_does_not_redeliver(transport)


async def test_contract_nack_increases_attempt_then_dead_letters(transport: Transport) -> None:
    """``orders.dlq`` is illegal on SQS; the fixture maps that topic to a hyphen queue."""

    await contract.test_nack_increases_attempt_then_dead_letters(transport)


async def test_contract_handler_exception_does_not_escape_send(transport: Transport) -> None:
    await contract.test_handler_exception_does_not_escape_send(transport)


async def test_contract_close_with_in_flight_message_then_rejects_send(transport: Transport) -> None:
    await contract.test_close_with_in_flight_message_then_rejects_send(transport)


async def test_legal_names_round_trip_and_group_competition(transport: Transport) -> None:
    billing: list[IncomingMessage] = []
    warehouse: list[IncomingMessage] = []
    rival: list[IncomingMessage] = []

    async def collect(sink: list[IncomingMessage], message: IncomingMessage) -> None:
        sink.append(message)
        await message.ack()

    await transport.subscribe(
        "order-paid",
        lambda message: collect(billing, message),
        SubscribeOptions(group="billing"),
    )
    await transport.subscribe(
        "order-paid",
        lambda message: collect(warehouse, message),
        SubscribeOptions(group="warehouse"),
    )
    await transport.subscribe(
        "order-paid",
        lambda message: collect(rival, message),
        SubscribeOptions(group="billing"),
    )
    message = Message(
        message_name="paid",
        topic="order-paid",
        payload={"sku": "book"},
        headers={"source": "focused"},
        content_type="application/json",
        key="order-1",
    )
    await transport.send_message(message)
    await transport.drain(timeout=2)

    assert len(billing) + len(rival) == 1
    assert warehouse and warehouse[0].body == message_wire_bytes(message)
    received = billing[0] if billing else rival[0]
    assert received.body == message_wire_bytes(message)
    assert received.headers == {"source": "focused"}
    assert received.content_type == "application/json"
    assert received.key == "order-1"
    assert received.topic == "order-paid"
    assert received.delivery_attempt == 1
    assert warehouse[0].topic == "order-paid"


async def test_legal_names_ack_nack_and_hyphen_dlq(transport: Transport) -> None:
    attempts: list[int] = []
    dead: list[IncomingMessage] = []
    direct: list[int] = []

    async def requeue(message: IncomingMessage) -> None:
        attempts.append(message.delivery_attempt)
        await message.nack(requeue=True)

    async def reject(message: IncomingMessage) -> None:
        direct.append(message.delivery_attempt)
        await message.nack(requeue=False)

    async def dead_letter(message: IncomingMessage) -> None:
        dead.append(message)
        await message.ack()

    await transport.subscribe(
        "order-paid",
        requeue,
        SubscribeOptions(group="billing", max_deliveries=2, dead_letter_topic="order-paid-dlq"),
    )
    await transport.subscribe(
        "order-reject",
        reject,
        SubscribeOptions(group="billing", max_deliveries=5, dead_letter_topic="order-paid-dlq"),
    )
    await transport.subscribe(
        "order-paid-dlq",
        dead_letter,
        SubscribeOptions(group="dead"),
    )

    await transport.send_message(
        Message(message_name="row", topic="order-paid", payload=b"row", payload_bytes=b"row", key="k"),
    )
    await transport.send_message(
        Message(message_name="drop", topic="order-reject", payload=b"drop", payload_bytes=b"drop", key="drop"),
    )
    await transport.drain(timeout=2)

    assert attempts == [1, 2]
    assert direct == [1]
    assert len(dead) == 2
    by_key = {message.key: message for message in dead}
    assert by_key["k"].topic == "order-paid-dlq"
    assert by_key["k"].body == b"row"
    assert by_key["k"].delivery_attempt == 2
    assert by_key["drop"].topic == "order-paid-dlq"
    assert by_key["drop"].body == b"drop"
    assert by_key["drop"].delivery_attempt == 1


async def test_default_dlq_suffix_is_a_hyphen_without_a_name_override() -> None:
    """Protocol topic ``{name}.dlq`` is created as ``{name}-dlq`` by the library."""

    _require_localstack()
    topic = f"orders-{uuid.uuid4().hex[:12]}"
    broker = _open()
    attempts: list[int] = []
    dead: list[IncomingMessage] = []
    try:

        async def handler(message: IncomingMessage) -> None:
            attempts.append(message.delivery_attempt)
            await message.nack(requeue=True)

        async def dead_letter(message: IncomingMessage) -> None:
            dead.append(message)
            await message.ack()

        await broker.subscribe(
            topic,
            handler,
            SubscribeOptions(group="billing", max_deliveries=2),
        )
        await broker.subscribe(f"{topic}.dlq", dead_letter, SubscribeOptions(group="dead"))
        await broker.send_message(
            Message(message_name="row", topic=topic, payload=b"row", payload_bytes=b"row", key="k"),
        )
        await broker.drain(timeout=2)
    finally:
        await broker.close()

    assert attempts == [1, 2]
    assert len(dead) == 1
    assert dead[0].topic == f"{topic}.dlq"
    assert dead[0].body == b"row"
    assert dead[0].key == "k"
    assert dead[0].delivery_attempt == 2


async def test_dotted_topic_is_not_rewritten() -> None:
    _require_localstack()
    broker = _open()
    try:
        with pytest.raises(TransportError, match="order.paid"):
            await broker.subscribe("order.paid", _noop, SubscribeOptions(group="billing"))
    finally:
        await broker.close()


async def test_fifo_queue_orders_one_key(transport: Transport) -> None:
    order: list[str] = []
    release_first = asyncio.Event()
    started_first = asyncio.Event()

    async def handler(message: IncomingMessage) -> None:
        body = message.body.decode()
        order.append(f"start-{body}")
        if body == "1":
            started_first.set()
            await release_first.wait()
        order.append(f"end-{body}")
        await message.ack()

    await transport.subscribe(
        "jobs.fifo",
        handler,
        SubscribeOptions(group="workers", concurrency=4),
    )
    await transport.send_message(
        Message(message_name="job", topic="jobs.fifo", payload=b"1", payload_bytes=b"1", key="same"),
    )
    await transport.send_message(
        Message(message_name="job", topic="jobs.fifo", payload=b"2", payload_bytes=b"2", key="same"),
    )
    await asyncio.wait_for(started_first.wait(), timeout=1)
    await asyncio.sleep(0)
    assert order == ["start-1"]
    release_first.set()
    await transport.drain(timeout=2)
    assert order == ["start-1", "end-1", "start-2", "end-2"]


async def _noop(message: IncomingMessage) -> None:
    await message.ack()
