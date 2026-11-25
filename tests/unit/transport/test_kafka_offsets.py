"""Kafka consumer decoding. No broker."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("aiokafka")

from cqrs.adapters.kafka import KafkaRecord, KafkaTopicConsumer  # noqa: E402
from cqrs.message_brokers.kafka import (  # noqa: E402
    KafkaTransport,
    _KafkaIncoming,
    _KafkaSubscription,
    _record_id,
    _rewind_points,
)
from cqrs.message_brokers.protocol import Message  # noqa: E402
from cqrs.transport.protocol import SubscribeOptions  # noqa: E402


def test_rewind_keeps_every_partition_at_its_first_unprocessed_offset() -> None:
    assert _rewind_points([(0, 5), (1, 3), (0, 6), (1, 4)]) == {0: 5, 1: 3}


async def test_poll_keeps_the_rest_of_a_batch_when_metadata_is_not_utf8() -> None:
    class _Record:
        def __init__(self, offset: int, key: bytes, header: bytes) -> None:
            self.topic = "orders"
            self.partition = 0
            self.offset = offset
            self.value = b"body"
            self.key = key
            self.headers = [("name", header)]

    class _Consumer:
        async def getmany(self, **kwargs: object) -> dict[object, list[_Record]]:
            return {"tp": [_Record(1, b"\xff", b"\xff"), _Record(2, b"good", b"plain")]}

    consumer = KafkaTopicConsumer(
        "localhost:9092",
        group_id="billing",
        topic="orders",
        auto_offset_reset="latest",
    )
    consumer._consumer = _Consumer()  # type: ignore[assignment]

    records = await consumer.poll()

    assert [record.offset for record in records] == [1, 2]
    assert records[0].key == "\ufffd"
    assert records[1].key == "good"
    assert records[1].headers["name"] == "plain"


async def test_cancelled_consumer_start_stops_the_client(monkeypatch: pytest.MonkeyPatch) -> None:
    stopped = asyncio.Event()

    class _Consumer:
        def __init__(self, *args: object, **kwargs: object) -> None:
            return None

        async def start(self) -> None:
            await asyncio.Future()

        async def stop(self) -> None:
            stopped.set()

        def assignment(self) -> set[object]:
            return {object()}

    monkeypatch.setattr("cqrs.adapters.kafka.aiokafka.AIOKafkaConsumer", _Consumer)
    consumer = KafkaTopicConsumer(
        "localhost:9092",
        group_id="billing",
        topic="orders",
        auto_offset_reset="latest",
    )
    starting = asyncio.create_task(consumer.start())
    await asyncio.sleep(0.05)
    starting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await starting

    assert stopped.is_set()
    assert consumer._consumer is None


async def test_start_returns_when_the_group_has_no_partitions(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Consumer:
        def __init__(self, *args: object, **kwargs: object) -> None:
            return None

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        def assignment(self) -> set[object]:
            return set()

    monkeypatch.setattr("cqrs.adapters.kafka.aiokafka.AIOKafkaConsumer", _Consumer)
    consumer = KafkaTopicConsumer(
        "localhost:9092",
        group_id="billing",
        topic="orders",
        auto_offset_reset="latest",
    )

    await asyncio.wait_for(consumer.start(), timeout=1)

    assert consumer._consumer is not None
    await consumer.stop()


def _subscription(transport: KafkaTransport, consumer: object) -> _KafkaSubscription:
    return _KafkaSubscription(
        transport,
        topic="orders",
        group="billing",
        handler=_unused,
        options=SubscribeOptions(group="billing"),
        consumer=consumer,  # type: ignore[arg-type]
    )


async def _unused(message: object) -> None:
    return None


async def test_seek_skips_a_revoked_partition_and_keeps_the_rest() -> None:
    sought: list[tuple[int, int]] = []

    class _Consumer:
        async def seek(self, partition: int, offset: int) -> None:
            if partition == 1:
                raise RuntimeError("revoked")
            sought.append((partition, offset))

    transport = KafkaTransport(bootstrap_servers="localhost:9092")
    subscription = _subscription(transport, _Consumer())
    records = [
        KafkaRecord(topic="orders", partition=partition, offset=offset, body=b"", key=None, headers={})
        for partition, offset in ((0, 5), (1, 3), (2, 8))
    ]

    await transport._seek_unprocessed(subscription, records)

    assert sought == [(0, 5), (2, 8)]


async def test_failed_commit_leaves_the_record_outstanding() -> None:
    class _Consumer:
        async def commit(self, partition: int, next_offset: int) -> None:
            raise RuntimeError("commit rejected")

    transport = KafkaTransport(bootstrap_servers="localhost:9092")
    subscription = _subscription(transport, _Consumer())
    record = KafkaRecord(topic="orders", partition=0, offset=4, body=b"row", key=None, headers={})
    incoming = _KafkaIncoming(topic="orders", body=b"row", headers={}, content_type=None, key=None)
    await incoming.ack()
    transport._watch.expect(_record_id(record.topic, record.partition, record.offset), 1)

    with pytest.raises(RuntimeError, match="commit rejected"):
        await transport._finish(subscription, record, incoming, {}, None)

    assert transport._watch.owed == 1


async def test_cancelled_send_drops_the_drain_reservation() -> None:
    transport = KafkaTransport(bootstrap_servers="localhost:9092")
    transport._subscriptions.append(_subscription(transport, object()))

    class _Producer:
        async def produce(self, *args: object, **kwargs: object) -> None:
            await asyncio.Future()

    transport._producer = _Producer()  # type: ignore[assignment]
    send = asyncio.create_task(
        transport.send_message(Message(message_name="order", topic="orders", payload=b"1")),
    )
    for _ in range(50):
        if transport._watch.owed:
            break
        await asyncio.sleep(0)
    assert transport._watch.owed == 1

    send.cancel()
    with pytest.raises(asyncio.CancelledError):
        await send
    await transport.drain(timeout=0.2)
