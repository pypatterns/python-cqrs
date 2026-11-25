"""Kafka consumer decoding. No broker."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("aiokafka")

from cqrs.adapters.kafka import KafkaTopicConsumer  # noqa: E402
from cqrs.message_brokers.kafka import _rewind_points  # noqa: E402


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
