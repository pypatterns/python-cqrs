"""Kafka offset commits and handler limits. No broker."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

pytest.importorskip("aiokafka")

from cqrs.adapters.kafka import KafkaRecord, KafkaTopicConsumer  # noqa: E402
from cqrs.message_brokers.kafka import (  # noqa: E402
    KafkaTransport,
    _Account,
    _KafkaGroup,
    _KafkaSubscription,
    _Offsets,
)
from cqrs.transport.errors import TransportError  # noqa: E402
from cqrs.transport.protocol import IncomingMessage, SubscribeOptions  # noqa: E402


def test_commit_point_walks_fetched_offsets_across_gaps() -> None:
    offsets = _Offsets()
    offsets.observe(0, 5)
    offsets.observe(0, 7)
    offsets.mark_done(0, 7)

    assert offsets.commit_point(0) is None

    offsets.mark_done(0, 5)
    accepted = offsets.accept(0, offsets.commit_point(0) or 0)

    assert [item.offset for item in accepted] == [5, 7]


async def test_rejected_commit_stays_outstanding() -> None:
    transport = KafkaTransport("kafka://localhost:9092")
    attempts = 0

    async def commit(partition: int, next_offset: int) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("broker rejected the offset")

    consumer = SimpleNamespace(commit=commit, assigned_partitions=lambda: {0})
    group = _KafkaGroup("orders", "billing", consumer)  # type: ignore[arg-type]
    record = KafkaRecord(topic="orders", partition=0, offset=5, body=b"1", key=None, headers={})
    account = _Account()
    group.offsets.observe(0, 5)
    group.offsets.bind(0, 5, "wire", account)

    committing = asyncio.create_task(transport._commit_prefix(group, record))
    for _ in range(50):
        if transport._commit_errors:
            break
        await asyncio.sleep(0.01)
    committing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await committing

    assert attempts >= 1
    assert not account.done
    assert isinstance(next(iter(transport._commit_errors.values())), RuntimeError)
    assert "commit_error=" in transport._drain_message()


async def test_lost_assignment_is_not_retried() -> None:
    transport = KafkaTransport("kafka://localhost:9092")
    attempts = 0

    async def commit(partition: int, next_offset: int) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("generation changed")

    consumer = SimpleNamespace(commit=commit, assigned_partitions=lambda: set())
    group = _KafkaGroup("orders", "billing", consumer)  # type: ignore[arg-type]
    record = KafkaRecord(topic="orders", partition=0, offset=5, body=b"1", key=None, headers={})
    account = _Account()
    group.offsets.bind(0, 5, "wire", account)

    await transport._commit_prefix(group, record)

    assert attempts == 0
    assert not account.done
    assert isinstance(transport._commit_errors[("orders", "billing", 0)], TransportError)


async def test_prefetch_does_not_run_extra_handlers() -> None:
    transport = KafkaTransport("kafka://localhost:9092")
    group = _KafkaGroup("orders", "billing", SimpleNamespace())  # type: ignore[arg-type]
    options = SubscribeOptions(group="billing", concurrency=1, prefetch=2)
    running = 0
    peak = 0
    release = asyncio.Event()
    started = asyncio.Event()

    async def handler(message: IncomingMessage) -> None:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        started.set()
        await release.wait()
        running -= 1
        await message.ack()

    subscription = _KafkaSubscription(transport, group, handler, options)
    group.subscriptions.append(subscription)
    group.add_buffer(options.concurrency * options.prefetch)

    async def commit_prefix(group: _KafkaGroup, record: KafkaRecord) -> None:
        return None

    transport._commit_prefix = commit_prefix  # type: ignore[method-assign]
    first = KafkaRecord(topic="orders", partition=0, offset=1, body=b"1", key=None, headers={})
    second = KafkaRecord(topic="orders", partition=0, offset=2, body=b"2", key=None, headers={})
    group.offsets.observe(0, 1)
    group.offsets.observe(0, 2)
    tasks = [
        asyncio.create_task(transport._guarded(group, subscription, first)),
        asyncio.create_task(transport._guarded(group, subscription, second)),
    ]
    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.sleep(0.05)
    observed = peak
    release.set()
    await asyncio.gather(*tasks)

    assert observed == 1


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
