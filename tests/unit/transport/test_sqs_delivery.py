"""SQS receiver recovery and dead-letter queue names. No broker."""

from __future__ import annotations

import pytest

pytest.importorskip("aioboto3")

from cqrs.adapters.sqs import SQSDelivery  # noqa: E402
from cqrs.message_brokers.sqs import (  # noqa: E402
    SQSTransport,
    _Bucket,
    _Incoming,
    _SQSSubscription,
    default_sqs_queue_name,
)
from cqrs.transport.protocol import IncomingMessage, SubscribeOptions  # noqa: E402


def _delivery(body: bytes, receive_count: int) -> SQSDelivery:
    return SQSDelivery(
        message_id=f"id-{body.decode()}",
        receipt_handle=f"receipt-{body.decode()}",
        body=body,
        headers={},
        content_type=None,
        key=None,
        receive_count=receive_count,
        sent_timestamp=0,
    )


async def test_visibility_failure_does_not_stop_polling() -> None:
    transport = SQSTransport("sqs://")
    bucket = _Bucket("orders", "billing", "orders-billing")
    bucket.queue_url = "http://queue"
    bucket.native_dlq = True
    seen: list[bytes] = []

    async def handler(message: IncomingMessage) -> None:
        seen.append(bytes(message.body))
        subscription.active = False
        await message.ack()

    subscription = _SQSSubscription(
        transport,
        bucket,
        handler,
        SubscribeOptions(group="billing", max_deliveries=1),
    )
    calls = 0

    class _Client:
        async def receive(
            self,
            queue_url: str,
            *,
            max_messages: int,
            wait_time_seconds: int,
            visibility_timeout: int,
        ) -> list[SQSDelivery]:
            nonlocal calls
            calls += 1
            if calls == 1:
                return [_delivery(b"old", 5)]
            return [_delivery(b"new", 1)]

        async def delete(self, queue_url: str, receipt_handle: str) -> None:
            return None

    transport._client = _Client()  # type: ignore[assignment]

    async def defer(target: _Bucket, delivery: SQSDelivery) -> None:
        raise RuntimeError("visibility")

    transport._defer = defer  # type: ignore[method-assign]

    await transport._poll(subscription)

    assert seen == [b"new"]


async def test_fallback_dead_letter_uses_the_queue_namer() -> None:
    def prefix(topic: str) -> str:
        return f"tenant-a-{topic}"

    transport = SQSTransport("sqs://", queue_name=prefix)
    names: list[str] = []

    class _Client:
        async def ensure_queue(self, name: str) -> str:
            names.append(name)
            return "http://queue"

        async def send(self, queue_url: str, **kwargs: object) -> str:
            return "id"

    transport._client = _Client()  # type: ignore[assignment]
    bucket = _Bucket("orders", "billing", "tenant-a-orders-billing")
    bucket.queue_url = "http://src"

    async def handler(message: IncomingMessage) -> None:
        return None

    subscription = _SQSSubscription(transport, bucket, handler, SubscribeOptions(group="billing"))
    incoming = _Incoming(transport, subscription, _delivery(b"row", 2))

    await transport._publish_dead_letter(incoming)

    assert names == [prefix("orders.dlq")]


async def test_fallback_dead_letter_uses_the_default_name_without_a_namer() -> None:
    transport = SQSTransport("sqs://")
    names: list[str] = []

    class _Client:
        async def ensure_queue(self, name: str) -> str:
            names.append(name)
            return "http://queue"

        async def send(self, queue_url: str, **kwargs: object) -> str:
            return "id"

    transport._client = _Client()  # type: ignore[assignment]
    bucket = _Bucket("orders", "billing", "orders-billing")
    bucket.queue_url = "http://src"

    async def handler(message: IncomingMessage) -> None:
        return None

    subscription = _SQSSubscription(transport, bucket, handler, SubscribeOptions(group="billing"))
    incoming = _Incoming(transport, subscription, _delivery(b"row", 2))

    await transport._publish_dead_letter(incoming)

    assert names == [default_sqs_queue_name("orders.dlq")]
