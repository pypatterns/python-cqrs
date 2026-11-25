"""
Accept a payment and publish ``PaymentCompleted`` through Kafka.

``AcceptPayment`` goes through ``RequestMediator``. The handler appends
``PaymentCompleted``, and the mediator publishes that notification on this
transport. The subscriber acks the wire bytes.

``kafka_tests`` in ``docker-compose-test.yml`` must be accepting connections
on ``localhost:9092``. No FastAPI or FastStream.

Run::

    python examples/transport/kafka_flow.py
"""

from __future__ import annotations

import asyncio

from cqrs.message_brokers.kafka import create_transport

from payment import run_payment_flow


async def main() -> None:
    transport = create_transport("kafka://localhost:9092")
    await run_payment_flow(transport)
    print("kafka flow ok")


if __name__ == "__main__":
    asyncio.run(main())
