"""
Accept a payment and publish ``PaymentCompleted`` through SQS on LocalStack.

``AcceptPayment`` goes through ``RequestMediator``. The handler appends
``PaymentCompleted``, and the mediator publishes that notification on this
transport. The subscriber acks the wire bytes.

SQS queue names cannot contain dots, so this file passes topic
``payments-completed``. The handler is the same one the other transports use.
``wait_time_seconds=0`` keeps the poll from blocking ``drain`` and ``close``.

``localstack_tests`` in ``docker-compose-test.yml`` must be accepting connections
on ``localhost:4566`` with the SQS service. No FastAPI or FastStream.

Run::

    python examples/transport/sqs_flow.py
"""

from __future__ import annotations

import asyncio

from cqrs.message_brokers.sqs import create_transport

from payment import run_payment_flow


async def main() -> None:
    transport = create_transport("sqs://localhost:4566?region=us-east-1&wait_time_seconds=0")
    await run_payment_flow(transport, topic="payments-completed")
    print("sqs flow ok")


if __name__ == "__main__":
    asyncio.run(main())
