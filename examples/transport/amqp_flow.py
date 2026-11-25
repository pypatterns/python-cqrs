"""
Accept a payment and publish ``PaymentCompleted`` through AMQP (RabbitMQ).

``AcceptPayment`` goes through ``RequestMediator``. The handler appends
``PaymentCompleted``, and the mediator publishes that notification on this
transport. The subscriber acks the wire bytes.

``rabbitmq_tests`` in ``docker-compose-test.yml`` must be accepting connections
on ``localhost:5672``. No FastAPI or FastStream.

Run::

    python examples/transport/amqp_flow.py
"""

from __future__ import annotations

import asyncio

from cqrs.message_brokers.amqp import create_transport

from payment import run_payment_flow


async def main() -> None:
    transport = create_transport("amqp://guest:guest@localhost:5672/")
    await run_payment_flow(transport)
    print("amqp flow ok")


if __name__ == "__main__":
    asyncio.run(main())
