"""
Accept a payment and publish ``PaymentCompleted`` through NATS JetStream.

``AcceptPayment`` goes through ``RequestMediator``. The handler appends
``PaymentCompleted``, and the mediator publishes that notification on this
transport. The subscriber acks the wire bytes.

``nats_tests`` in ``docker-compose-test.yml`` must be accepting connections
on ``localhost:4222`` with JetStream enabled (``-js``). No FastAPI or FastStream.

Run::

    python examples/transport/nats_flow.py
"""

from __future__ import annotations

import asyncio

import cqrs

from payment import run_payment_flow


async def main() -> None:
    transport = cqrs.create_transport("nats://localhost:4222")
    await run_payment_flow(transport)
    print("nats flow ok")


if __name__ == "__main__":
    asyncio.run(main())
