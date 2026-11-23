"""
Accept a payment and publish ``PaymentCompleted`` through Redis Streams.

``AcceptPayment`` goes through ``RequestMediator``. The handler appends
``PaymentCompleted``, and the mediator publishes that notification on this
transport. The subscriber acks the wire bytes.

``redis_tests`` in ``docker-compose-test.yml`` must be accepting connections
on ``localhost:6379``. No FastAPI or FastStream.

Run::

    python examples/transport/redis_flow.py
"""

from __future__ import annotations

import asyncio

import cqrs

from payment import run_payment_flow


async def main() -> None:
    transport = cqrs.create_transport("redis://localhost:6379/0")
    await run_payment_flow(transport)
    print("redis flow ok")


if __name__ == "__main__":
    asyncio.run(main())
