"""
Accept a payment and publish ``PaymentCompleted`` through the in-memory transport.

``AcceptPayment`` goes through ``RequestMediator``. The handler appends
``PaymentCompleted``, and the mediator publishes that notification on
``InMemoryTransport``. The subscriber acks the wire bytes.

No Docker, FastAPI, or FastStream.

Run::

    python examples/transport/memory_flow.py
"""

from __future__ import annotations

import asyncio

import cqrs

from payment import run_payment_flow


async def main() -> None:
    await run_payment_flow(cqrs.InMemoryTransport())
    print("memory flow ok")


if __name__ == "__main__":
    asyncio.run(main())
