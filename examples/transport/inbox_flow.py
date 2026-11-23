"""
Accept a command from an in-memory transport into the inbox, then process it.

The subscriber reads bytes, rebuilds the command with ``InboxMessageMap``,
and ``add``s a batch of one. Ack happens after ``add``. A duplicate id returns
``[False]`` and is still acked. ``InboxProcessor`` runs in a later step.

No Docker, FastAPI, or FastStream.

Run::

    python examples/transport/inbox_flow.py
"""

from __future__ import annotations

import asyncio

import cqrs
from cqrs.bootstrap import requests as bootstrap
from cqrs.inbox.memory import MemoryInboxRepository
from cqrs.message_brokers.protocol import Message


class ProcessOrder(cqrs.Request[None]):
    message_id: str
    account_id: str
    amount: str


APPLIED: list[str] = []


class RecordingHandler(cqrs.RequestHandler[ProcessOrder]):
    async def handle(self, request: ProcessOrder) -> None:
        APPLIED.append(f"{request.account_id}:{request.amount}")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ProcessOrder, RecordingHandler)


async def main() -> None:
    transport = cqrs.InMemoryTransport()
    message_map = cqrs.InboxMessageMap()
    message_map.register("ProcessOrder", ProcessOrder)
    inbox = MemoryInboxRepository(message_map)
    stored: list[list[bool]] = []

    async def on_order(message: cqrs.IncomingMessage) -> None:
        decoded = message_map.deserialize("ProcessOrder", message.body)
        command = decoded if isinstance(decoded, ProcessOrder) else None
        if command is None:
            await message.nack(requeue=False)
            return
        stored.append(await inbox.add([(command.message_id, command)]))
        await message.ack()

    try:
        await transport.subscribe("deposits", on_order, cqrs.SubscribeOptions(group="billing"))
        command = ProcessOrder(message_id="msg-1", account_id="acc-1", amount="10.00")
        _name, body = message_map.payload_for(command)
        envelope = Message(
            message_name="ProcessOrder",
            topic="deposits",
            payload=command.to_dict(),
            payload_bytes=body,
        )
        await transport.send_message(envelope)
        await transport.send_message(envelope)
        await transport.drain()
        assert stored == [[True], [False]], stored

        mediator = bootstrap.bootstrap(commands_mapper=commands_mapper)
        processor = cqrs.InboxProcessor(inbox, message_map, request_mediator=mediator)
        rows = await inbox.get_many()
        assert len(rows) == 1
        for row in rows:
            await processor.process(row)
        assert await inbox.get_many() == []
        assert APPLIED == ["acc-1:10.00"], APPLIED
    finally:
        await transport.close()
    print("inbox flow ok")


if __name__ == "__main__":
    asyncio.run(main())
