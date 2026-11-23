"""
Publish a notification and hand the bytes to ``EventMediator``.

The subscriber decodes JSON with ``JsonDeserializer`` and calls ``send``.
There is no inbox. The transport is in-memory: no Docker, FastAPI, or FastStream.

Run::

    python examples/transport/event_flow.py
"""

from __future__ import annotations

import asyncio

import cqrs
from cqrs.bootstrap import events as events_bootstrap
from cqrs.bootstrap import requests as requests_bootstrap
from cqrs.deserializers import DeserializeJsonError, JsonDeserializer


class PlaceOrder(cqrs.Request[None]):
    order_id: str


class OrderPlaced(cqrs.NotificationEvent):
    event_name: str = "order_placed"
    topic: str = "events"
    payload: str


class PlaceOrderHandler(cqrs.RequestHandler[PlaceOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: PlaceOrder) -> None:
        self._events = [OrderPlaced(payload=request.order_id)]


HANDLED: list[str] = []


class RecordPlaced(cqrs.EventHandler[OrderPlaced]):
    async def handle(self, event: OrderPlaced) -> None:
        HANDLED.append(event.payload)


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(PlaceOrder, PlaceOrderHandler)


def events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderPlaced, RecordPlaced)


async def main() -> None:
    transport = cqrs.InMemoryTransport()
    mediator = requests_bootstrap.bootstrap(
        commands_mapper=commands_mapper,
        message_broker=transport,
    )
    event_mediator = events_bootstrap.bootstrap(events_mapper=events_mapper)
    deserializer: JsonDeserializer[OrderPlaced] = JsonDeserializer(OrderPlaced)

    async def on_event(message: cqrs.IncomingMessage) -> None:
        event = deserializer(message.body)
        if isinstance(event, DeserializeJsonError) or event is None:
            await message.nack(requeue=False)
            return
        await event_mediator.send(event)
        await message.ack()

    try:
        await transport.subscribe("events", on_event, cqrs.SubscribeOptions(group="billing"))
        await mediator.send(PlaceOrder(order_id="o-1"))
        await transport.drain()
        assert HANDLED == ["o-1"], HANDLED
    finally:
        await transport.close()
    print("event flow ok")


if __name__ == "__main__":
    asyncio.run(main())
