"""
Accept a payment and publish ``PaymentCompleted`` through a transport.

``AcceptPayment`` is the command. Its handler appends one
``PaymentCompleted`` notification. ``run_payment_flow`` builds a
``RequestMediator`` with the given transport as ``message_broker``,
subscribes to the notification topic, and sends the command. The
subscriber acks the delivery. The bytes are the wire bytes
``EventEmitter`` published (``payload_bytes`` via ``message_wire_bytes``).

Losing the message is acceptable here: there is no outbox. Each backend
example only chooses the transport and calls ``run_payment_flow``.
The consumer group includes the process id and a uuid so a rerun does
not share a group with a previous run.
"""

from __future__ import annotations

import dataclasses
import os
import uuid

import cqrs
from cqrs.bootstrap import requests as requests_bootstrap
from cqrs.message_brokers.protocol import Message
from cqrs.serializers.default import message_wire_bytes

PAYMENT_TOPIC = "payments.completed"


@dataclasses.dataclass(frozen=True)
class PaymentCompletedPayload:
    payment_id: str
    amount: str


class AcceptPayment(cqrs.Request[None]):
    payment_id: str
    amount: str


class PaymentCompleted(cqrs.NotificationEvent[PaymentCompletedPayload]):
    event_name: str = "payment_completed"
    topic: str = PAYMENT_TOPIC
    payload: PaymentCompletedPayload


class AcceptPaymentHandler(cqrs.RequestHandler[AcceptPayment]):
    def __init__(self, topic: str = PAYMENT_TOPIC) -> None:
        self._topic = topic
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: AcceptPayment):
        self._events = [
            PaymentCompleted(
                topic=self._topic,
                payload=PaymentCompletedPayload(
                    payment_id=request.payment_id,
                    amount=request.amount,
                ),
            ),
        ]


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(AcceptPayment, AcceptPaymentHandler)


def _consumer_group() -> str:
    return f"billing-{os.getpid()}-{uuid.uuid4().hex}"


async def run_payment_flow(transport: cqrs.Transport, *, topic: str = PAYMENT_TOPIC) -> None:
    """Subscribe, accept one payment, and require that one notification delivery."""

    published: list[Message] = []
    seen: list[bytes] = []
    original_send = transport.send_message

    async def capture_send(message: Message) -> None:
        published.append(message)
        await original_send(message)

    # Record the Message EventEmitter passes to this transport. The subscriber
    # body is message_wire_bytes of that message (payload_bytes when set).
    setattr(transport, "send_message", capture_send)

    handler = AcceptPaymentHandler(topic=topic)
    container = cqrs.SimpleContainer()
    container.register(AcceptPaymentHandler, handler)
    mediator = requests_bootstrap.bootstrap(
        di_container=container,
        commands_mapper=commands_mapper,
        message_broker=transport,
    )

    async def on_payment(message: cqrs.IncomingMessage) -> None:
        seen.append(message.body)
        await message.ack()

    try:
        await transport.start()
        await transport.subscribe(topic, on_payment, cqrs.SubscribeOptions(group=_consumer_group()))
        await mediator.send(AcceptPayment(payment_id="pay-1", amount="10.00"))
        await transport.drain()
        assert len(published) == 1, published
        notice = published[0]
        assert notice.message_name == "PaymentCompleted", notice
        assert notice.topic == topic, notice
        assert seen == [message_wire_bytes(notice)], (seen, notice)
        event = handler.events[0]
        assert isinstance(event, PaymentCompleted)
        assert event.payload.payment_id == "pay-1"
        assert event.payload.amount == "10.00"
    finally:
        await transport.close()
