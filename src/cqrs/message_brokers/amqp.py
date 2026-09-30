import logging

import aio_pika

from cqrs.adapters import protocol as adapters_protocol
from cqrs.message_brokers import protocol
from cqrs.serializers.default import message_wire_bytes


class AMQPMessageBroker(protocol.MessageBroker):
    def __init__(
        self,
        publisher: adapters_protocol.AMQPPublisher,
        exchange_name: str,
        pika_log_level: str = "ERROR",
    ):
        self.publisher = publisher
        self.exchange_name = exchange_name
        logging.getLogger("aiormq").setLevel(pika_log_level)
        logging.getLogger("aio_pika").setLevel(pika_log_level)

    async def send_message(self, message: protocol.Message) -> None:
        kwargs: dict = {}
        if message.content_type is not None:
            kwargs["content_type"] = message.content_type
        if message.headers is not None:
            kwargs["headers"] = message.headers
        await self.publisher.publish(
            message=aio_pika.Message(
                body=message_wire_bytes(message),
                **kwargs,
            ),
            queue_name=message.topic,
            exchange_name=self.exchange_name,
        )
