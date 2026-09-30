import logging
import typing

from cqrs.adapters import protocol as adapters_protocol
from cqrs.message_brokers import protocol
from cqrs.serializers.default import message_wire_bytes


class KafkaMessageBroker(protocol.MessageBroker):
    def __init__(
        self,
        producer: adapters_protocol.KafkaProducer,
        aiokafka_log_level: typing.Text = "ERROR",
    ):
        self._producer = producer
        logging.getLogger("aiokafka").setLevel(aiokafka_log_level)

    async def send_message(self, message: protocol.Message) -> None:
        payload = message_wire_bytes(message)
        if message.headers is None:
            await self._producer.produce(message.topic, payload)
        else:
            await self._producer.produce(
                message.topic,
                payload,
                headers=message.headers,
            )
