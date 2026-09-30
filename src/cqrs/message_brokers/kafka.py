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

    def _produce_value(self, message: protocol.Message) -> typing.Any:
        """
        Choose the value passed to ``KafkaProducer.produce``.

        When a legacy ``value_serializer`` was configured on the producer and the
        message has no codec wire bytes, forward ``message.payload`` so that
        serializer can run. Otherwise prefer ``payload_bytes`` / encoded payload
        so Outbox and EventSerializer bytes are not discarded.
        """
        legacy_value_serializer = getattr(
            self._producer,
            "legacy_value_serializer",
            False,
        )
        if legacy_value_serializer and message.payload_bytes is None and message.content_type is None:
            return message.payload
        return message_wire_bytes(message)

    async def send_message(self, message: protocol.Message) -> None:
        payload = self._produce_value(message)
        if message.headers is None:
            await self._producer.produce(message.topic, payload)
        else:
            await self._producer.produce(
                message.topic,
                payload,
                headers=message.headers,
            )
