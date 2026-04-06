import asyncio
import logging
import ssl
import typing

from cqrs.adapters import protocol
from cqrs.serializers import default

import aiokafka
from aiokafka import errors


__all__ = (
    "KafkaProducer",
    "kafka_producer_factory",
)

_RETRYABLE_KAFKA_EXCEPTIONS = (
    errors.KafkaConnectionError,
    errors.NodeNotReadyError,
    errors.RequestTimedOutError,
)

SecurityProtocol = typing.Literal[
    "PLAINTEXT",
    "SSL",
    "SASL_PLAINTEXT",
    "SASL_SSL",
]
SaslMechanism = typing.Literal[
    "PLAIN",
    "GSSAPI",
    "SCRAM-SHA-256",
    "SCRAM-SHA-512",
    "OAUTHBEARER",
]

logger = logging.getLogger("cqrs")
logger.setLevel(logging.DEBUG)

Serializer = typing.Callable[[typing.Any], typing.Optional[typing.ByteString]]


class KafkaProducer(protocol.KafkaProducer):
    def __init__(
        self,
        producer: aiokafka.AIOKafkaProducer,
        retry_count: int = 3,
        retry_delay: int = 1,
    ):
        self._producer = producer
        self._retry_count = retry_count
        self._retry_delay = retry_delay

    async def _check_connection(self):
        node_id = self._producer.client.get_random_node()
        if not await self._producer.client.ready(node_id=node_id):
            await self._producer.start()

    async def _produce(self, topic: typing.Text, message: typing.Any):
        await self._check_connection()
        logger.debug(f"produce message {message} to topic {topic}")
        await self._producer.send_and_wait(topic, value=message)

    async def produce(self, topic: typing.Text, message: typing.Any):
        """
        Produces event to kafka broker.
        Tries to reconnect if connect has been lost or has not been opened.
        """
        for attempt in range(1, self._retry_count + 1):
            try:
                await self._produce(topic, message)
                return
            except _RETRYABLE_KAFKA_EXCEPTIONS:
                if attempt == self._retry_count:
                    raise
                await asyncio.sleep(self._retry_delay)


def kafka_producer_factory(
    dsn: typing.Text,
    security_protocol: SecurityProtocol = "PLAINTEXT",
    sasl_mechanism: SaslMechanism = "PLAIN",
    ssl_context: typing.Optional[ssl.SSLContext] = None,
    retry_count: int = 3,
    retry_delay: int = 1,
    user: typing.Optional[typing.Text] = None,
    password: typing.Optional[typing.Text] = None,
    value_serializer: typing.Optional[Serializer] = None,
) -> KafkaProducer:
    producer = aiokafka.AIOKafkaProducer(
        bootstrap_servers=dsn,
        value_serializer=value_serializer or default.default_serializer,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=user,
        sasl_plain_password=password,
        ssl_context=ssl_context,
    )
    return KafkaProducer(
        producer=producer,
        retry_count=retry_count,
        retry_delay=retry_delay,
    )
