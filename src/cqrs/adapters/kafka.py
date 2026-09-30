import asyncio
import functools
import logging
import ssl
import typing

from cqrs.adapters import protocol
from cqrs.serializers.default import passthrough_value_serializer

import aiokafka
import retry_async
from aiokafka import errors


__all__ = (
    "KafkaProducer",
    "kafka_producer_factory",
)

_retry = functools.partial(
    retry_async.retry,
    exceptions=(
        errors.KafkaConnectionError,
        errors.NodeNotReadyError,
        errors.RequestTimedOutError,
    ),
    is_async=True,
)

SecurityProtocol: typing.TypeAlias = typing.Literal[
    "PLAINTEXT",
    "SSL",
    "SASL_PLAINTEXT",
    "SASL_SSL",
]
SaslMechanism: typing.TypeAlias = typing.Literal[
    "PLAIN",
    "GSSAPI",
    "SCRAM-SHA-256",
    "SCRAM-SHA-512",
    "OAUTHBEARER",
]

logger = logging.getLogger("cqrs")
logger.setLevel(logging.DEBUG)

Serializer = typing.Callable[[typing.Any], typing.ByteString | None]


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

    async def _produce(
        self,
        topic: typing.Text,
        message: typing.Any,
        headers: dict[str, str] | None = None,
    ):
        await self._check_connection()
        logger.debug(f"produce message {message} to topic {topic}")
        produce_kwargs: dict[str, typing.Any] = {"value": message}
        if headers is not None:
            produce_kwargs["headers"] = [
                (key, value.encode("utf-8") if isinstance(value, str) else value) for key, value in headers.items()
            ]
        await self._producer.send_and_wait(topic, **produce_kwargs)

    async def produce(
        self,
        topic: typing.Text,
        message: typing.Any,
        headers: dict[str, str] | None = None,
    ):
        """
        Produces event to kafka broker.
        Tries to reconnect if connect has been lost or has not been opened.
        """
        await _retry(tries=self._retry_count, delay=self._retry_delay)(self._produce)(
            topic,
            message,
            headers,
        )


def kafka_producer_factory(
    dsn: typing.Text,
    security_protocol: SecurityProtocol = "PLAINTEXT",
    sasl_mechanism: SaslMechanism = "PLAIN",
    ssl_context: ssl.SSLContext | None = None,
    retry_count: int = 3,
    retry_delay: int = 1,
    user: typing.Text | None = None,
    password: typing.Text | None = None,
    value_serializer: Serializer | None = None,
) -> KafkaProducer:
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)

    producer = aiokafka.AIOKafkaProducer(
        bootstrap_servers=dsn,
        value_serializer=passthrough_value_serializer(value_serializer),
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=user,
        sasl_plain_password=password,
        ssl_context=ssl_context,
        loop=loop,
    )
    return KafkaProducer(
        producer=producer,
        retry_count=retry_count,
        retry_delay=retry_delay,
    )
