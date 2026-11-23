"""
Kafka producer and consumer adapters.

[AI Instruction]
Import ``kafka_producer_factory`` from this module and pass the result to
``cqrs.message_brokers.kafka.KafkaMessageBroker``. Both need the ``kafka``
extra. Application code does not call the producer directly when an outbox
or an event emitter is already publishing.

``KafkaTopicConsumer`` is the subscribe side used by ``KafkaTransport``.
Ack commits the record offset. Nack does not commit: the transport retries
the record in process, and a restart redelivers from the last commit.
Seeking back would replay later offsets that are still in flight, so nack
does not seek.
"""

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
    "KafkaRecord",
    "KafkaTopicConsumer",
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

# typing.ByteString (bytes | bytearray | memoryview) warns on Python 3.15.
Serializer = typing.Callable[[typing.Any], bytes | bytearray | memoryview | None]


class KafkaProducer(protocol.KafkaProducer):
    def __init__(
        self,
        producer: aiokafka.AIOKafkaProducer,
        retry_count: int = 3,
        retry_delay: int = 1,
        legacy_value_serializer: bool = False,
    ):
        self._producer = producer
        self._retry_count = retry_count
        self._retry_delay = retry_delay
        # True when kafka_producer_factory got an explicit value_serializer so
        # KafkaMessageBroker can pass message.payload (dict) on the non-codec path.
        self.legacy_value_serializer = legacy_value_serializer

    async def _check_connection(self):
        node_id = self._producer.client.get_random_node()
        if not await self._producer.client.ready(node_id=node_id):
            await self._producer.start()

    async def _produce(
        self,
        topic: typing.Text,
        message: typing.Any,
        headers: dict[str, str] | None = None,
        key: str | None = None,
    ):
        await self._check_connection()
        logger.debug(f"produce message {message} to topic {topic}")
        produce_kwargs: dict[str, typing.Any] = {"value": message}
        if headers is not None:
            produce_kwargs["headers"] = [
                (header_key, value.encode("utf-8") if isinstance(value, str) else value)
                for header_key, value in headers.items()
            ]
        if key is not None:
            produce_kwargs["key"] = key.encode()
        await self._producer.send_and_wait(topic, **produce_kwargs)

    async def produce(
        self,
        topic: typing.Text,
        message: typing.Any,
        headers: dict[str, str] | None = None,
        key: str | None = None,
    ):
        """
        Produces event to kafka broker.
        Tries to reconnect if connect has been lost or has not been opened.
        """
        await _retry(tries=self._retry_count, delay=self._retry_delay)(self._produce)(
            topic,
            message,
            headers,
            key,
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
        legacy_value_serializer=value_serializer is not None,
    )


def _decode_bytes(value: bytes | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _record_body(value: typing.Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return str(value).encode("utf-8")


class KafkaRecord:
    """One fetched record. aiokafka types stay inside the adapter."""

    def __init__(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        body: bytes,
        key: str | None,
        headers: dict[str, str],
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.body = body
        self.key = key
        self.headers = headers


class KafkaTopicConsumer:
    """
    ``AIOKafkaConsumer`` for one topic and one ``group_id``.

    ``commit`` records the next offset to read. ``nack`` does not commit and
    does not seek: seeking would replay later offsets still in flight. The
    transport retries the record until it commits. After a restart Kafka
    redelivers from the last commit.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        group_id: str,
        topic: str,
        auto_offset_reset: str,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._topic = topic
        self._auto_offset_reset = auto_offset_reset
        self._consumer: aiokafka.AIOKafkaConsumer | None = None

    async def start(self) -> None:
        if self._consumer is not None:
            return
        consumer = aiokafka.AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset=self._auto_offset_reset,
            fetch_max_wait_ms=50,
        )
        try:
            await consumer.start()
            self._consumer = consumer
            # Leave the position alone. ``auto_offset_reset`` applies only when
            # this group has no committed offset. Seeking would skip a committed
            # position or replay records the group already acknowledged.
            await self._wait_for_assignment()
        except Exception:
            self._consumer = consumer
            try:
                await self.stop()
            except Exception:
                logger.debug("kafka consumer cleanup failed", exc_info=True)
            raise

    async def stop(self) -> None:
        consumer = self._consumer
        self._consumer = None
        if consumer is not None:
            await consumer.stop()

    async def poll(self, *, timeout_ms: int = 100, max_records: int = 10) -> list[KafkaRecord]:
        consumer = self._require()
        batches = await consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
        records: list[KafkaRecord] = []
        for batch in batches.values():
            for record in batch:
                headers: dict[str, str] = {}
                for header_key, header_value in record.headers or ():
                    if isinstance(header_value, bytes):
                        headers[header_key] = header_value.decode("utf-8")
                    elif header_value is None:
                        headers[header_key] = ""
                    else:
                        headers[header_key] = str(header_value)
                records.append(
                    KafkaRecord(
                        topic=record.topic,
                        partition=record.partition,
                        offset=record.offset,
                        body=_record_body(record.value),
                        key=_decode_bytes(record.key),
                        headers=headers,
                    ),
                )
        return records

    async def commit(self, partition: int, next_offset: int) -> None:
        """Commit ``next_offset`` as the next record to read on ``partition``."""

        consumer = self._require()
        topic_partition = aiokafka.TopicPartition(self._topic, partition)
        await consumer.commit({topic_partition: next_offset})

    async def _wait_for_assignment(self) -> None:
        consumer = self._require()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 30
        while not consumer.assignment():
            if loop.time() >= deadline:
                raise TimeoutError(f"Kafka assigned no partitions for topic {self._topic}")
            await asyncio.sleep(0.05)

    def _require(self) -> aiokafka.AIOKafkaConsumer:
        if self._consumer is None:
            raise RuntimeError("Kafka consumer is not started")
        return self._consumer
