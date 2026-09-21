"""
Example: Outbox Pattern for Reliable Event Publishing

This example demonstrates the Outbox pattern for reliable event publishing to Kafka.
The system shows how to ensure events are reliably published even if the system
crashes, by storing events in an outbox before publishing them.

Use case: Guaranteeing event delivery in distributed systems. The Outbox pattern
ensures that events are persisted before being published, preventing event loss
during system failures. A separate process reads from the outbox and publishes
events to message brokers.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Step 1: Start Kafka Infrastructure
-----------------------------------
   docker compose -f ./docker-compose-dev.yml up -d

Wait for Kafka to be ready (usually takes 30-60 seconds).

Step 2: Run the Event Producer
-------------------------------
   python examples/outbox/kafka_outboxed_event_producing.py

The producer will:
- Read events from the mock outbox repository in batches
- Publish events to Kafka message broker
- Commit transactions after successful publishing
- Sleep for 10 seconds between batches (for demonstration)

Note: This example uses MockOutboxedEventRepository for demonstration. In production,
you would use a real database-backed outbox repository (e.g., SQLAlchemyOutboxedEventRepository).

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Outbox Repository:
   - Store events in outbox before publishing (MockOutboxedEventRepository)
   - Events are persisted and can survive system crashes
   - Repository provides methods to read events in batches

2. Event Producer:
   - EventProducer reads events from outbox in batches
   - Processes events and publishes them to message broker
   - Commits transactions only after successful publishing

3. Batch Processing:
   - Read multiple events at once for efficiency
   - Process batches sequentially to maintain order
   - Commit after each batch to ensure progress

4. Reliable Publishing:
   - Events are only removed from outbox after successful publishing
   - Failed publishes can be retried
   - Guarantees at-least-once delivery

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - Kafka adapter dependencies

Make sure Kafka is running:
   - Use docker-compose-dev.yml to start Kafka locally
   - Or configure connection to existing Kafka cluster

================================================================================
"""

import asyncio
import functools
import logging
import typing
import uuid
from collections import defaultdict

import cqrs
from cqrs.adapters import kafka as kafka_adapters
from cqrs.message_brokers import kafka
from cqrs.outbox import mock

logging.basicConfig(level=logging.DEBUG)

MOCK_STORAGE = defaultdict[
    uuid.UUID,
    typing.List[cqrs.NotificationEvent],
](lambda: [])

cqrs.OutboxedEventMap.register(
    "empty_event",
    cqrs.NotificationEvent[typing.Dict],
)

repository = mock.MockOutboxedEventRepository(
    session_factory=functools.partial(lambda: MOCK_STORAGE),
)

repository.add(
    cqrs.NotificationEvent[typing.Dict](
        event_name="empty_event",
        topic="empty_topic",
        payload={},
    ),
)
repository.add(
    cqrs.NotificationEvent[typing.Dict](
        event_name="empty_event",
        topic="empty_topic",
        payload={},
    ),
)
repository.add(
    cqrs.NotificationEvent[typing.Dict](
        event_name="empty_event",
        topic="empty_topic",
        payload={},
    ),
)


async def main():
    broker = kafka.KafkaMessageBroker(
        producer=kafka_adapters.kafka_producer_factory(dsn="localhost:9092"),
    )
    producer = cqrs.EventProducer(message_broker=broker, repository=repository)
    async for messages in producer.event_batch_generator():
        for message in messages:
            await producer.send_message(message)
        await producer.repository.commit()
        await asyncio.sleep(10)


if __name__ == "__main__":
    print(
        "Run kafka infrastructure with: `docker compose -f ./docker-compose-dev.yml up -d`",
    )
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())
