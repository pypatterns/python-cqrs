"""
Example: Consuming Events from Kafka

This example demonstrates how to consume events from Kafka topics and process
them using CQRS event handlers. The system shows how to integrate CQRS with Kafka
for event-driven microservices.

Use case: Building event-driven microservices that consume events from Kafka.
Events are deserialized from Kafka messages and processed by CQRS event handlers,
enabling scalable, asynchronous event processing.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Step 1: Start Kafka Infrastructure
-----------------------------------
   docker compose -f ./docker-compose-dev.yml up -d

Wait for Kafka to be ready (usually takes 30-60 seconds).

Step 2: Send Events to Kafka
------------------------------
The example will print instructions on how to send events. You can send events
using Kafka console producer or any Kafka client:

   # Example event payload (JSON):
   {
     "event_id": "123e4567-e89b-12d3-a456-426614174000",
     "event_timestamp": "2024-01-01T00:00:00Z",
     "event_name": "HelloWorldECSTEvent",
     "topic": "hello_world",
     "payload": {
       "hello": "Hello",
       "world": "World"
     }
   }

Step 3: Run the Consumer
-------------------------
   python examples/kafka_event_consuming.py

The consumer will:
- Connect to Kafka broker at localhost:9092
- Subscribe to "hello_world" topic
- Process incoming events and dispatch them to event handlers
- Print "Hello World" for each received event

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Kafka Consumer Setup:
   - Configure KafkaBroker with bootstrap servers
   - Set up consumer group, offset reset, and commit settings
   - Use FastStream for Kafka integration

2. JSON Deserialization:
   - Use JsonDeserializer to deserialize Kafka messages
   - Deserialize into typed NotificationEvent objects
   - Handle deserialization errors gracefully

3. Event Handler Registration:
   - Register event handlers using events_mapper
   - Map NotificationEvent types to their handlers
   - EventMediator dispatches events to handlers

4. Error Handling:
   - Check for DeserializeJsonError before processing
   - Acknowledge messages only after successful processing
   - Handle deserialization failures without crashing

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - di (dependency injection)
   - faststream (Kafka integration)
   - orjson (JSON serialization)

Make sure Kafka is running:
   - Use docker-compose-dev.yml to start Kafka locally
   - Or configure connection to existing Kafka cluster

================================================================================
"""

import asyncio
import logging
import typing

import di
import faststream
import orjson
import pydantic
from faststream import kafka, types

import cqrs
from cqrs import deserializers
from cqrs.events import bootstrap

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("aiokafka").setLevel(logging.ERROR)

broker = kafka.KafkaBroker(bootstrap_servers=["localhost:9092"])
app = faststream.FastStream(broker)


async def empty_message_decoder(
    msg: kafka.KafkaMessage,
    original_decoder: typing.Callable[
        [kafka.KafkaMessage],
        typing.Awaitable[types.DecodedMessage],
    ],
) -> typing.Optional[types.DecodedMessage]:
    """
    Decode a kafka message and return it if it is not empty.
    """
    if not msg.body:
        return None
    return await original_decoder(msg)


class HelloWorldPayload(pydantic.BaseModel):
    hello: str = pydantic.Field(default="Hello")
    world: str = pydantic.Field(default="World")


class HelloWorldECSTEventHandler(
    cqrs.EventHandler[cqrs.NotificationEvent[HelloWorldPayload]],
):
    async def handle(self, event: cqrs.NotificationEvent[HelloWorldPayload]) -> None:
        print(f"{event.payload.hello} {event.payload.world}")  # type: ignore


def events_mapper(mapper: cqrs.EventMap) -> None:
    """Maps events to handlers."""
    mapper.bind(cqrs.NotificationEvent[HelloWorldPayload], HelloWorldECSTEventHandler)


def mediator_factory() -> cqrs.EventMediator:
    return bootstrap.bootstrap(
        di_container=di.Container(),
        events_mapper=events_mapper,
    )


@broker.subscriber(
    "hello_world",
    group_id="examples",
    auto_commit=False,
    auto_offset_reset="earliest",
    value_deserializer=deserializers.JsonDeserializer(
        model=cqrs.NotificationEvent[HelloWorldPayload],
    ),
    decoder=empty_message_decoder,
)
async def hello_world_event_handler(
    body: typing.Union[cqrs.NotificationEvent[HelloWorldPayload], typing.Optional[deserializers.DeserializeJsonError]],
    msg: kafka.KafkaMessage,
    mediator: cqrs.EventMediator = faststream.Depends(mediator_factory),
):
    if not isinstance(body, deserializers.DeserializeJsonError) and body is not None:
        await mediator.send(body)
    await msg.ack()


if __name__ == "__main__":
    ev = cqrs.NotificationEvent[HelloWorldPayload](
        event_name="HelloWorldECSTEvent",
        topic="hello_world",
        payload=HelloWorldPayload(),
    )
    print(
        f"1. Run kafka infrastructure with: `docker compose -f ./docker-compose-dev.yml up -d`\n"
        f"2. Send to kafka topic `hello_world` event: {orjson.dumps(ev.to_dict()).decode()}",
    )
    asyncio.run(app.run())
