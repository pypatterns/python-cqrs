"""
Message broker protocol.

[AI Instruction]
This package exports ``Message`` and ``MessageBroker`` only. Concrete
brokers are submodules. Kafka is ``cqrs.message_brokers.kafka.KafkaMessageBroker``
together with ``cqrs.adapters.kafka.kafka_producer_factory`` (extra
``kafka``). AMQP is ``cqrs.message_brokers.amqp.AMQPMessageBroker`` (extra
``rabbit``). Pass the broker to bootstrap or to ``cqrs.EventProducer``.
"""

from cqrs.message_brokers.protocol import Message, MessageBroker

__all__ = (
    "Message",
    "MessageBroker",
)
