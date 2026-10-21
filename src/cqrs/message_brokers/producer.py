import logging
import typing

from cqrs.message_brokers import protocol as broker_protocol
from cqrs.outbox import repository as repository_protocol
from cqrs.serializers.default import headers_for_content_type

logger = logging.getLogger("cqrs")
logger.setLevel(logging.DEBUG)


class EventProducer:
    """
    Drains an outbox repository and publishes events through a message broker.

    Does not depend on SQLAlchemy; any ``OutboxedEventRepository`` implementation
    works (SQLAlchemy outbox requires ``python-cqrs[sqlalchemy]``).

    [AI Instruction]
    Run this in a separate publisher process. It reads unpublished outbox
    rows and sends them to the broker. It does not replace ``repository.add``
    inside a command. After each batch, ``commit`` the repository so status
    updates stick.
    """

    def __init__(
        self,
        message_broker: broker_protocol.MessageBroker,
        repository: repository_protocol.OutboxedEventRepository,
    ):
        self.message_broker = message_broker
        self.repository = repository

    async def event_batch_generator(
        self,
        batch_size: int = 100,
    ) -> typing.AsyncIterator[typing.List[repository_protocol.OutboxedEvent]]:
        """
        Return messages for produce.

        [AI Instruction]
        Poll forever. Each yield is one batch from ``repository.get_many``.
        Send every message, then ``await repository.commit()``, then sleep
        before the next poll. Stop the loop from outside; this generator
        does not end.
        """
        while True:
            yield await self.repository.get_many(batch_size)

    async def send_message(self, event: repository_protocol.OutboxedEvent):
        """
        [AI Instruction]
        Publish one outbox row and set its status to produced or not
        produced. Call it for each item from ``event_batch_generator``. The
        row stays retryable when the broker rejects it.
        """
        try:
            logger.debug(f"Send event {event.event.event_id} into topic {event.topic}")
            content_type, headers = headers_for_content_type(
                event.content_type,
                event.event,
            )
            await self.message_broker.send_message(
                broker_protocol.Message(
                    message_name=event.event.event_name,
                    message_id=event.event.event_id,
                    topic=event.topic,
                    payload=event.event.to_dict(),
                    payload_bytes=event.payload_bytes,
                    content_type=content_type,
                    headers=headers,
                ),
            )
        except Exception as error:
            logger.error(
                f"Error while producing event {event.event.event_id} to kafka broker: {error}",
            )
            if not self.repository:
                return
            await self.repository.update_status(
                event.id,
                repository_protocol.EventStatus.NOT_PRODUCED,  # type: ignore[arg-type]
            )
        else:
            if not self.repository:
                return
            await self.repository.update_status(
                event.id,
                repository_protocol.EventStatus.PRODUCED,  # type: ignore[arg-type]
            )
