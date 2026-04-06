import logging
import typing

from sqlalchemy.ext.asyncio import session as sql_session

from cqrs.message_brokers import protocol as broker_protocol
from cqrs.outbox import repository as repository_protocol

logger = logging.getLogger("cqrs")
logger.setLevel(logging.DEBUG)

SessionFactory = typing.Callable[[], sql_session.AsyncSession]


class EventProducer:
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
        """Return messages for produce"""
        while True:
            yield await self.repository.get_many(batch_size)

    async def send_message(self, event: repository_protocol.OutboxedEvent):
        try:
            logger.debug(f"Send event {event.event.event_id} into topic {event.topic}")
            await self.message_broker.send_message(
                broker_protocol.Message(
                    message_name=event.event.event_name,
                    message_id=event.event.event_id,
                    topic=event.topic,
                    payload=event.event.to_dict(),
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
