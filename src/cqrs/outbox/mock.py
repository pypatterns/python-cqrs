import typing

import cqrs
from cqrs.outbox import repository


class MockOutboxedEventRepository(repository.OutboxedEventRepository):
    COUNTER: typing.ClassVar = 0

    def __init__(self, session_factory: typing.Callable[[], typing.Dict]):
        self.session = session_factory()

    async def __aenter__(self) -> typing.Dict:
        return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    def add(self, event: cqrs.INotificationEvent) -> None:
        MockOutboxedEventRepository.COUNTER += 1
        self.session[MockOutboxedEventRepository.COUNTER] = repository.OutboxedEvent(
            id=MockOutboxedEventRepository.COUNTER,
            event=event,
            topic=event.topic,
            status=repository.EventStatus.NEW,
        )

    async def get_many(
        self,
        batch_size: int = 100,
        topic: typing.Optional[typing.Text] = None,
    ) -> typing.List[repository.OutboxedEvent]:
        return list(
            filter(lambda e: topic == e.topic, self.session.values()) if topic else list(self.session.values()),
        )

    async def update_status(
        self,
        outboxed_event_id: int,
        new_status: repository.EventStatus,
    ):
        if outboxed_event_id not in self.session:
            return
        if new_status is repository.EventStatus.PRODUCED:
            del self.session[outboxed_event_id]

    async def commit(self):
        pass

    async def rollback(self):
        pass
