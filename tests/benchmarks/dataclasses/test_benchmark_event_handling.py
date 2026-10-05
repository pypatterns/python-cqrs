"""Benchmarks for event handling performance (dataclass DCEvent)."""

import asyncio
import typing

import cqrs
import di
import pytest
from cqrs.bootstrap import events as bootstrap


class UserJoinedEvent(cqrs.DCEvent):
    user_id: str
    meeting_id: str


class UserJoinedEventHandler(cqrs.EventHandler[UserJoinedEvent]):
    def __init__(self):
        self.processed_events: typing.List[UserJoinedEvent] = []

    async def handle(self, event: UserJoinedEvent) -> None:
        self.processed_events.append(event)


def events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(UserJoinedEvent, UserJoinedEventHandler)


@pytest.fixture
def event_mediator():
    return bootstrap.bootstrap(
        di_container=di.Container(),
        events_mapper=events_mapper,
    )


@pytest.mark.benchmark
def test_benchmark_event_processing(benchmark, event_mediator):
    """Benchmark event processing performance."""
    event = UserJoinedEvent(user_id="user_1", meeting_id="meeting_1")

    async def run():
        await event_mediator.send(event)

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_multiple_events(benchmark, event_mediator):
    """Benchmark processing multiple events in sequence."""
    events = [UserJoinedEvent(user_id=f"user_{i}", meeting_id="meeting_1") for i in range(10)]

    async def run():
        for evt in events:
            await event_mediator.send(evt)

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_notification_event(benchmark):
    """Benchmark notification event creation and serialization."""

    def run():
        event = cqrs.NotificationEvent[UserJoinedEvent](
            event_name="UserJoined",
            topic="test_topic",
            payload=UserJoinedEvent(user_id="user_1", meeting_id="meeting_1"),
        )
        return event.to_dict()

    benchmark(run)
