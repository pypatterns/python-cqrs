import asyncio
import typing

import pydantic
import pytest

import cqrs
from cqrs import events, Request, RequestMap, StreamingRequestHandler
from cqrs.message_brokers import kafka as kafka_broker


class ProcessItemsCommand(Request):
    item_ids: list[str] = pydantic.Field()


class ProcessItemResult(cqrs.Response):
    item_id: str = pydantic.Field()
    status: str = pydantic.Field()
    processed_count: int = pydantic.Field()


class ProcessItemsCommandHandler(
    StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
):
    def __init__(self) -> None:
        self.called = False
        self._events: typing.List[events.Event] = []
        self._processed_count = 0

    @property
    def events(self) -> typing.Sequence[events.IEvent]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(
        self,
        request: ProcessItemsCommand,
    ) -> typing.AsyncIterator[ProcessItemResult]:
        self.called = True
        self._processed_count = 0

        for item_id in request.item_ids:
            self._processed_count += 1
            result = ProcessItemResult(
                item_id=item_id,
                status="processed",
                processed_count=self._processed_count,
            )

            event = events.NotificationEvent(
                event_name="ItemProcessed",
                payload={
                    "item_id": item_id,
                    "processed_count": self._processed_count,
                },
            )
            self._events.append(event)

            yield result


class MockContainer:
    def __init__(self):
        self.command_handler = ProcessItemsCommandHandler()

    async def resolve(self, type_: typing.Type):
        if type_ == ProcessItemsCommandHandler:
            return self.command_handler
        return None


@pytest.fixture
async def streaming_mediator(
    kafka_producer,
) -> tuple[cqrs.StreamingRequestMediator, MockContainer]:
    container = MockContainer()
    broker = kafka_broker.KafkaMessageBroker(kafka_producer)
    event_emitter = events.EventEmitter(
        event_map=events.EventMap(),
        container=container,  # type: ignore
        message_broker=broker,
    )
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, ProcessItemsCommandHandler)
    event_map = events.EventMap()
    mediator = cqrs.StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=10,
    )
    return mediator, container


async def test_streaming_mediator_integration(
    streaming_mediator: tuple[cqrs.StreamingRequestMediator, MockContainer],
    kafka_producer,
) -> None:
    mediator, container = streaming_mediator
    handler: typing.Optional[ProcessItemsCommandHandler] = await container.resolve(
        ProcessItemsCommandHandler,
    )

    command = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])

    results = []
    async for result in mediator.stream(command):
        results.append(result)

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    assert handler
    assert handler.called

    assert len(results) == 3
    assert results[0].item_id == "item1"
    assert results[0].status == "processed"
    assert results[0].processed_count == 1
    assert results[1].item_id == "item2"
    assert results[1].processed_count == 2
    assert results[2].item_id == "item3"
    assert results[2].processed_count == 3

    assert kafka_producer.produce.call_count == 3
    assert len(handler.events) == 0


async def test_streaming_mediator_events_emitted_after_each_yield(
    streaming_mediator: tuple[cqrs.StreamingRequestMediator, MockContainer],
    kafka_producer,
) -> None:
    mediator, container = streaming_mediator
    _: typing.Optional[ProcessItemsCommandHandler] = await container.resolve(
        ProcessItemsCommandHandler,
    )

    command = ProcessItemsCommand(item_ids=["item1", "item2"])

    call_counts = []
    results = []
    async for result in mediator.stream(command):
        results.append(result)
        # Wait a bit for background task to complete
        await asyncio.sleep(0.05)
        call_counts.append(kafka_producer.produce.call_count)

    # Wait for final background tasks to complete
    await asyncio.sleep(0.1)

    assert call_counts[0] == 1
    assert call_counts[1] == 2

    assert len(results) == 2


async def test_streaming_mediator_empty_items_list(
    streaming_mediator: tuple[cqrs.StreamingRequestMediator, MockContainer],
    kafka_producer,
) -> None:
    mediator, container = streaming_mediator
    handler: typing.Optional[ProcessItemsCommandHandler] = await container.resolve(
        ProcessItemsCommandHandler,
    )

    command = ProcessItemsCommand(item_ids=[])

    results = []
    async for result in mediator.stream(command):
        results.append(result)

    assert handler
    assert handler.called
    assert len(results) == 0
    assert kafka_producer.produce.call_count == 0


async def test_streaming_mediator_single_item(
    streaming_mediator: tuple[cqrs.StreamingRequestMediator, MockContainer],
    kafka_producer,
) -> None:
    mediator, container = streaming_mediator
    handler: typing.Optional[ProcessItemsCommandHandler] = await container.resolve(
        ProcessItemsCommandHandler,
    )

    command = ProcessItemsCommand(item_ids=["single_item"])

    results = []
    async for result in mediator.stream(command):
        results.append(result)

    # Wait for background tasks to complete
    await asyncio.sleep(0.1)

    assert handler
    assert handler.called
    assert len(results) == 1
    assert results[0].item_id == "single_item"
    assert results[0].processed_count == 1
    assert kafka_producer.produce.call_count == 1
