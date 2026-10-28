"""One request that yields progress. Full demo: examples/streaming/typed_streaming_handler.py."""

import di

import cqrs
from cqrs.bootstrap import requests as bootstrap


class ItemProgress(cqrs.Response):
    item_id: str
    status: str


class ProcessItems(cqrs.Request[ItemProgress]):
    item_ids: list[str]


class ItemDone(cqrs.DomainEvent, frozen=True):
    item_id: str


class ProcessItemsHandler(cqrs.StreamingRequestHandler[ProcessItems]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(self, request: ProcessItems):
        for item_id in request.item_ids:
            self._events.append(ItemDone(item_id=item_id))
            yield ItemProgress(item_id=item_id, status="done")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ProcessItems, ProcessItemsHandler)


def build_streaming_mediator() -> cqrs.StreamingRequestMediator:
    return bootstrap.bootstrap_streaming(
        di_container=di.Container(),
        commands_mapper=commands_mapper,
    )


async def read_progress(mediator: cqrs.StreamingRequestMediator, item_ids: list[str]) -> list[ItemProgress]:
    seen: list[ItemProgress] = []
    async for item in mediator.stream(ProcessItems(item_ids=item_ids)):
        seen.append(item)
    return seen
