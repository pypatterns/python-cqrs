"""
Example: typed streaming request handler (one type argument).

The command is ``Request[ItemResult]``. ``StreamingRequestHandler[Command]``
yields ``ItemResult`` without an element annotation on ``handle``.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

   python examples/streaming/typed_streaming_handler.py

================================================================================
REQUIREMENTS
================================================================================

   - cqrs (this package)
   - di (dependency injection)

No Docker and no pydantic extra.
================================================================================
"""

import asyncio

import di

import cqrs
from cqrs.bootstrap import requests as bootstrap


class ItemResult(cqrs.Response):
    item_id: str
    status: str


class ProcessItemsCommand(cqrs.Request[ItemResult]):
    item_ids: list[str]


class ProcessItemsCommandHandler(cqrs.StreamingRequestHandler[ProcessItemsCommand]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(self, request: ProcessItemsCommand):
        for item_id in request.item_ids:
            yield ItemResult(item_id=item_id, status="done")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ProcessItemsCommand, ProcessItemsCommandHandler)


async def main() -> None:
    mediator = bootstrap.bootstrap_streaming(
        di_container=di.Container(),
        commands_mapper=commands_mapper,
    )
    seen: list[ItemResult] = []
    async for item in mediator.stream(ProcessItemsCommand(item_ids=["a", "b"])):
        seen.append(item)
        print(item.item_id, item.status)
    assert [item.item_id for item in seen] == ["a", "b"]
    assert all(item.status == "done" for item in seen)


if __name__ == "__main__":
    asyncio.run(main())
