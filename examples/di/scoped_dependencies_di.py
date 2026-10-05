"""
Scoped dependencies with the ``di`` package — generator UoW finalized after handle.
"""

from __future__ import annotations

import asyncio
import typing

import di
from di import dependent

import cqrs
from cqrs.bootstrap import requests as bootstrap

ORDER: list[str] = []


class IUoW(typing.Protocol):
    async def commit(self) -> None: ...


class UoW:
    async def commit(self) -> None:
        ORDER.append("commit")

    async def close(self) -> None:
        ORDER.append("close")


async def uow_provider() -> typing.AsyncIterator[UoW]:
    uow = UoW()
    ORDER.append("enter")
    try:
        yield uow
    finally:
        await uow.close()


class CancelTask(cqrs.Request):
    task_id: int


class CancelTaskHandler(cqrs.RequestHandler[CancelTask, None]):
    def __init__(self, uow: IUoW) -> None:
        self._uow = uow

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return []

    async def handle(self, request: CancelTask) -> None:
        ORDER.append(f"handle:{request.task_id}")
        await self._uow.commit()


def setup_di() -> di.Container:
    container = di.Container()
    container.bind(
        di.bind_by_type(
            dependent.Dependent(uow_provider, scope="request"),
            IUoW,
        ),
    )
    return container


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CancelTask, CancelTaskHandler)


async def main() -> None:
    mediator = bootstrap.bootstrap(
        di_container=setup_di(),
        commands_mapper=commands_mapper,
        scope_strategy=cqrs.ScopeStrategy.SEND,
    )
    await mediator.send(CancelTask(task_id=42))
    assert ORDER == ["enter", "handle:42", "commit", "close"]
    print("OK:", ORDER)


if __name__ == "__main__":
    asyncio.run(main())
