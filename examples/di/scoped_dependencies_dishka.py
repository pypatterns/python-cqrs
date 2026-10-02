"""
Scoped dependencies with dishka (issue #70) — shared UoW for command + domain event.

Handlers must be registered as dishka providers (Scope.REQUEST) so the container
can construct them with injected dependencies.
"""

from __future__ import annotations

import asyncio
import typing

from dishka import Provider, Scope, make_async_container, provide

import cqrs
from cqrs.container.dishka import DishkaCQRSContainer
from cqrs.container.scope import ScopeStrategy
from cqrs.bootstrap import requests as bootstrap

SEEN: list[int] = []


class InMemoryUoW:
    def __init__(self) -> None:
        self.id = id(self)

    async def cancel(self, task_id: int) -> None:
        SEEN.append(task_id)

    async def commit(self) -> None:
        pass


class CancelTask(cqrs.Request):
    task_id: int


class TaskCancelled(cqrs.DomainEvent, frozen=True):
    task_id: int


class CancelTaskHandler(cqrs.RequestHandler[CancelTask, None]):
    def __init__(self, uow: InMemoryUoW) -> None:
        self._uow = uow
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return self._events

    async def handle(self, request: CancelTask) -> None:
        await self._uow.cancel(request.task_id)
        await self._uow.commit()
        self._events.append(TaskCancelled(task_id=request.task_id))
        CancelTaskHandler.uow_id = self._uow.id  # type: ignore[attr-defined]


class TaskCancelledHandler(cqrs.EventHandler[TaskCancelled]):
    def __init__(self, uow: InMemoryUoW) -> None:
        self._uow = uow

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return []

    async def handle(self, event: TaskCancelled) -> None:
        TaskCancelledHandler.uow_id = self._uow.id  # type: ignore[attr-defined]


class AppProvider(Provider):
    @provide(scope=Scope.REQUEST)
    async def uow(self) -> typing.AsyncIterator[InMemoryUoW]:
        uow = InMemoryUoW()
        try:
            yield uow
        finally:
            pass

    cancel_handler = provide(CancelTaskHandler, scope=Scope.REQUEST)
    event_handler = provide(TaskCancelledHandler, scope=Scope.REQUEST)


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CancelTask, CancelTaskHandler)


def events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(TaskCancelled, TaskCancelledHandler)


async def main() -> None:
    dishka = make_async_container(AppProvider())
    mediator = bootstrap.bootstrap(
        di_container=DishkaCQRSContainer(dishka),
        commands_mapper=commands_mapper,
        domain_events_mapper=events_mapper,
        scope_strategy=ScopeStrategy.SEND,
    )
    await mediator.send(CancelTask(task_id=7))
    assert CancelTaskHandler.uow_id == TaskCancelledHandler.uow_id  # type: ignore[attr-defined]
    assert SEEN == [7]
    await dishka.close()
    print("OK: command and event shared the same UoW")


if __name__ == "__main__":
    asyncio.run(main())
