"""
dependency-injector has no per-request scope — Resource is process-level;
use a Factory for per-request UoW (or switch to ``di`` / dishka for generator scopes).
"""

from __future__ import annotations

import asyncio
import typing

from dependency_injector import containers, providers

import cqrs
from cqrs.container.dependency_injector import DependencyInjectorCQRSContainer
from cqrs.bootstrap import requests as bootstrap

CREATED: list[str] = []


class ConnectionPool:
    """Process-level resource (initialized once)."""

    def __init__(self) -> None:
        CREATED.append("pool")


class UoW:
    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool
        CREATED.append("uow")

    async def commit(self) -> None:
        pass


class CancelTask(cqrs.Request):
    task_id: int


class CancelTaskHandler(cqrs.RequestHandler[CancelTask, None]):
    def __init__(self, uow: UoW) -> None:
        self._uow = uow

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return []

    async def handle(self, request: CancelTask) -> None:
        await self._uow.commit()
        print(f"cancelled {request.task_id} via {id(self._uow)}")


class ApplicationContainer(containers.DeclarativeContainer):
    # Process-level resource — fine for pools / clients
    pool = providers.Singleton(ConnectionPool)
    # Per-resolve instance (not a true request scope with finalization)
    uow = providers.Factory(UoW, pool=pool)
    cancel_task_handler = providers.Factory(CancelTaskHandler, uow=uow)


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CancelTask, CancelTaskHandler)


async def main() -> None:
    app = ApplicationContainer()
    cqrs_container = DependencyInjectorCQRSContainer()
    cqrs_container.attach_external_container(app)

    mediator = bootstrap.bootstrap(
        di_container=cqrs_container,
        commands_mapper=commands_mapper,
        scope_strategy=cqrs.ScopeStrategy.SEND,
    )
    await mediator.send(CancelTask(task_id=1))
    await mediator.send(CancelTask(task_id=2))
    # Singleton pool once; Factory UoW twice — no SupportsScope / generator cleanup
    assert CREATED.count("pool") == 1
    assert CREATED.count("uow") == 2
    print("OK:", CREATED)
    print("Note: for async-generator UoW scopes use di or dishka adapters.")


if __name__ == "__main__":
    asyncio.run(main())
