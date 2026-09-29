"""
FastAPI + bind_scope — attach an externally opened scope (e.g. dishka middleware).

``bind_scope`` is for ScopeStrategy.SEND (or NONE): the outer scope is reused.
Under HANDLER the framework always opens a nested scope (``reuse_existing=False``),
so ``bind_scope`` does not share a UoW across command + domain-event handlers.

Run the CLI demo without FastAPI installed::

    python examples/di/scoped_dependencies_fastapi.py
"""

from __future__ import annotations

import asyncio
import typing

import di
from di import dependent

import cqrs
from cqrs.container.di import DIContainer
from cqrs.container.scope import ScopeStrategy, bind_scope, enter_scope
from cqrs.requests import bootstrap

UOW_IDS: list[int] = []


class UoW:
    def __init__(self) -> None:
        self.id = id(self)

    async def close(self) -> None:
        pass


async def uow_provider() -> typing.AsyncIterator[UoW]:
    uow = UoW()
    try:
        yield uow
    finally:
        await uow.close()


class DoCmd(cqrs.Request):
    pass


class DidWork(cqrs.DomainEvent, frozen=True):
    pass


class DoCmdHandler(cqrs.RequestHandler[DoCmd, None]):
    def __init__(self, uow: UoW) -> None:
        self.uow = uow
        self._events: list[cqrs.Event] = [DidWork()]

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return self._events

    async def handle(self, request: DoCmd) -> None:
        UOW_IDS.append(self.uow.id)


class DidWorkHandler(cqrs.EventHandler[DidWork]):
    def __init__(self, uow: UoW) -> None:
        self.uow = uow

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return []

    async def handle(self, event: DidWork) -> None:
        UOW_IDS.append(self.uow.id)


def setup_container() -> DIContainer:
    external = di.Container()
    external.bind(
        di.bind_by_type(
            dependent.Dependent(uow_provider, scope="request"),
            UoW,
        ),
    )
    container = DIContainer()
    container.attach_external_container(external)
    return container


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(DoCmd, DoCmdHandler)


def events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(DidWork, DidWorkHandler)


CONTAINER = setup_container()


def mediator_send() -> cqrs.RequestMediator:
    return bootstrap.bootstrap(
        di_container=CONTAINER,
        commands_mapper=commands_mapper,
        domain_events_mapper=events_mapper,
        scope_strategy=ScopeStrategy.SEND,
    )


def mediator_handler() -> cqrs.RequestMediator:
    return bootstrap.bootstrap(
        di_container=CONTAINER,
        commands_mapper=commands_mapper,
        domain_events_mapper=events_mapper,
        scope_strategy=ScopeStrategy.HANDLER,
    )


def create_app() -> typing.Any:
    """Build FastAPI app (requires ``python-cqrs[examples]`` / fastapi)."""
    from fastapi import Depends, FastAPI

    app = FastAPI()

    @app.post("/send")
    async def with_send(
        mediator: cqrs.RequestMediator = Depends(mediator_send),
    ) -> dict:
        # bind_scope / enter_scope + SEND → shared UoW for command + events
        UOW_IDS.clear()
        async with CONTAINER.open_scope() as scoped:
            async with bind_scope(scoped):
                await mediator.send(DoCmd())
        shared = len(set(UOW_IDS)) == 1
        return {"strategy": "SEND", "shared_uow": shared, "ids": list(UOW_IDS)}

    @app.post("/handler")
    async def with_handler(
        mediator: cqrs.RequestMediator = Depends(mediator_handler),
    ) -> dict:
        # HANDLER ignores an outer bind_scope for sharing — nested scopes per handler
        UOW_IDS.clear()
        await mediator.send(DoCmd())
        separate = len(set(UOW_IDS)) == 2
        return {
            "strategy": "HANDLER",
            "separate_uow": separate,
            "ids": list(UOW_IDS),
        }

    return app


# Expose ``app`` only when FastAPI is installed (uvicorn ``examples.di...:app``).
try:
    app = create_app()
except ImportError:
    app = None  # type: ignore[assignment]


async def _demo() -> None:
    """Run without uvicorn / FastAPI for a quick sanity check."""
    med = mediator_send()
    async with enter_scope(CONTAINER):
        await med.send(DoCmd())
    assert len(set(UOW_IDS)) == 1
    print("SEND shared UoW:", UOW_IDS)

    UOW_IDS.clear()
    med_h = mediator_handler()
    await med_h.send(DoCmd())
    assert len(set(UOW_IDS)) == 2
    print("HANDLER separate UoW:", UOW_IDS)


if __name__ == "__main__":
    asyncio.run(_demo())
