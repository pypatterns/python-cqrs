"""
Custom container with SupportsScope — template for third-party DI libraries.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import typing

import cqrs
from cqrs.container.protocol import Container
from cqrs.requests import bootstrap

T = typing.TypeVar("T")


class SimpleExternal:
    def __init__(self) -> None:
        self.providers: dict[type, typing.Callable[[], typing.Any]] = {}

    def register(self, type_: type, factory: typing.Callable[[], typing.Any]) -> None:
        self.providers[type_] = factory


class MyCQRSContainer(Container[SimpleExternal]):
    def __init__(self, external: SimpleExternal | None = None) -> None:
        self._external = external or SimpleExternal()
        self._scoped_values: dict[type, typing.Any] = {}
        self._is_scoped = False

    @property
    def external_container(self) -> SimpleExternal:
        return self._external

    def attach_external_container(self, container: SimpleExternal) -> None:
        self._external = container

    async def resolve(self, type_: typing.Type[T]) -> T:
        if type_ in self._scoped_values:
            return typing.cast(T, self._scoped_values[type_])
        factory = self._external.providers.get(type_)
        if factory is not None:
            value = factory()
            if self._is_scoped:
                self._scoped_values[type_] = value
            return typing.cast(T, value)
        hints = typing.get_type_hints(type_.__init__)
        kwargs = {name: await self.resolve(ann) for name, ann in hints.items() if name != "return"}
        return type_(**kwargs)  # type: ignore[call-arg]

    @contextlib.asynccontextmanager
    async def open_scope(
        self,
        context: typing.Mapping[type, typing.Any] | None = None,
    ) -> typing.AsyncIterator[Container]:
        _ = context
        scoped = MyCQRSContainer(self._external)
        scoped._is_scoped = True
        try:
            yield scoped
        finally:
            for value in scoped._scoped_values.values():
                close = getattr(value, "close", None)
                if close is not None:
                    result = close()
                    if inspect.isawaitable(result):
                        await result


class FakeUoW:
    instances: list[FakeUoW] = []

    def __init__(self) -> None:
        FakeUoW.instances.append(self)
        self.closed = False

    def close(self) -> None:
        self.closed = True


class DoWork(cqrs.Request):
    pass


class DoWorkHandler(cqrs.RequestHandler[DoWork, None]):
    def __init__(self, uow: FakeUoW) -> None:
        self.uow = uow

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return []

    async def handle(self, request: DoWork) -> None:
        print("work with", id(self.uow))


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(DoWork, DoWorkHandler)


async def main() -> None:
    FakeUoW.instances.clear()
    external = SimpleExternal()
    external.register(FakeUoW, FakeUoW)
    container = MyCQRSContainer(external)
    mediator = bootstrap.bootstrap(
        di_container=container,
        commands_mapper=commands_mapper,
        scope_strategy=cqrs.ScopeStrategy.SEND,
    )
    await mediator.send(DoWork())
    assert FakeUoW.instances[0].closed is True
    print("OK: custom open_scope finalized UoW")


if __name__ == "__main__":
    asyncio.run(main())
