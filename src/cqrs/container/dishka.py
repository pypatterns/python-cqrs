from __future__ import annotations

import contextlib
import typing

from dishka import AsyncContainer

from cqrs.container.protocol import Container

T = typing.TypeVar("T")


class DishkaCQRSContainer(Container[AsyncContainer]):
    """
    Adapter bridging dishka :class:`~dishka.AsyncContainer` with python-cqrs.

    Request-scoped providers (``Scope.REQUEST``) are finalized when the CQRS
    scope opened via :meth:`open_scope` exits.
    """

    def __init__(self, container: AsyncContainer | None = None) -> None:
        self._external_container = container

    @classmethod
    def of(cls, container: AsyncContainer) -> DishkaCQRSContainer:
        """Wrap an already-opened dishka container (e.g. from FastAPI middleware)."""
        return cls(container)

    @property
    def external_container(self) -> AsyncContainer:
        if self._external_container is None:
            raise ValueError("External container not attached")
        return self._external_container

    def attach_external_container(self, container: AsyncContainer) -> None:
        self._external_container = container

    @contextlib.asynccontextmanager
    async def open_scope(
        self,
        context: typing.Mapping[type, typing.Any] | None = None,
    ) -> typing.AsyncIterator[Container]:
        ctx = dict(context) if context is not None else None
        async with self.external_container(context=ctx) as sub:
            yield DishkaCQRSContainer(sub)

    async def resolve(self, type_: typing.Type[T]) -> T:
        return await self.external_container.get(type_)
