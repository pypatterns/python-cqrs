from __future__ import annotations

import contextlib
import typing

import di
from di import dependent, executors

from cqrs.container.protocol import Container

T = typing.TypeVar("T")


class DIContainer(Container[di.Container]):
    """
    Adapter for the ``di`` package with async request-scoped dependencies.

    When a scope is opened via :meth:`open_scope`, generator providers stay alive
    until the scope exits. Legacy :meth:`resolve` on the root container still
    opens a one-shot scope (previous behaviour).
    """

    def __init__(self, scope: typing.Any = "request") -> None:
        self._scope = scope
        self._external_container: di.Container | None = None
        self._solve_cache: dict[type, typing.Any] = {}
        self._state: di.ScopeState | None = None

    @property
    def external_container(self) -> di.Container:
        if self._external_container is None:
            raise ValueError("External container not attached")
        return self._external_container

    def attach_external_container(self, container: di.Container) -> None:
        self._external_container = container
        self._solve_cache.clear()

    def _get_solved(self, type_: typing.Type[T]) -> typing.Any:
        cached = self._solve_cache.get(type_)
        if cached is not None:
            return cached
        solved = self.external_container.solve(
            dependent.Dependent(type_, scope=self._scope),
            scopes=[self._scope],
        )
        self._solve_cache[type_] = solved
        return solved

    def _clone_scoped(self, state: di.ScopeState) -> DIContainer:
        scoped = DIContainer(scope=self._scope)
        scoped._external_container = self._external_container
        scoped._solve_cache = self._solve_cache
        scoped._state = state
        return scoped

    @contextlib.asynccontextmanager
    async def open_scope(
        self,
        context: typing.Mapping[type, typing.Any] | None = None,
    ) -> typing.AsyncIterator[Container]:
        # ``context`` is accepted for SupportsScope compatibility (used by dishka);
        # the ``di`` package does not inject request context into scopes.
        _ = context
        async with self.external_container.enter_scope(self._scope) as state:
            yield self._clone_scoped(state)

    async def resolve(self, type_: typing.Type[T]) -> T:
        executor = executors.AsyncExecutor()
        solved = self._get_solved(type_)
        if self._state is not None:
            return await solved.execute_async(executor=executor, state=self._state)
        # Legacy one-shot scope when no ambient / open_scope is active
        async with self.external_container.enter_scope(self._scope) as state:
            return await solved.execute_async(executor=executor, state=state)
