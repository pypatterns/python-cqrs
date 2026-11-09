import typing

class ScopeState: ...

class _Solved:
    async def execute_async(
        self,
        *,
        executor: typing.Any,
        state: ScopeState,
    ) -> typing.Any: ...

class Container:
    def solve(
        self,
        dependent: typing.Any,
        *,
        scopes: list[typing.Any],
    ) -> _Solved: ...
    def enter_scope(self, scope: typing.Any) -> typing.AsyncContextManager[ScopeState]: ...
