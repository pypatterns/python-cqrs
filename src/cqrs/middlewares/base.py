import functools
import typing

from cqrs.saga.models import SagaContext
from cqrs.requests.request import ReqT, ResT

HandleType = typing.Callable[[ReqT], typing.Awaitable[typing.Optional[ResT]]]


class Middleware(typing.Protocol[ReqT, ResT]):
    async def __call__(self, request: ReqT, handle: HandleType) -> typing.Optional[ResT]:
        raise NotImplementedError


class MiddlewareChain:
    def __init__(self) -> None:
        self._chain: typing.List[Middleware[typing.Any, typing.Any]] = []

    def set(self, chain: typing.List[Middleware[typing.Any, typing.Any]]) -> None:
        self._chain = chain

    def add(self, middleware: Middleware[typing.Any, typing.Any]) -> None:
        self._chain.append(middleware)

    def wrap(self, handle: HandleType) -> HandleType:
        for middleware in reversed(self._chain):
            handle = functools.partial(middleware.__call__, handle=handle)

        return handle


# TypeVar for SagaMiddleware protocol
SagaContextT = typing.TypeVar("SagaContextT", bound=SagaContext)

SagaHandlerType = typing.Callable[[SagaContextT], typing.Awaitable[None]]


class SagaMiddleware(typing.Protocol[SagaContextT]):
    async def __call__(
        self,
        context: SagaContextT,
        handler: typing.Callable[[SagaContextT], typing.Awaitable[None]],
    ) -> None:
        pass


class SagaMiddlewareChain:
    def __init__(self) -> None:
        pass
