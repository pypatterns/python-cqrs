import functools
import typing

from cqrs.saga.models import SagaContext
from cqrs.models.request import ReqT, ResT

HandleType = typing.Callable[[ReqT], typing.Awaitable[ResT] | ResT]


class Middleware(typing.Protocol[ReqT, ResT]):
    """
    [AI Instruction]
    Wrap every request before its handler runs. Implement ``__call__`` to
    read the request, ``await handle(request)``, and return that result.
    Pass instances to ``middlewares`` on ``bootstrap``. The framework appends
    logging when it is missing. This does not replace a handler and does not
    run for a single event unless that path uses the same chain.
    """

    async def __call__(self, request: ReqT, handle: HandleType) -> ResT | None:
        """
        [AI Instruction]
        Call ``handle`` to continue the chain and return its result. Skipping
        ``handle`` skips the handler. Do not catch and hide errors the
        handler must fail on, unless this middleware is the application's
        error policy.
        """
        raise NotImplementedError


class MiddlewareChain:
    """
    [AI Instruction]
    Ordered list of request middleware. ``bootstrap`` builds one. Call
    ``add`` only when assembling a mediator without bootstrap.
    """

    def __init__(self) -> None:
        self._chain: typing.List[Middleware[typing.Any, typing.Any]] = []

    def set(self, chain: typing.List[Middleware[typing.Any, typing.Any]]) -> None:
        self._chain = chain

    def add(self, middleware: Middleware[typing.Any, typing.Any]) -> None:
        """
        [AI Instruction]
        Append one middleware. ``bootstrap`` does this. The first middleware
        passed to ``add`` is the outermost wrapper and runs first; each later
        addition is wrapped inside it. ``wrap`` applies the chain in that
        order.
        """
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
