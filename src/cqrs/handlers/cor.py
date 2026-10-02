from __future__ import annotations

import abc
import functools
import typing

from cqrs.models.event import IEvent
from cqrs.models.request import ReqT, ResT


class CORRequestHandler(abc.ABC, typing.Generic[ReqT, ResT]):
    """
    The chain of responsibility request handler interface.

    Implements the chain of responsibility pattern, allowing multiple handlers
    to process a request in sequence until one handles it successfully.

    Chain handler example::

      class AuthenticationHandler(CORRequestHandler[LoginCommand, None]):
          def __init__(self, auth_service: AuthServiceProtocol) -> None:
              self._auth_service = auth_service
              self.events: typing.List[IEvent] = []

          async def handle(self, request: LoginCommand) -> None | None:
              if self._auth_service.can_authenticate(request):
                  return await self._auth_service.authenticate(request)
              return await super().handle(request)
    """

    _next_handler: "CORRequestHandler[ReqT, ResT] | None" = None

    def set_next(
        self,
        handler: "CORRequestHandler[ReqT, ResT]",
    ) -> "CORRequestHandler[ReqT, ResT]":
        if self._next_handler is None:
            self._next_handler = handler

        return self._next_handler

    async def next(self, request: ReqT) -> ResT | None:
        if self._next_handler:
            return await self._next_handler.handle(request)

        return typing.cast(ResT, None)

    @property
    def events(self) -> typing.Sequence[IEvent]:
        """
        Events produced by this handler after :meth:`handle` was called.

        Override in subclasses to return follow-up events. By default returns
        an empty sequence.
        """
        return ()

    @abc.abstractmethod
    async def handle(self, request: ReqT) -> ResT | None:
        raise NotImplementedError


CORRequestHandlerT: typing.TypeAlias = CORRequestHandler


def build_chain(handlers: typing.List[CORRequestHandlerT]) -> CORRequestHandlerT:
    """
    Build a chain of responsibility from a list of handlers.

    Links handlers together in the order they appear in the list,
    with each handler pointing to the next one in sequence.

    Args:
        handlers: List of handlers to be linked together

    Returns:
        The first handler in the chain
    """

    def link(handler1, handler2):
        handler1.set_next(handler2)
        return handler2

    functools.reduce(link, handlers)
    return handlers[0]
