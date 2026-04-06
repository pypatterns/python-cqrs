from __future__ import annotations

import abc
import functools
import typing

from cqrs.events.event import IEvent
from cqrs.requests.request import ReqT, ResT
from cqrs.response import IResponse


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

          async def handle(self, request: LoginCommand) -> typing.Optional[None ]:
              if self._auth_service.can_authenticate(request):
                  return await self._auth_service.authenticate(request)
              return await super().handle(request)
    """

    _next_handler: "typing.Optional[CORRequestHandler[ReqT, ResT] ]" = None

    def __class_getitem__(cls, params):  # type: ignore[override]
        """
        Backward-compatible generic subscription.

        Supports both forms:
        - CORRequestHandler[RequestType, ResponseType]
        - CORRequestHandler[RequestType]  # legacy shorthand
        """
        if not isinstance(params, tuple):
            params = (params,)
        if len(params) == 1:
            params = (
                params[0],
                typing.Optional[IResponse],
            )
        return super().__class_getitem__(params)  # pyright: ignore[reportAttributeAccessIssue]

    def set_next(
        self,
        handler: "CORRequestHandler[ReqT, ResT]",
    ) -> "CORRequestHandler[ReqT, ResT]":
        if self._next_handler is None:
            self._next_handler = handler

        return self._next_handler

    async def next(self, request: ReqT) -> typing.Optional[ResT]:
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
    async def handle(self, request: ReqT) -> typing.Optional[ResT]:
        raise NotImplementedError


CORRequestHandlerT = CORRequestHandler


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
