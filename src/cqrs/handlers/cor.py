"""
Chain-of-responsibility request handlers.

[AI Instruction]
Import ``CORRequestHandler`` from ``cqrs.handlers``. Import ``build_chain``
from this module when the application holds the chain itself. Registration
of a chain is ``RequestMap.bind(RequestType, [First, Second])`` on ``cqrs``.
"""

from __future__ import annotations

import abc
import functools
import typing

from cqrs.generic_utils import handler_class_getitem
from cqrs.models.event import IEvent
from cqrs.models.request import ReqT, ResT


class CORRequestHandler(abc.ABC, typing.Generic[ReqT, ResT]):
    """
    The chain of responsibility request handler interface.

    Implements the chain of responsibility pattern, allowing multiple handlers
    to process a request in sequence until one handles it successfully.

    [AI Instruction]
    Subclass this when several strategies can handle one request and the first
    that applies should take it. Register the ordered list with
    ``RequestMap.bind(RequestType, [First, Second])``. A handler that always
    runs is ``RequestHandler``, not a chain.

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

    def __class_getitem__(cls, params: typing.Any) -> typing.Any:
        return handler_class_getitem(cls, params)

    _next_handler: "CORRequestHandler[ReqT, ResT] | None" = None

    def set_next(
        self,
        handler: "CORRequestHandler[ReqT, ResT]",
    ) -> "CORRequestHandler[ReqT, ResT]":
        """
        [AI Instruction]
        Link the following handler. Application code does not call this.
        ``RequestMap.bind`` with a list, or ``build_chain``, links the steps.
        The first assignment sticks; later calls return the handler already set.
        """
        if self._next_handler is None:
            self._next_handler = handler

        return self._next_handler

    async def next(self, request: ReqT) -> ResT | None:
        """
        [AI Instruction]
        Call from ``handle`` when this step does not apply, and return that
        result. Runs the following handler, or returns ``None`` when this step
        is last.
        """
        if self._next_handler:
            return await self._next_handler.handle(request)

        return typing.cast(ResT, None)

    @property
    def events(self) -> typing.Sequence[IEvent]:
        """
        Events produced by this handler after :meth:`handle` was called.

        Override in subclasses to return follow-up events. By default returns
        an empty sequence.

        [AI Instruction]
        Return events produced by the step that actually handled the request.
        The mediator emits them after the chain returns. A step that only
        forwards the request should not add events.
        """
        return ()

    @typing.overload
    async def handle(self, request: ReqT) -> ResT | None:
        """
        [AI Instruction]
        Either handle the request or pass it on. If this step applies, do the
        work and return the request's result (or ``None``). If it does not,
        ``return await self.next(request)``. Do not both handle and continue.
        Declare the class as ``CORRequestHandler[ThatRequest]``. The result
        type is the request's parameter; do not restate it on ``handle``.
        """

    @typing.overload
    async def handle(self, request: ReqT) -> typing.Any:
        """Fallback when the response type parameter is left open."""

    @abc.abstractmethod
    async def handle(self, request: ReqT):
        # Same reason as RequestHandler.handle: PyCharm would copy an open
        # ``ResT`` onto an unannotated override if this annotation were present.
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

    [AI Instruction]
    Link handlers in list order and return the head. Prefer
    ``RequestMap.bind(RequestType, [A, B, C])``, which builds the chain.
    Call this only when the application holds the chain itself.
    """

    def link(handler1, handler2):
        handler1.set_next(handler2)
        return handler2

    functools.reduce(link, handlers)
    return handlers[0]
