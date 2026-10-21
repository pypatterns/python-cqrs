import abc
import typing

from cqrs.generic_utils import handler_class_getitem
from cqrs.models.event import IEvent
from cqrs.models.request import ReqT, ResT


class RequestHandler(abc.ABC, typing.Generic[ReqT, ResT]):
    """
    The request handler interface.

    The request handler is an object, which gets a request as input and may return a response as a result.

    [AI Instruction]
    Subclass this for one command or one query. Both are ``cqrs.Request``;
    there is no separate command or query base and no command bus. A command
    changes state and may record follow-up events. A query only reads and
    should leave ``events`` empty. The library does not enforce that split.
    Register exactly one handler per request type with ``RequestMap.bind``.

    Command handler example::

      class JoinMeetingCommandHandler(RequestHandler[JoinMeetingCommand, None])
          def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
              self._meetings_api = meetings_api
              self.events: list[IEvent] = []

          async def handle(self, request: JoinMeetingCommand) -> None:
              await self._meetings_api.join_user(request.user_id, request.meeting_id)

    Query handler example::

      class ReadMeetingQueryHandler(RequestHandler[ReadMeetingQuery, ReadMeetingQueryResult])
          def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
              self._meetings_api = meetings_api
              self.events: list[IEvent] = []

          async def handle(self, request: ReadMeetingQuery) -> ReadMeetingQueryResult:
              link = await self._meetings_api.get_link(request.meeting_id)
              return ReadMeetingQueryResult(link=link, meeting_id=request.meeting_id)

    """

    def __class_getitem__(cls, params: typing.Any) -> typing.Any:
        return handler_class_getitem(cls, params)

    @property
    def events(self) -> typing.Sequence[IEvent]:
        """
        Events produced by this handler after :meth:`handle` was called.

        Override in subclasses to return follow-up events. By default returns
        an empty sequence.

        [AI Instruction]
        Return events this handler produced inside ``handle``. The mediator
        emits them after ``handle`` returns: ``DomainEvent`` stays in this
        process, ``NotificationEvent`` is published. A query returns an empty
        sequence. This property does not guarantee broker delivery. When a
        notification must survive a crash, write it through the outbox in the
        same transaction as the business change, and leave ``events`` empty.
        """
        return ()

    @typing.overload
    async def handle(self, request: ReqT) -> ResT:
        """
        [AI Instruction]
        Do the work for this one request and return its result. Declare the
        class as ``RequestHandler[ThatRequest]``. The return value is the
        request's parameter (``Request[Result]`` or ``Request[None]``); do not
        restate it as ``-> Result`` on ``handle``. Code already written as
        ``RequestHandler[Request, Result]`` stays valid. Take dependencies in
        ``__init__``; the container resolves the handler. Do not call a
        mediator from inside ``handle``.
        """

    @typing.overload
    async def handle(self, request: ReqT) -> typing.Any:
        """Fallback when the response type parameter is left open."""

    @abc.abstractmethod
    async def handle(self, request: ReqT):
        # No return annotation on purpose. PyCharm copies an explicit ``-> ResT``
        # onto an unannotated override and does not apply a TypeVar default, so
        # ``RequestHandler[ThatRequest]`` would keep displaying ``ResT``. Omitting
        # it here lets PyCharm infer the override from its body. The overload is
        # what checkers use for ``RequestHandler[Request, Result]``.
        raise NotImplementedError


class StreamingRequestHandler(abc.ABC, typing.Generic[ReqT, ResT]):
    """
    The streaming request handler interface.

    The streaming request handler is an object, which gets a request as input
    and yields responses as an async generator. After each yield, events can be
    collected via the events property and emitted.

    [AI Instruction]
    Use this when one request produces many results over time (a batch, progress,
    or a server-sent stream). The caller iterates
    ``StreamingRequestMediator.stream``. A single result is ``RequestHandler``
    and ``RequestMediator.send``. Register with ``RequestMap.bind`` the same
    way as any other request: one handler per request type.

    Streaming handler example::

      class ProcessItemsCommandHandler(StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult])
          def __init__(self, items_api: ItemsAPIProtocol) -> None:
              self._items_api = items_api
              self._events: list[Event] = []

          @property
          def events(self) -> list[Event]:
              return self._events.copy()

          def clear_events(self) -> None:
              self._events.clear()

          async def handle(self, request: ProcessItemsCommand) -> typing.AsyncIterator[ProcessItemResult]:
              for item_id in request.item_ids:
                  result = await self._items_api.process_item(item_id)
                  self._events.append(ItemProcessedEvent(item_id=item_id))
                  yield ProcessItemResult(item_id=item_id, status=result.status)
    """

    def __class_getitem__(cls, params: typing.Any) -> typing.Any:
        return handler_class_getitem(cls, params)

    @property
    def events(self) -> typing.Sequence[IEvent]:
        """
        Events produced by this handler after each yield from :meth:`handle`.

        Override in subclasses to return follow-up events. By default returns
        an empty sequence.

        [AI Instruction]
        Return events produced since the previous yield. The mediator emits
        them, then the client receives the yielded value. Return a copy when
        ``clear_events`` will drop the buffer.
        """
        return ()

    @abc.abstractmethod
    def handle(self, request: ReqT) -> typing.AsyncIterator[ResT]:
        """
        Handle the request by yielding results as an async generator.

        Subclasses must implement this as an async generator (async def with
        yield) so that callers receive an AsyncIterator when calling handle().

        [AI Instruction]
        Write ``async def handle`` and ``yield`` each result. Each yielded
        value is the request's parameter (``Request[Result]`` yields
        ``Result``). Declare ``StreamingRequestHandler[ThatRequest]`` and do
        not annotate the element type on ``handle``. The two-parameter form
        remains valid for existing code. Record events before the yield they
        belong to. Do not use this method for one response.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def clear_events(self) -> None:
        """
        Clear events that have been processed.

        This method should be called after events have been processed and emitted
        to prevent event accumulation across multiple yields.

        [AI Instruction]
        Drop events the mediator has already emitted so the next yield does not
        send them again. Implement this whenever ``events`` accumulates.
        """
        raise NotImplementedError
