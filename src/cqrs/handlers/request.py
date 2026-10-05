import abc
import typing

from cqrs.models.event import IEvent
from cqrs.models.request import ReqT, ResT


class RequestHandler(abc.ABC, typing.Generic[ReqT, ResT]):
    """
    The request handler interface.

    The request handler is an object, which gets a request as input and may return a response as a result.

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

    @property
    def events(self) -> typing.Sequence[IEvent]:
        """
        Events produced by this handler after :meth:`handle` was called.

        Override in subclasses to return follow-up events. By default returns
        an empty sequence.
        """
        return ()

    @abc.abstractmethod
    async def handle(self, request: ReqT) -> ResT:
        raise NotImplementedError


class StreamingRequestHandler(abc.ABC, typing.Generic[ReqT, ResT]):
    """
    The streaming request handler interface.

    The streaming request handler is an object, which gets a request as input
    and yields responses as an async generator. After each yield, events can be
    collected via the events property and emitted.

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

    @property
    def events(self) -> typing.Sequence[IEvent]:
        """
        Events produced by this handler after each yield from :meth:`handle`.

        Override in subclasses to return follow-up events. By default returns
        an empty sequence.
        """
        return ()

    @abc.abstractmethod
    def handle(self, request: ReqT) -> typing.AsyncIterator[ResT]:
        """
        Handle the request by yielding results as an async generator.

        Subclasses must implement this as an async generator (async def with
        yield) so that callers receive an AsyncIterator when calling handle().
        """
        raise NotImplementedError

    @abc.abstractmethod
    def clear_events(self) -> None:
        """
        Clear events that have been processed.

        This method should be called after events have been processed and emitted
        to prevent event accumulation across multiple yields.
        """
        raise NotImplementedError
