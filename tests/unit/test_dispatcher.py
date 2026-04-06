from uuid import UUID, uuid4
import typing

import pydantic

from cqrs.dispatcher import RequestDispatcher
from cqrs.events import Event
from cqrs.middlewares import MiddlewareChain
from cqrs.requests.map import RequestMap
from cqrs.requests.request import Request
from cqrs.requests.request_handler import RequestHandler
from cqrs.response import Response


class ReadMeetingDetailsQuery(Request):
    meeting_room_id: UUID = pydantic.Field()
    status: typing.Optional[str] = pydantic.Field(default=None)


class ReadMeetingDetailsQueryResult(Response):
    meeting_room_id: UUID = pydantic.Field()
    status: typing.Optional[str] = pydantic.Field(default=None)


class ReadMeetingDetailsQueryHandler(
    RequestHandler[ReadMeetingDetailsQuery, ReadMeetingDetailsQueryResult],  # type: ignore
):
    def __init__(self) -> None:
        self.called = False
        self._events: list[Event] = []

    @property
    def events(self) -> list:
        return self._events

    async def handle(
        self,
        request: ReadMeetingDetailsQuery,
    ) -> ReadMeetingDetailsQueryResult:
        self.called = True
        return ReadMeetingDetailsQueryResult(meeting_room_id=request.meeting_room_id)


class TestQueryContainer:
    _handler = ReadMeetingDetailsQueryHandler()

    async def resolve(self, type_):
        return self._handler


async def test_default_dispatcher_logic() -> None:
    middleware = FirstMiddleware()
    request_map = RequestMap()
    request_map.bind(ReadMeetingDetailsQuery, ReadMeetingDetailsQueryHandler)
    middleware_chain = MiddlewareChain()
    middleware_chain.add(middleware)
    dispatcher = RequestDispatcher(
        request_map=request_map,
        container=TestQueryContainer(),  # type: ignore
        middleware_chain=middleware_chain,
    )

    request = ReadMeetingDetailsQuery(meeting_room_id=uuid4())

    result = await dispatcher.dispatch(request)

    assert request.status == "REQ"  # type: ignore
    assert result.response.status == "RES"  # type: ignore


async def test_default_dispatcher_chain_logic() -> None:
    request_map = RequestMap()
    request_map.bind(ReadMeetingDetailsQuery, ReadMeetingDetailsQueryHandler)
    middleware_chain = MiddlewareChain()
    middleware_chain.set([FirstMiddleware(), SecondMiddleware(), ThirdMiddleware()])
    dispatcher = RequestDispatcher(
        request_map=request_map,
        container=TestQueryContainer(),  # type: ignore
        middleware_chain=middleware_chain,
    )

    request = ReadMeetingDetailsQuery(meeting_room_id=uuid4())

    result = await dispatcher.dispatch(request)

    assert request.status == "REQ"  # type: ignore
    assert result.response.status == "RES"  # type: ignore

    assert request.status == "REQ"  # type: ignore
    assert result.response.status == "RES"  # type: ignore

    assert request.status == "REQ"  # type: ignore
    assert result.response.status == "RES"  # type: ignore


class FirstMiddleware:
    async def __call__(self, request: Request, handle):
        request.status = "REQ"  # type: ignore
        response = await handle(request)
        response.status = "RES"  # type: ignore
        return response


class SecondMiddleware:
    async def __call__(self, request: Request, handle):
        request.status = "REQ"  # type: ignore
        response = await handle(request)
        response.status = "RES"  # type: ignore
        return response


class ThirdMiddleware:
    async def __call__(self, request: Request, handle):
        request.status = "REQ"  # type: ignore
        response = await handle(request)
        response.status = "RES"  # type: ignore
        return response
