"""Runtime projection and static result types for Request[Result] (issue #103)."""

import importlib.resources
import typing

import pytest
from typing_extensions import assert_type

import cqrs
from cqrs.handlers.cor import CORRequestHandler
from cqrs.handlers.request import RequestHandler, StreamingRequestHandler


class ReadMeetingQueryResult(cqrs.Response):
    users: list[str]


class ReadMeetingQuery(cqrs.Request[ReadMeetingQueryResult]):
    meeting_id: str


class JoinMeetingCommand(cqrs.Request[None]):
    user_id: str
    meeting_id: str


class BareQuery(cqrs.Request):
    meeting_id: str


class ChildQuery(ReadMeetingQuery):
    extra: int = 0


class ReadMeetingQueryHandler(RequestHandler[ReadMeetingQuery]):
    async def handle(self, request: ReadMeetingQuery):
        return ReadMeetingQueryResult(users=["1"])


class JoinMeetingCommandHandler(RequestHandler[JoinMeetingCommand]):
    async def handle(self, request: JoinMeetingCommand):
        return None


class BareQueryHandler(RequestHandler[BareQuery]):
    async def handle(self, request: BareQuery):
        return ReadMeetingQueryResult(users=["bare"])


class OldReadMeetingQueryHandler(RequestHandler[ReadMeetingQuery, ReadMeetingQueryResult]):
    async def handle(self, request: ReadMeetingQuery) -> ReadMeetingQueryResult:
        return ReadMeetingQueryResult(users=["old"])


class PassthroughHandler(RequestHandler[ReadMeetingQuery, None]):
    async def handle(self, request: ReadMeetingQuery) -> None:
        return None


class ChildQueryHandler(RequestHandler[ChildQuery]):
    async def handle(self, request: ChildQuery):
        return ReadMeetingQueryResult(users=["child"])


class ItemResult(cqrs.Response):
    item_id: str


class ProcessItemsCommand(cqrs.Request[ItemResult]):
    item_ids: list[str]


class ProcessItemsHandler(StreamingRequestHandler[ProcessItemsCommand]):
    def clear_events(self) -> None:
        return None

    async def handle(self, request: ProcessItemsCommand):
        for item_id in request.item_ids:
            yield ItemResult(item_id=item_id)


class OldStreamingHandler(StreamingRequestHandler[ProcessItemsCommand, ItemResult]):
    def clear_events(self) -> None:
        return None

    async def handle(self, request: ProcessItemsCommand) -> typing.AsyncIterator[ItemResult]:
        for item_id in request.item_ids:
            yield ItemResult(item_id=item_id)


class PaymentResult(cqrs.Response):
    success: bool


class ProcessPaymentCommand(cqrs.Request[PaymentResult]):
    amount: int


class PaymentHandler(CORRequestHandler[ProcessPaymentCommand]):
    async def handle(self, request: ProcessPaymentCommand):
        return PaymentResult(success=True)


class OldPaymentHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    async def handle(self, request: ProcessPaymentCommand) -> PaymentResult | None:
        return PaymentResult(success=request.amount > 0)


def _handler_args(handler: type) -> tuple[typing.Any, ...]:
    return typing.get_args(handler.__orig_bases__[0])


def test_parameterized_and_bare_requests_round_trip() -> None:
    query = ReadMeetingQuery(meeting_id="1")
    assert query.to_dict() == {"meeting_id": "1"}
    assert ReadMeetingQuery.from_dict(meeting_id="2").to_dict() == {"meeting_id": "2"}

    command = JoinMeetingCommand(user_id="u", meeting_id="m")
    assert command.to_dict() == {"user_id": "u", "meeting_id": "m"}
    restored = JoinMeetingCommand.from_dict(user_id="a", meeting_id="b")
    assert restored.to_dict() == {"user_id": "a", "meeting_id": "b"}

    bare = BareQuery(meeting_id="9")
    assert bare.to_dict() == {"meeting_id": "9"}
    assert BareQuery.from_dict(**bare.to_dict()).meeting_id == "9"
    assert isinstance(query, cqrs.Request)
    assert isinstance(bare, cqrs.DCRequest)


def test_one_argument_handler_projects_response_type() -> None:
    assert _handler_args(ReadMeetingQueryHandler) == (ReadMeetingQuery, ReadMeetingQueryResult)
    assert _handler_args(JoinMeetingCommandHandler) == (JoinMeetingCommand, type(None))
    assert _handler_args(BareQueryHandler) == (BareQuery, typing.Any)
    assert _handler_args(ChildQueryHandler) == (ChildQuery, ReadMeetingQueryResult)
    assert _handler_args(ProcessItemsHandler) == (ProcessItemsCommand, ItemResult)
    assert _handler_args(PaymentHandler) == (ProcessPaymentCommand, PaymentResult)


def test_two_argument_handler_is_not_rewritten() -> None:
    assert _handler_args(OldReadMeetingQueryHandler) == (ReadMeetingQuery, ReadMeetingQueryResult)
    assert _handler_args(PassthroughHandler) == (ReadMeetingQuery, type(None))
    assert _handler_args(OldStreamingHandler) == (ProcessItemsCommand, ItemResult)
    assert _handler_args(OldPaymentHandler) == (ProcessPaymentCommand, PaymentResult)
    assert issubclass(ReadMeetingQueryHandler, RequestHandler)
    assert issubclass(OldReadMeetingQueryHandler, RequestHandler)
    assert issubclass(ProcessItemsHandler, StreamingRequestHandler)
    assert issubclass(PaymentHandler, CORRequestHandler)


def test_py_typed_is_packaged() -> None:
    resource = importlib.resources.files("cqrs").joinpath("py.typed")
    assert resource.is_file()


async def test_unannotated_handle_call_types() -> None:
    query_result = await ReadMeetingQueryHandler().handle(ReadMeetingQuery(meeting_id="1"))
    assert_type(query_result, ReadMeetingQueryResult)
    assert query_result.users == ["1"]

    command_result = await JoinMeetingCommandHandler().handle(JoinMeetingCommand(user_id="1", meeting_id="1"))
    assert_type(command_result, None)
    assert command_result is None

    bare_result = await BareQueryHandler().handle(BareQuery(meeting_id="1"))
    assert_type(bare_result, ReadMeetingQueryResult)
    assert bare_result.users == ["bare"]

    old_result = await OldReadMeetingQueryHandler().handle(ReadMeetingQuery(meeting_id="1"))
    assert_type(old_result, ReadMeetingQueryResult)

    items = [item async for item in ProcessItemsHandler().handle(ProcessItemsCommand(item_ids=["a"]))]
    assert_type(items[0], ItemResult)
    assert items[0].item_id == "a"

    payment = await PaymentHandler().handle(ProcessPaymentCommand(amount=1))
    assert_type(payment, PaymentResult)
    assert payment is not None
    assert payment.success


async def _send_and_stream_types(mediator: cqrs.RequestMediator, streaming: cqrs.StreamingRequestMediator) -> None:
    users = await mediator.send(ReadMeetingQuery(meeting_id="1"))
    assert_type(users, ReadMeetingQueryResult)
    assert_type(users.users, list[str])

    nothing = await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="1"))
    assert_type(nothing, None)

    bare = await mediator.send(BareQuery(meeting_id="1"))
    assert_type(bare, typing.Any)
    bare.users

    async for item in streaming.stream(ProcessItemsCommand(item_ids=["a"])):
        assert_type(item, ItemResult)


@pytest.mark.pydantic
def test_pydantic_request_generic_round_trip() -> None:
    pytest.importorskip("pydantic")
    from cqrs.models.pydantic import PydanticRequest

    class PydanticResult(cqrs.Response):
        users: list[str]

    class PydanticQuery(PydanticRequest[PydanticResult]):
        meeting_id: str

    class PydanticCommand(PydanticRequest):
        user_id: str

    query = PydanticQuery(meeting_id="1")
    assert query.to_dict() == {"meeting_id": "1"}
    assert PydanticQuery.from_dict(meeting_id="2").meeting_id == "2"
    assert isinstance(query, PydanticRequest)

    command = PydanticCommand(user_id="u")
    assert command.to_dict() == {"user_id": "u"}
    assert PydanticCommand.from_dict(user_id="a").user_id == "a"

    class PydanticQueryHandler(RequestHandler[PydanticQuery]):
        async def handle(self, request: PydanticQuery):
            return PydanticResult(users=[request.meeting_id])

    assert _handler_args(PydanticQueryHandler) == (PydanticQuery, PydanticResult)

    class BarePydanticHandler(RequestHandler[PydanticCommand]):
        async def handle(self, request: PydanticCommand):
            return None

    assert _handler_args(BarePydanticHandler) == (PydanticCommand, typing.Any)
