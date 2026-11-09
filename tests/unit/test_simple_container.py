"""Default container: bootstrap works without a DI library."""

from __future__ import annotations

import dataclasses
import importlib.util
import pathlib
import re
import typing

import pytest

import cqrs
from cqrs.bootstrap import events as events_bootstrap
from cqrs.bootstrap import requests as requests_bootstrap
from cqrs.bootstrap import saga as saga_bootstrap
from cqrs.container.simple import SimpleContainer
from cqrs.saga.models import SagaContext
from cqrs.testing import test_harness


def _runtime_source(module_name: str) -> str:
    spec = importlib.util.find_spec(module_name)
    assert spec is not None and spec.origin is not None
    text = open(spec.origin, encoding="utf-8").read()
    return re.sub(
        r"if typing\.TYPE_CHECKING:.*?(?=\n(?:[^\s#]|$))",
        "",
        text,
        flags=re.S,
    )


class Created(cqrs.Response):
    order_id: str


class Create(cqrs.Request[Created]):
    sku: str


class CreateHandler(cqrs.RequestHandler[Create]):
    async def handle(self, request: Create):
        return Created(order_id=request.sku)


class PrefixedHandler(cqrs.RequestHandler[Create]):
    def __init__(self, prefix: str) -> None:
        self._prefix = prefix

    async def handle(self, request: Create):
        return Created(order_id=f"{self._prefix}-{request.sku}")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(Create, CreateHandler)


class OrderPlaced(cqrs.DomainEvent, frozen=True):
    order_id: str


_SEEN: list[str] = []


class RecordPlaced(cqrs.EventHandler[OrderPlaced]):
    async def handle(self, event: OrderPlaced) -> None:
        _SEEN.append(event.order_id)


def events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderPlaced, RecordPlaced)


@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str = ""


class Reserve(cqrs.SagaStepHandler[OrderContext, None]):
    async def act(self, context: OrderContext):
        context.order_id = "reserved"
        return self._generate_step_result(None)

    async def compensate(self, context: OrderContext) -> None:
        return None


class OrderSaga(cqrs.Saga[OrderContext]):
    steps: typing.ClassVar[list] = [Reserve]


def sagas_mapper(mapper: cqrs.SagaMap) -> None:
    mapper.bind(OrderContext, OrderSaga)


def test_core_modules_do_not_import_di_libraries_at_import_time() -> None:
    for name in (
        "cqrs",
        "cqrs.bootstrap.requests",
        "cqrs.bootstrap.events",
        "cqrs.bootstrap.saga",
        "cqrs.container.simple",
    ):
        for line in _runtime_source(name).splitlines():
            assert not line.startswith("import di"), name
            assert not line.startswith("from di "), name
            assert not line.startswith("from dependency_injector"), name
            assert not line.startswith("import dependency_injector"), name


def test_di_adapters_document_extras() -> None:
    di_source = _runtime_source("cqrs.container.di")
    injector_source = _runtime_source("cqrs.container.dependency_injector")
    assert "python-cqrs[di]" in di_source
    assert "python-cqrs[dependency-injector]" in injector_source


def test_di_container_stays_available_when_extra_is_installed() -> None:
    assert cqrs.DIContainer.__name__ == "DIContainer"


async def test_bootstrap_without_container_sends_request() -> None:
    mediator = requests_bootstrap.bootstrap(commands_mapper=commands_mapper)
    result = await mediator.send(Create(sku="book"))
    assert result == Created(order_id="book")


async def test_simple_container_register_supplies_constructor_argument() -> None:
    container = SimpleContainer()
    container.register(PrefixedHandler, PrefixedHandler("ord"))

    def bind(mapper: cqrs.RequestMap) -> None:
        mapper.bind(Create, PrefixedHandler)

    mediator = requests_bootstrap.bootstrap(di_container=container, commands_mapper=bind)
    result = await mediator.send(Create(sku="book"))
    assert result.order_id == "ord-book"


async def test_simple_container_rejects_handler_with_required_arguments() -> None:
    with pytest.raises(TypeError, match=r"python-cqrs\[di\]"):
        await SimpleContainer().resolve(PrefixedHandler)


async def test_event_bootstrap_without_container() -> None:
    _SEEN.clear()
    mediator = events_bootstrap.bootstrap(events_mapper=events_mapper)
    await mediator.send(OrderPlaced(order_id="ord-1"))
    assert _SEEN == ["ord-1"]


async def test_saga_bootstrap_without_container() -> None:
    mediator = saga_bootstrap.bootstrap(sagas_mapper=sagas_mapper)
    context = OrderContext()
    steps = [step async for step in mediator.execute(context)]
    assert steps
    assert context.order_id == "reserved"


async def test_harness_without_container() -> None:
    async with test_harness(commands_mapper=commands_mapper) as harness:
        result = await harness.mediator.send(Create(sku="book"))
        assert result.order_id == "book"
        harness.requests.assert_contains(Create)
        harness.invocations.assert_entered(CreateHandler)


async def test_resolve_returns_the_registered_instance() -> None:
    container = SimpleContainer()
    handler = PrefixedHandler("ord")
    container.register(PrefixedHandler, handler)
    assert await container.resolve(PrefixedHandler) is handler
    assert await container.resolve(PrefixedHandler) is handler


class Joined(cqrs.DomainEvent, frozen=True):
    user_id: str


class Join(cqrs.Request[None]):
    user_id: str


class JoinHandler(cqrs.RequestHandler[Join]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: Join):
        self._events = []
        self._events.append(Joined(user_id=request.user_id))


class RecordJoined(cqrs.EventHandler[Joined]):
    async def handle(self, event: Joined) -> None:
        _JOINED.append(event.user_id)


_JOINED: list[str] = []


async def test_pinned_handler_publishes_only_this_send() -> None:
    _JOINED.clear()
    container = SimpleContainer()
    container.register(JoinHandler, JoinHandler())

    def bind_commands(mapper: cqrs.RequestMap) -> None:
        mapper.bind(Join, JoinHandler)

    def bind_events(mapper: cqrs.EventMap) -> None:
        mapper.bind(Joined, RecordJoined)

    mediator = requests_bootstrap.bootstrap(
        di_container=container,
        commands_mapper=bind_commands,
        domain_events_mapper=bind_events,
    )
    await mediator.send(Join(user_id="ann"))
    await mediator.send(Join(user_id="ben"))
    assert _JOINED == ["ann", "ben"]


def _load_setup_asset() -> typing.Any:
    path = pathlib.Path(__file__).resolve().parents[2] / "skills" / "cqrs-setup" / "assets" / "join_meeting.py"
    spec = importlib.util.spec_from_file_location("cqrs_setup_join_meeting", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def test_setup_asset_sends_command_and_query() -> None:
    asset = _load_setup_asset()
    rooms: dict[str, list[str]] = {}
    mediator = asset.build_mediator(rooms)
    await mediator.send(asset.JoinMeetingCommand(user_id="ann", meeting_id="m-1"))
    result = await mediator.send(asset.ReadMeetingQuery(meeting_id="m-1"))
    assert result.users == ["ann"]
    assert rooms == {"m-1": ["ann"]}
