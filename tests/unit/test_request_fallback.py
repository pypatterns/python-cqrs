"""Tests for RequestHandlerFallback (without circuit breaker)."""

from typing import Any, TypeVar

import pytest

from cqrs import RequestHandlerFallback
from cqrs.container.protocol import Container
from cqrs.dispatcher import RequestDispatcher
from cqrs.models.event import IEvent
from cqrs.mapping.requests import RequestMap
from cqrs.models.request import Request
from cqrs.handlers.request import RequestHandler
from cqrs.models.response import Response

T = TypeVar("T")


class SimpleCommand(Request):
    value: str


class SimpleResult(Response):
    value: str


class PrimaryHandler(RequestHandler[SimpleCommand, SimpleResult]):
    def __init__(self) -> None:
        self._events: list[IEvent] = []
        self.called = False

    @property
    def events(self) -> list[IEvent]:
        return self._events.copy()

    async def handle(self, request: SimpleCommand) -> SimpleResult:
        self.called = True
        raise RuntimeError("Primary failed")


class FallbackHandler(RequestHandler[SimpleCommand, SimpleResult]):
    def __init__(self) -> None:
        self._events: list[IEvent] = []
        self.called = False

    @property
    def events(self) -> list[IEvent]:
        return self._events.copy()

    async def handle(self, request: SimpleCommand) -> SimpleResult:
        self.called = True
        return SimpleResult(value=f"fallback:{request.value}")


class _TestRequestContainer(Container[Any]):
    """Minimal container for request fallback tests."""

    def __init__(self) -> None:
        self._primary = PrimaryHandler()
        self._fallback = FallbackHandler()
        self._external_container: Any = None

    @property
    def external_container(self) -> Any:
        return self._external_container

    def attach_external_container(self, container: Any) -> None:
        self._external_container = container

    async def resolve(self, type_: type[T]) -> T:
        if type_ is PrimaryHandler:
            return self._primary  # type: ignore[return-value]
        if type_ is FallbackHandler:
            return self._fallback  # type: ignore[return-value]
        raise KeyError(type_)


@pytest.mark.asyncio
async def test_request_fallback_no_cb_primary_fails_uses_fallback() -> None:
    request_map: RequestMap = RequestMap()
    request_map.bind(
        SimpleCommand,
        RequestHandlerFallback(PrimaryHandler, FallbackHandler),
    )
    container: Container[Any] = _TestRequestContainer()
    dispatcher = RequestDispatcher(request_map=request_map, container=container)

    result = await dispatcher.dispatch(SimpleCommand(value="x"))

    assert result.response.value == "fallback:x"
    assert container._primary.called
    assert container._fallback.called


@pytest.mark.asyncio
async def test_request_fallback_failure_exceptions_only_matching_triggers_fallback() -> None:
    request_map = RequestMap()
    request_map.bind(
        SimpleCommand,
        RequestHandlerFallback(
            PrimaryHandler,
            FallbackHandler,
            failure_exceptions=(ValueError,),
        ),
    )
    container: Container[Any] = _TestRequestContainer()
    dispatcher = RequestDispatcher(request_map=request_map, container=container)

    with pytest.raises(RuntimeError, match="Primary failed"):
        await dispatcher.dispatch(SimpleCommand(value="x"))

    assert container._primary.called
    assert not container._fallback.called


@pytest.mark.asyncio
async def test_request_fallback_primary_succeeds_fallback_not_invoked() -> None:
    """When the primary handler succeeds, the fallback is not invoked."""

    class SuccessPrimaryHandler(RequestHandler[SimpleCommand, SimpleResult]):
        def __init__(self) -> None:
            self._events: list[IEvent] = []
            self.called = False

        @property
        def events(self) -> list[IEvent]:
            return self._events.copy()

        async def handle(self, request: SimpleCommand) -> SimpleResult:
            self.called = True
            return SimpleResult(value=f"primary:{request.value}")

    class UnusedFallbackHandler(RequestHandler[SimpleCommand, SimpleResult]):
        def __init__(self) -> None:
            self._events: list[IEvent] = []
            self.called = False

        @property
        def events(self) -> list[IEvent]:
            return self._events.copy()

        async def handle(self, request: SimpleCommand) -> SimpleResult:
            self.called = True
            return SimpleResult(value="unused")

    class SuccessContainer(Container[Any]):
        def __init__(self) -> None:
            self._primary = SuccessPrimaryHandler()
            self._fallback = UnusedFallbackHandler()
            self._external_container: Any = None

        @property
        def external_container(self) -> Any:
            return self._external_container

        def attach_external_container(self, container: Any) -> None:
            self._external_container = container

        async def resolve(self, type_: type[T]) -> T:
            if type_ is SuccessPrimaryHandler:
                return self._primary  # type: ignore[return-value]
            if type_ is UnusedFallbackHandler:
                return self._fallback  # type: ignore[return-value]
            raise KeyError(type_)

    request_map = RequestMap()
    request_map.bind(
        SimpleCommand,
        RequestHandlerFallback(SuccessPrimaryHandler, UnusedFallbackHandler),
    )
    container = SuccessContainer()
    dispatcher = RequestDispatcher(request_map=request_map, container=container)

    result = await dispatcher.dispatch(SimpleCommand(value="ok"))

    assert result.response.value == "primary:ok"
    assert container._primary.called
    assert not container._fallback.called


# --- Validation tests ---


def test_request_fallback_validation_same_request_and_response_types_accepts() -> None:
    """Same request and response types (including None) are accepted."""
    RequestHandlerFallback(PrimaryHandler, FallbackHandler)


def test_request_fallback_validation_different_request_type_raises() -> None:
    """Different request types raise TypeError."""
    from cqrs.models.request import Request
    from cqrs.models.response import Response

    class OtherCommand(Request):
        value: int

    class OtherResult(Response):
        value: int

    class FallbackOther(RequestHandler[OtherCommand, OtherResult]):
        async def handle(self, request: OtherCommand) -> OtherResult:
            return OtherResult(value=0)

    with pytest.raises(TypeError, match="same request type"):
        RequestHandlerFallback(PrimaryHandler, FallbackOther)


def test_request_fallback_validation_different_response_type_raises() -> None:
    """Different response types raise TypeError."""
    from cqrs.models.response import Response

    class OtherResult(Response):
        value: int

    class FallbackOtherResult(RequestHandler[SimpleCommand, OtherResult]):
        async def handle(self, request: SimpleCommand) -> OtherResult:
            return OtherResult(value=0)

    with pytest.raises(TypeError, match="same response type"):
        RequestHandlerFallback(PrimaryHandler, FallbackOtherResult)


def test_request_fallback_validation_same_types_with_none_response_accepts() -> None:
    """Both request and response (None) matching is accepted."""
    from cqrs.models.request import Request

    class NoResultCommand(Request):
        x: str

    class PrimaryNoRes(RequestHandler[NoResultCommand, None]):
        async def handle(self, request: NoResultCommand) -> None:
            return None

    class FallbackNoRes(RequestHandler[NoResultCommand, None]):
        async def handle(self, request: NoResultCommand) -> None:
            return None

    RequestHandlerFallback(PrimaryNoRes, FallbackNoRes)


def test_request_fallback_validation_not_classes_raises() -> None:
    """Passing non-classes raises TypeError."""
    with pytest.raises(TypeError, match="must be handler classes"):
        RequestHandlerFallback(PrimaryHandler, FallbackHandler())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be handler classes"):
        RequestHandlerFallback(PrimaryHandler(), FallbackHandler)  # type: ignore[arg-type]


def test_request_fallback_validation_mixed_handler_base_raises() -> None:
    """Mixing RequestHandler and StreamingRequestHandler raises TypeError."""
    from cqrs.handlers.request import StreamingRequestHandler

    class StreamingPrimary(StreamingRequestHandler[SimpleCommand, SimpleResult]):
        async def handle(self, request: SimpleCommand):
            yield SimpleResult(value=request.value)

        def clear_events(self) -> None:
            pass

    with pytest.raises(TypeError, match="same handler base type"):
        RequestHandlerFallback(PrimaryHandler, StreamingPrimary)
