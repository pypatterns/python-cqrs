import typing

import di

import cqrs
from cqrs.requests import bootstrap
from cqrs.requests.cor_request_handler import CORRequestHandler


class TRequest(cqrs.Request):
    method: str
    user_id: str


class TResult(cqrs.Response):
    success: bool
    handler_name: str
    message: str = ""


class TestHandlerA(CORRequestHandler[TRequest, TResult]):
    """Test handler that processes method_a and tracks calls."""

    call_count: int = 0

    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return []

    async def handle(self, request: TRequest) -> typing.Optional[TResult]:
        TestHandlerA.call_count += 1

        if request.method == "method_a":
            return TResult(
                success=True,
                handler_name="TestHandlerA",
                message=f"Processed method_a for user {request.user_id}",
            )

        return await self.next(request)


class TestHandlerB(CORRequestHandler[TRequest, TResult]):
    """Test handler that processes method_b and tracks calls."""

    call_count: int = 0

    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return []

    async def handle(self, request: TRequest) -> typing.Optional[TResult]:
        TestHandlerB.call_count += 1

        if request.method == "method_b":
            return TResult(
                success=True,
                handler_name="TestHandlerB",
                message=f"Processed method_b for user {request.user_id}",
            )

        return await self.next(request)


class TestHandlerC(CORRequestHandler[TRequest, TResult]):
    """Test handler that processes method_c and tracks calls."""

    call_count: int = 0

    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return []

    async def handle(self, request: TRequest) -> typing.Optional[TResult]:
        TestHandlerC.call_count += 1

        if request.method == "method_c":
            return TResult(
                success=True,
                handler_name="TestHandlerC",
                message=f"Processed method_c for user {request.user_id}",
            )

        return await self.next(request)


class DefaultTestHandler(CORRequestHandler[TRequest, TResult]):
    """Default handler that always handles the request (end of chain)."""

    call_count: int = 0

    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return []

    async def handle(self, request: TRequest) -> typing.Optional[TResult]:
        DefaultTestHandler.call_count += 1
        return TResult(
            success=False,
            handler_name="DefaultTestHandler",
            message=f"Unsupported method: {request.method}",
        )


def cor_mapper(mapper: cqrs.RequestMap) -> None:
    """Register the chain of test handlers."""
    mapper.bind(
        TRequest,
        [
            TestHandlerA,
            TestHandlerB,
            TestHandlerC,
            DefaultTestHandler,
        ],
    )


def reset_call_counts():
    """Reset all handler call counts for clean tests."""
    TestHandlerA.call_count = 0
    TestHandlerB.call_count = 0
    TestHandlerC.call_count = 0
    DefaultTestHandler.call_count = 0


async def test_cor_chain_stops_after_first_handler():
    """Test that chain stops after first handler successfully processes request."""
    reset_call_counts()
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=cor_mapper,
    )

    # Send request that should be handled by TestHandlerA (first in chain)
    result: TResult = await mediator.send(TRequest(method="method_a", user_id="user1"))

    # Verify result
    assert result.success is True
    assert result.handler_name == "TestHandlerA"
    assert "method_a" in result.message
    # Verify call counts - only first handler should be called
    assert TestHandlerA.call_count == 1
    assert TestHandlerB.call_count == 0  # Should NOT be called
    assert TestHandlerC.call_count == 0  # Should NOT be called
    assert DefaultTestHandler.call_count == 0  # Should NOT be called


async def test_cor_chain_continues_to_second_handler():
    """Test that chain continues to second handler when first can't handle request."""
    reset_call_counts()
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=cor_mapper,
    )

    # Send request that should be handled by TestHandlerB (second in chain)
    result: TResult = await mediator.send(TRequest(method="method_b", user_id="user2"))

    # Verify result
    assert result.success is True
    assert result.handler_name == "TestHandlerB"
    assert "method_b" in result.message
    # Verify call counts - first and second handlers should be called
    assert TestHandlerA.call_count == 1  # Called but didn't handle
    assert TestHandlerB.call_count == 1  # Called and handled
    assert TestHandlerC.call_count == 0  # Should NOT be called
    assert DefaultTestHandler.call_count == 0  # Should NOT be called


async def test_cor_chain_continues_to_third_handler():
    """Test that chain continues to third handler when earlier ones can't handle request."""
    reset_call_counts()
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=cor_mapper,
    )

    # Send request that should be handled by TestHandlerC (third in chain)
    result: TResult = await mediator.send(TRequest(method="method_c", user_id="user3"))

    # Verify result
    assert result.success is True
    assert result.handler_name == "TestHandlerC"
    assert "method_c" in result.message
    # Verify call counts - first, second, and third handlers should be called
    assert TestHandlerA.call_count == 1  # Called but didn't handle
    assert TestHandlerB.call_count == 1  # Called but didn't handle
    assert TestHandlerC.call_count == 1  # Called and handled
    assert DefaultTestHandler.call_count == 0  # Should NOT be called


async def test_cor_chain_reaches_default_handler():
    """Test that chain reaches default handler when no earlier handler can process request."""
    reset_call_counts()
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=cor_mapper,
    )

    # Send request with unsupported method
    result: TResult = await mediator.send(
        TRequest(method="unsupported_method", user_id="user4"),
    )

    # Verify result
    assert result.success is False
    assert result.handler_name == "DefaultTestHandler"
    assert "Unsupported method" in result.message
    # Verify call counts - all handlers should be called
    assert TestHandlerA.call_count == 1  # Called but didn't handle
    assert TestHandlerB.call_count == 1  # Called but didn't handle
    assert TestHandlerC.call_count == 1  # Called but didn't handle
    assert DefaultTestHandler.call_count == 1  # Called and handled (as fallback)


async def test_cor_multiple_requests_independent():
    """Test that multiple requests are processed independently."""
    reset_call_counts()
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=cor_mapper,
    )

    # Send multiple requests in sequence
    result1 = await mediator.send(TRequest(method="method_a", user_id="user1"))
    result2 = await mediator.send(TRequest(method="method_b", user_id="user2"))
    result3 = await mediator.send(TRequest(method="method_a", user_id="user3"))

    # Verify results
    assert result1.success is True and result1.handler_name == "TestHandlerA"
    assert result2.success is True and result2.handler_name == "TestHandlerB"
    assert result3.success is True and result3.handler_name == "TestHandlerA"
    # Verify call counts - handlers called appropriate number of times
    assert TestHandlerA.call_count == 3  # Called for all 3 requests
    assert TestHandlerB.call_count == 1  # Called only for method_b request
    assert TestHandlerC.call_count == 0  # Never called
    assert DefaultTestHandler.call_count == 0  # Never called
