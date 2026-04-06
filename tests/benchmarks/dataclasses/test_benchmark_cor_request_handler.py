"""Benchmarks for Chain of Responsibility (dataclass DCRequest/DCResponse)."""

import asyncio
import dataclasses
import typing

import cqrs
import di
import pytest
from cqrs.requests import bootstrap
from cqrs.requests.cor_request_handler import CORRequestHandler


@dataclasses.dataclass
class TRequest(cqrs.DCRequest):
    method: str
    user_id: str


@dataclasses.dataclass
class TResult(cqrs.DCResponse):
    success: bool
    handler_name: str
    message: str = ""


class HandlerA(CORRequestHandler[TRequest, TResult]):
    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return []

    async def handle(self, request: TRequest) -> typing.Optional[TResult]:
        if request.method == "method_a":
            return TResult(
                success=True,
                handler_name="HandlerA",
                message=f"Processed method_a for {request.user_id}",
            )
        return await self.next(request)


class HandlerB(CORRequestHandler[TRequest, TResult]):
    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return []

    async def handle(self, request: TRequest) -> typing.Optional[TResult]:
        if request.method == "method_b":
            return TResult(
                success=True,
                handler_name="HandlerB",
                message=f"Processed method_b for {request.user_id}",
            )
        return await self.next(request)


class HandlerC(CORRequestHandler[TRequest, TResult]):
    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return []

    async def handle(self, request: TRequest) -> typing.Optional[TResult]:
        if request.method == "method_c":
            return TResult(
                success=True,
                handler_name="HandlerC",
                message=f"Processed method_c for {request.user_id}",
            )
        return await self.next(request)


class DefaultHandler(CORRequestHandler[TRequest, TResult]):
    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return []

    async def handle(self, request: TRequest) -> typing.Optional[TResult]:
        return TResult(
            success=False,
            handler_name="DefaultHandler",
            message=f"Unsupported method: {request.method}",
        )


def cor_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(
        TRequest,
        [HandlerA, HandlerB, HandlerC, DefaultHandler],
    )


@pytest.fixture
def cor_mediator():
    return bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=cor_mapper,
    )


@pytest.mark.benchmark
def test_benchmark_cor_first_handler(cor_mediator, benchmark):
    """Benchmark CoR when first handler in chain handles the request."""

    async def run():
        return await cor_mediator.send(TRequest(method="method_a", user_id="user_1"))

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_cor_second_handler(cor_mediator, benchmark):
    """Benchmark CoR when second handler in chain handles the request."""

    async def run():
        return await cor_mediator.send(TRequest(method="method_b", user_id="user_1"))

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_cor_third_handler(cor_mediator, benchmark):
    """Benchmark CoR when third handler in chain handles the request."""

    async def run():
        return await cor_mediator.send(TRequest(method="method_c", user_id="user_1"))

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_cor_default_handler(cor_mediator, benchmark):
    """Benchmark CoR when only default (last) handler handles the request."""

    async def run():
        return await cor_mediator.send(TRequest(method="other", user_id="user_1"))

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_cor_ten_requests_first_handler(cor_mediator, benchmark):
    """Benchmark CoR handling 10 requests (first handler)."""

    async def run():
        for i in range(10):
            await cor_mediator.send(TRequest(method="method_a", user_id=f"user_{i}"))

    benchmark(lambda: asyncio.run(run()))
