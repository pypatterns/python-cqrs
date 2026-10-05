"""Benchmarks for request handling performance (dataclass DCRequest/DCResponse)."""

import asyncio
import dataclasses
import typing
from collections import defaultdict

import cqrs
import di
import pytest
from cqrs.bootstrap import requests as bootstrap

STORAGE = defaultdict[str, typing.List[str]](lambda: [])


@dataclasses.dataclass
class JoinMeetingCommand(cqrs.DCRequest):
    user_id: str
    meeting_id: str


@dataclasses.dataclass
class ReadMeetingQuery(cqrs.DCRequest):
    meeting_id: str


@dataclasses.dataclass
class ReadMeetingQueryResult(cqrs.DCResponse):
    users: list[str]


class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    @property
    def events(self):
        return []

    async def handle(self, request: JoinMeetingCommand) -> None:
        STORAGE[request.meeting_id].append(request.user_id)


class ReadMeetingQueryHandler(
    cqrs.RequestHandler[ReadMeetingQuery, ReadMeetingQueryResult],
):
    @property
    def events(self):
        return []

    async def handle(self, request: ReadMeetingQuery) -> ReadMeetingQueryResult:
        return ReadMeetingQueryResult(users=STORAGE[request.meeting_id])


def command_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(JoinMeetingCommand, JoinMeetingCommandHandler)


def query_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ReadMeetingQuery, ReadMeetingQueryHandler)


@pytest.fixture
def mediator():
    return bootstrap.bootstrap(
        di_container=di.Container(),
        queries_mapper=query_mapper,
        commands_mapper=command_mapper,
    )


@pytest.mark.benchmark
def test_benchmark_command_handling(benchmark, mediator):
    """Benchmark command handling performance."""
    STORAGE.clear()
    command = JoinMeetingCommand(user_id="user_1", meeting_id="meeting_1")

    async def run():
        await mediator.send(command)

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_query_handling(benchmark, mediator):
    """Benchmark query handling performance."""
    STORAGE.clear()
    STORAGE["meeting_1"] = ["user_1", "user_2", "user_3"]
    query = ReadMeetingQuery(meeting_id="meeting_1")

    async def run():
        return await mediator.send(query)

    benchmark(lambda: asyncio.run(run()))


@pytest.mark.benchmark
def test_benchmark_multiple_commands(benchmark, mediator):
    """Benchmark handling multiple commands in sequence."""
    STORAGE.clear()
    commands = [JoinMeetingCommand(user_id=f"user_{i}", meeting_id="meeting_2") for i in range(10)]

    async def run():
        for cmd in commands:
            await mediator.send(cmd)

    benchmark(lambda: asyncio.run(run()))
