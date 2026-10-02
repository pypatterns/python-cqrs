"""
Example: Dependency Injection with CQRS Request Handlers

This example demonstrates how to use dependency injection (DI) with CQRS request handlers.
The system shows how to integrate DI containers with CQRS mediators, allowing handlers
to receive dependencies through constructor injection.

Use case: Decoupling business logic from infrastructure dependencies. Handlers can
declare their dependencies in constructors, and the DI container automatically resolves
and injects them when handlers are instantiated.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/di/di_basic.py

The example will:
- Set up a DI container with dependency bindings
- Create a command handler that requires a dependency
- Execute the command and verify the dependency was injected correctly

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Dependency Injection Setup:
   - Define abstract dependencies (AbstractDependency) and concrete implementations
   - Configure DI container with type bindings
   - Use scope="request" to create new instances per request

2. Constructor Injection:
   - Command handlers receive dependencies through constructor parameters
   - Dependencies are automatically resolved by the DI container
   - No need for manual instantiation or service locator pattern

3. DI Container Integration:
   - Pass DI container to bootstrap.bootstrap()
   - Mediator uses the container to resolve handler dependencies
   - Handlers are created with all required dependencies injected

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - di (dependency injection library)

================================================================================
"""

import abc
import asyncio
import logging
import typing

import di
from di import dependent

import cqrs
from cqrs.bootstrap import requests as bootstrap

STACK = []

logging.basicConfig(level=logging.DEBUG)


class AbstractDependency(abc.ABC):
    @abc.abstractmethod
    async def do_something(self):
        raise NotImplementedError


class ConcreteDependency(AbstractDependency):
    async def do_something(self):
        print("Do something")
        STACK.append(1)


class Command(cqrs.Request):
    pass


class CommandHandler(cqrs.RequestHandler[Command, None]):
    def __init__(self, dependency: AbstractDependency) -> None:
        self.dependency = dependency

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return []

    async def handle(self, request: Command) -> None:
        await self.dependency.do_something()


def setup_di() -> di.Container:
    """
    Initialize DI container
    """
    container = di.Container()
    bind = di.bind_by_type(
        dependent.Dependent(ConcreteDependency, scope="request"),
        AbstractDependency,
    )
    container.bind(bind)
    return container


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    """Maps queries to handlers."""
    mapper.bind(Command, CommandHandler)


async def main():
    mediator = bootstrap.bootstrap(
        di_container=setup_di(),
        commands_mapper=commands_mapper,
    )

    result = await mediator.send(Command())

    assert result is None
    assert len(STACK) == 1


if __name__ == "__main__":
    asyncio.run(main())
