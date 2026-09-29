"""
Simple example of using the [dependency-injector](https://github.com/ets-labs/python-dependency-injector) integration.
"""

import abc
import asyncio
import logging
import typing


import cqrs
from cqrs.requests import bootstrap

# The dependency-injector library
from dependency_injector import containers, providers

# The container protocol from this package
from cqrs.container.protocol import Container as CQRSContainer

# The CQRS container adapter for containers implemented using dependency-injector
from cqrs.container.dependency_injector import DependencyInjectorCQRSContainer


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


class Container(containers.DeclarativeContainer):
    dependency = providers.Singleton(ConcreteDependency)
    command_handler = providers.Factory(
        CommandHandler,
        dependency=dependency,
    )


def setup_di_container() -> CQRSContainer:
    """
    Set up the dependency injector container for the CQRS framework.
    """

    # Create the container instance
    container = Container()

    # Attach it to the CQRS container adapter
    cqrs_container = DependencyInjectorCQRSContainer()
    cqrs_container.attach_external_container(container)

    return cqrs_container


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    """Maps queries to handlers."""
    mapper.bind(Command, CommandHandler)


async def main():
    # Pass the CQRS container to the request mediator bootstrap function
    mediator = bootstrap.bootstrap(
        di_container=setup_di_container(),
        commands_mapper=commands_mapper,
    )

    result = await mediator.send(Command())

    assert result is None
    assert len(STACK) == 1


if __name__ == "__main__":
    asyncio.run(main())
