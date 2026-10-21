# Agents using python-cqrs

Copy this file into an application that depends on `python-cqrs`. It describes the library's API, not how to change the library.

## Requests

- Commands and queries are both `cqrs.Request`. There is no `Query` class and no `CommandBus`.
- There is no `register_handler` decorator. Register with `mapper.bind(RequestType, Handler)`.
- One handler per request type. A second `bind` for the same request raises `KeyError`.
- An event may have many handlers. `EventMap.bind` appends.

## Typing

- A new request that returns data is `class Query(cqrs.Request[QueryResult])` with `class Handler(cqrs.RequestHandler[Query])`.
- Do not annotate `handle` with `-> QueryResult`. The return type is the request's parameter.
- A new command with an empty body is `class Command(cqrs.Request[None])`. `handle` returns `None` and does not need a return annotation.
- Already written `Request` and `RequestHandler[Request, Result]` stay as they are. `send` on an unparameterized `Request` is `Any`.
- The same parameter types `StreamingRequestMediator.stream` and `PydanticRequest[Result]` (the `pydantic` extra).

## Behavior the library does not check

- A read handler should not write state and should not append to `events`. A command handler may. The library does not enforce that.
- Publish to a broker through the outbox. Do not write the business row and then publish to Kafka yourself.
- Use a saga for a step that has compensation across services, not for a single `UPDATE`.

## Extras

Install extras explicitly: `pydantic`, `sqlalchemy`, `sqlmodel`, `kafka`, and the others listed in the library `pyproject.toml`.
