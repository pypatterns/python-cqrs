"""
Example: FastAPI Integration with CQRS

This example demonstrates how to integrate CQRS request handlers with FastAPI.
The system shows how to build REST APIs where FastAPI routes delegate to CQRS
handlers for business logic processing.

Use case: Building RESTful APIs with CQRS architecture. FastAPI handles HTTP
requests/responses, while CQRS handles business logic through command/query handlers.
This separation allows for better testability and maintainability.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the server:
   python examples/events/fastapi_integration.py

The server will start on http://localhost:8000

Option 1: Use Swagger UI
------------------------
1. Open your browser and go to:
   http://localhost:8000/docs

2. Find the GET /api/hello endpoint and click "Try it out"

3. Optionally modify the "msg" query parameter

4. Click "Execute" to see the response

Option 2: Use curl
-------------------
   curl "http://localhost:8000/api/hello?msg=Hello%20World"

Option 3: Use Python requests
------------------------------
   import requests
   response = requests.get("http://localhost:8000/api/hello", params={"msg": "Hello"})
   print(response.json())

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. FastAPI Route Definition:
   - Define REST endpoints using FastAPI decorators
   - Use query parameters, path parameters, and request bodies
   - Return typed responses from handlers

2. Mediator Dependency Injection:
   - Use FastAPI's Depends() to inject mediator into routes
   - Create mediator factory function for dependency injection
   - Mediator is created per request (or can be singleton)

3. Query Handler Integration:
   - Map queries to handlers using queries_mapper
   - Handlers return typed Response objects
   - Responses are automatically serialized to JSON by FastAPI

4. Separation of Concerns:
   - FastAPI handles HTTP layer (routing, serialization, validation)
   - CQRS handles business logic (command/query processing)
   - Clear separation enables independent testing and evolution

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - fastapi
   - uvicorn
   - cqrs (this package)
   - di (dependency injection)

================================================================================
"""

import logging
import typing

import di
import fastapi
import uvicorn

import cqrs
from cqrs.requests import bootstrap

logging.basicConfig(level=logging.DEBUG)

api_router = fastapi.APIRouter(prefix="/api")


class Hello(cqrs.Request):
    msg: str


class World(cqrs.Response):
    msg: str


class HelloWorldQueryHandler(cqrs.RequestHandler[Hello, World]):
    @property
    def events(self) -> typing.List[cqrs.Event]:
        return []

    async def handle(self, request: Hello) -> World:
        return World(msg="Hello CQRS!")


def queries_mapper(mapper: cqrs.RequestMap) -> None:
    """Maps queries to handlers."""
    mapper.bind(Hello, HelloWorldQueryHandler)


def mediator_factory() -> cqrs.RequestMediator:
    return bootstrap.bootstrap(
        di_container=di.Container(),
        queries_mapper=queries_mapper,
    )


@api_router.get("/hello", status_code=fastapi.status.HTTP_200_OK)
async def hello_world(
    msg: typing.Text = fastapi.Query(default="Hello World"),
    mediator: cqrs.RequestMediator = fastapi.Depends(mediator_factory),
) -> World:
    return await mediator.send(Hello(msg=msg))


if __name__ == "__main__":
    app = fastapi.FastAPI()
    app.include_router(api_router)

    uvicorn.run(app, host="0.0.0.0", port=8000)
