"""
Example: Dependency Injector Integration (Practical / FastAPI)

This example demonstrates a practical application of integrating `dependency-injector` with the CQRS framework
within a FastAPI application. It covers domain events, repositories, and clean architecture principles.

Use case: Building a web API using FastAPI and CQRS where `dependency-injector` is used for
managing application components (repositories, handlers, services) and configuration.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/di/dependency_injector_practical.py

The example will start a FastAPI server on http://0.0.0.0:8000.

You can interact with the API:
- Open http://localhost:8000/docs in your browser to see Swagger UI
- Use the POST /api/users endpoint to register a user (Command)
- Use the GET /api/users endpoint to list users (Query)
- Use the GET /api/text-stream endpoint to see direct injection in FastAPI views

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Domain Layer:
   - Domain entities (User) and Repository interfaces
   - Domain Events (UserRegisteredEvent)

2. Application Layer:
   - Command Handlers using injected repositories
   - Event Handlers for side effects (logging)
   - Query Handlers for data retrieval

3. Infrastructure / DI:
   - `dependency-injector` wiring for FastAPI
   - Configuration management (providers.Configuration)
   - Resource management (logging setup)
   - Adapting the container for CQRS using `DependencyInjectorCQRSContainer`

4. FastAPI Integration:
   - Using `Lifespan` to manage container lifecycle
   - Injecting the `RequestMediator` into route handlers
   - Combining CQRS with standard FastAPI dependency injection

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - dependency-injector
   - fastapi
   - uvicorn
   - faker

================================================================================
"""

import asyncio
import functools
import hashlib
import logging
import uuid
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import sys
from typing import Generic, Optional, TypeVar

if sys.version_info >= (3, 11):
    from typing import Self  # novm
else:
    from typing_extensions import Self

import uvicorn

# The dependency-injector library
from dependency_injector import containers, providers
from dependency_injector.wiring import inject, Provide
from faker import Faker
from fastapi import Depends, FastAPI, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

import cqrs

# The CQRS container adapter for containers implemented using dependency-injector
from cqrs.container.dependency_injector import DependencyInjectorCQRSContainer

# The container protocol from this package
from cqrs.container.protocol import Container as CQRSContainer
from cqrs.requests import bootstrap

logger = logging.getLogger(__name__)


# ==============================
# Domain Layer
# ==============================


class User:
    def __init__(
        self,
        *,
        id: str,
        name: str,
        email: str,
        hashed_password: str,
    ) -> None:
        """User domain entity."""

        self.id = id
        self.name = name
        self.email = email
        self.hashed_password = hashed_password

    @classmethod
    def create(
        cls,
        *,
        id: Optional[str] = None,
        name: str,
        email: str,
        password: str,
    ) -> Self:
        """Factory method to create a new user."""

        return cls(
            id=id or str(uuid.uuid4()),
            name=name,
            email=email,
            hashed_password=hashlib.sha256(password.encode()).hexdigest(),
        )


class UserRepository(ABC):
    """Iterface for user repository implementations."""

    @abstractmethod
    async def save_user(self, user: User) -> None:
        pass

    @abstractmethod
    async def get_user_by_id(self, id: str) -> Optional[User]:
        pass

    @abstractmethod
    async def get_user_by_email(self, email: str) -> Optional[User]:
        pass

    @abstractmethod
    async def list_users(self, *, page: int, page_size: int) -> list[User]:
        pass


class FakeUserRepository(UserRepository):
    """Fake in-memory user repository seeded with random users for demonstration purposes."""

    NUM_USERS = 100

    def __init__(self) -> None:
        self._faker = Faker()
        self._users: list[User] = []

        # Generate and add NUM_USERS fake users on initialization
        for _ in range(self.NUM_USERS):
            self._users.append(
                User.create(
                    id=self._faker.uuid4(),
                    name=self._faker.name(),
                    email=self._faker.email(),
                    password=self._faker.password(),
                ),
            )

    async def save_user(self, user: User) -> None:
        # Find the existing user by id
        existing_user = await self.get_user_by_id(user.id)

        # If the user already exists by ID, update their data
        if existing_user:
            self._users.remove(existing_user)
            self._users.append(user)
            return

        # The user does not exist, so check if their email is already registered
        # Gather all existing emails
        existing_emails = [user.email for user in self._users]
        if user.email in existing_emails:
            raise ValueError(f"An user with email {user.email} is already registered")

        # Add the new user to the repository
        self._users.append(user)

    async def get_user_by_id(self, id: str) -> Optional[User]:
        return next((user for user in self._users if user.id == id), None)

    async def get_user_by_email(self, email: str) -> Optional[User]:
        return next((user for user in self._users if user.email == email), None)

    async def list_users(self, *, page: int, page_size: int) -> list[User]:
        return self._users[(page - 1) * page_size : page * page_size]


class UserRegisteredEvent(cqrs.DomainEvent, frozen=True):
    """Domain event indicating successful registration of a new user."""

    user_id: str
    user_name: str
    user_email: str


# ==============================
# Application Layer
# ==============================


class RegisterUserCommand(cqrs.Request):
    """CQRS command to register a new user."""

    name: str
    email: str
    password: str


class RegisterUserCommandHandler(cqrs.RequestHandler[RegisterUserCommand, None]):
    """Handles RegisterUserCommand, creates a user and emits UserRegisteredEvent."""

    def __init__(self, user_repository: UserRepository) -> None:
        self._user_repository = user_repository

        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: RegisterUserCommand) -> None:
        # Create the user domain object with hashed password
        user = User.create(
            name=request.name,
            email=request.email,
            password=request.password,
        )
        await self._user_repository.save_user(user)

        # Emit the user registered domain event
        self._events.append(
            UserRegisteredEvent(
                user_id=user.id,
                user_name=user.name,
                user_email=user.email,
            ),
        )


class UserRegisteredEventHandler(cqrs.EventHandler[UserRegisteredEvent]):
    """Handles UserRegisteredEvent (logs to stdout in this demo)."""

    async def handle(self, event: UserRegisteredEvent) -> None:
        print(f"User registered: {event.user_name} ({event.user_email})")


class ListUsersQuery(cqrs.Request):
    """CQRS query to list users."""

    page: int
    page_size: int


class UserDTO(BaseModel):
    user_id: str
    name: str
    email: str


class ListUsersQueryResponse(cqrs.Response):
    users: list[UserDTO]


class ListUsersQueryHandler(
    cqrs.RequestHandler[ListUsersQuery, ListUsersQueryResponse],
):
    """Handles ListUsersQuery, returns all users."""

    def __init__(self, user_repository: UserRepository) -> None:
        self._user_repository = user_repository

        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: ListUsersQuery) -> ListUsersQueryResponse:
        return ListUsersQueryResponse(
            users=[
                # Map domain entity to DTO
                UserDTO(user_id=user.id, name=user.name, email=user.email)
                for user in await self._user_repository.list_users(
                    page=request.page,
                    page_size=request.page_size,
                )
            ],
        )


class FakeTextStreamGenerator:
    """
    Demo generator for streaming random words asynchronously, simulating real-time text events.
    """

    def __init__(
        self,
        *,
        num_words: int,
    ) -> None:
        self._faker = Faker()
        self._num_words = num_words

    async def generate(self) -> AsyncGenerator[str, None]:
        # Asynchronously yield random words one at a time
        for i in range(self._num_words):
            # Generate a random word
            yield self._faker.word()

            # Simulate a short delay between words
            await asyncio.sleep(0.1)


# ==============================
# Bootstrap
# ==============================


def setup_logging() -> None:
    """
    Set up and configure root logging for the application,
    ensuring no duplicate StreamHandlers are added on reloads.
    Also sets log levels for key third-party libraries.
    """
    # Configure root logger once, avoiding duplicate handlers on reloads
    root_logger = logging.getLogger()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Add a StreamHandler if none exists
    has_stream_handler = any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers)
    if not has_stream_handler:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)

    # Set root level
    root_logger.setLevel(logging.DEBUG)

    # Tune third-party loggers
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("faker").setLevel(logging.INFO)

    # Ensure this module's logger propagates to root
    logger.propagate = True


class Container(containers.DeclarativeContainer):
    """
    Dependency-injector DI container holding configuration/providers for the example app.
    """

    # Leave the config empty for now since we will set it later
    # This gives us flexibility for loading app configurations, e.g., in different environments
    config = providers.Configuration()

    # It is common to have a resource provider for logging setup
    # Logging will be set up when init_resources() is called
    logging = providers.Resource(setup_logging)

    user_repository = providers.Factory(FakeUserRepository)

    register_user_command_handler = providers.Factory(
        RegisterUserCommandHandler,
        user_repository=user_repository,
    )

    user_registered_event_handler = providers.Factory(
        UserRegisteredEventHandler,
    )

    list_users_query_handler = providers.Factory(
        ListUsersQueryHandler,
        user_repository=user_repository,
    )

    text_stream_generator = providers.Factory(
        FakeTextStreamGenerator,
        num_words=config.text_stream_generator.num_words,
    )


@functools.cache
def setup_di_container() -> CQRSContainer:
    """
    Instantiate and wire the application's Dependency Injector container, then
    adapt it for python-cqrs using the DependencyInjectorCQRSContainer adapter.
    """

    # Load the configurations
    # In practice, you may want to use pydantic-settings or YAML
    app_settings = {
        "text_stream_generator": {
            "num_words": 30,
        },
    }

    # Create the container instance
    container = Container()

    # Set the configurations
    # Alternatively, you may use .from_pydantic() or .from_yaml()
    container.config.from_dict(app_settings)

    # Initialize the resources
    container.init_resources()

    # Wire the container
    container.wire(modules=[__name__])

    # Attach it to the CQRS container adapter
    cqrs_container = DependencyInjectorCQRSContainer()
    cqrs_container.attach_external_container(container)

    return cqrs_container


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(RegisterUserCommand, RegisterUserCommandHandler)


def queries_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ListUsersQuery, ListUsersQueryHandler)


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(UserRegisteredEvent, UserRegisteredEventHandler)


@functools.cache
def request_mediator_factory() -> cqrs.RequestMediator:
    """Factory for creating a cached CQRS request mediator using DI."""
    return bootstrap.bootstrap(
        di_container=setup_di_container(),
        commands_mapper=commands_mapper,
        queries_mapper=queries_mapper,
        domain_events_mapper=domain_events_mapper,
    )


# ==============================
# Presentation Layer
# ==============================


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Runs once at startup and shutdown: sets up CQRS container before requests are handled.
    """
    # Startup

    # Set up the dependency injector container on startup
    _cqrs_container = setup_di_container()

    # Yield control back to the FastAPI application
    yield

    # Shutdown


app = FastAPI(
    lifespan=lifespan,
    title="python-cqrs examples",
)


@app.get("/")
def home() -> HTMLResponse:
    """
    Simple homepage with link to the interactive API documentation.
    """
    return HTMLResponse(
        content="""
        <html>
            <body>
                <h1>Welcome to python-cqrs!</h1>
                <div>Go to <a href="/docs">docs</a> to play around with the API.</div>
            </body>
        </html>
        """,
    )


# Examples of using python-cqrs's request mediator pattern for endpoint implementations.
# This is the pattern you may use for MOST of the endpoints.

TResponseData = TypeVar("TResponseData")


class ApiResponse(BaseModel, Generic[TResponseData]):
    """
    API response base model.
    """

    success: bool
    message: Optional[str] = None
    error: Optional[str] = None
    data: Optional[TResponseData] = None


class ListUsersResponseData(BaseModel):
    users: list[UserDTO]


ListUsersResponse = ApiResponse[ListUsersResponseData]


@app.get(
    "/api/users",
    response_model=ListUsersResponse,
)
async def list_users(
    page: int = Query(default=1),
    page_size: int = Query(default=12),
    mediator: cqrs.RequestMediator = Depends(request_mediator_factory),
):
    try:
        query_response: ListUsersQueryResponse = await mediator.send(
            ListUsersQuery(
                page=page,
                page_size=page_size,
            ),
        )

        # Create the response data
        response_data = ListUsersResponseData(
            users=[
                UserDTO(
                    user_id=user.user_id,
                    name=user.name,
                    email=user.email,
                )
                for user in query_response.users
            ],
        )

        return ListUsersResponse(
            success=True,
            message="Users listed successfully",
            data=response_data,
        )

    except Exception as e:
        error_message = f"Error listing users: {e}"
        logger.error(f"{error_message}")  # Log the error for debugging

        return ListUsersResponse(
            success=False,
            error=error_message,
        )


class RegisterUserRequest(BaseModel):
    """
    Request schema for registering a user through the API.
    """

    name: str
    email: str
    password: str


class RegisterUserResponseData(BaseModel):
    user_name: str
    user_email: str


RegisterUserResponse = ApiResponse[RegisterUserResponseData]


@app.post(
    "/api/users",
    response_model=RegisterUserResponse,
)
async def register_user(
    request: RegisterUserRequest,
    mediator: cqrs.RequestMediator = Depends(request_mediator_factory),
):
    try:
        # Use CQRS request mediator to handle user registration command
        await mediator.send(
            RegisterUserCommand(
                name=request.name,
                email=request.email,
                password=request.password,
            ),
        )

        # Create the response data
        response_data = RegisterUserResponseData(
            user_name=request.name,
            user_email=request.email,
        )

        return RegisterUserResponse(
            # Indicate successful registration
            success=True,
            message=f"User {request.name} registered successfully",
            data=response_data,
        )

    except Exception as e:
        error_message = f"Error registering user {request.name}: {e}"
        logger.error(f"{error_message}")  # Log the error for debugging

        return RegisterUserResponse(
            success=False,
            error=error_message,
        )


# Example SSE endpoint demonstrating dependency-injector's injection pattern


@app.get("/api/text-stream")
@inject
async def get_text_stream(
    text_stream_generator: FakeTextStreamGenerator = Depends(
        Provide[Container.text_stream_generator],
    ),
) -> StreamingResponse:
    async def generator() -> AsyncGenerator[str, None]:
        # Yield strings formatted as event-stream (SSE) data
        async for word in text_stream_generator.generate():
            # event-stream format
            yield f"data: {word}\n\n"

    return StreamingResponse(
        content=generator(),
        media_type="text/event-stream",
    )


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
    )
