<div align="center">
  <h1>Python CQRS</h1>
  <p><strong>Event-Driven Architecture Framework for Distributed Systems</strong></p>
  <p>
    <strong>Python 3.10+</strong> · Full documentation: <a href="https://mkdocs.python-cqrs.dev/">mkdocs.python-cqrs.dev</a>
  </p>
  <p>
    <a href="https://pypi.org/project/python-cqrs/">
      <img src="https://img.shields.io/pypi/pyversions/python-cqrs?logo=python&logoColor=white" alt="Python Versions">
    </a>
    <a href="https://pypi.org/project/python-cqrs/">
      <img src="https://img.shields.io/pypi/v/python-cqrs?label=pypi&logo=pypi" alt="PyPI version">
    </a>
    <a href="https://pepy.tech/projects/python-cqrs">
      <img src="https://pepy.tech/badge/python-cqrs" alt="Total downloads">
    </a>
    <a href="https://pepy.tech/projects/python-cqrs">
      <img src="https://pepy.tech/badge/python-cqrs/month" alt="Downloads per month">
    </a>
    <a href="https://codecov.io/gh/vadikko2/python-cqrs">
      <img src="https://img.shields.io/codecov/c/github/vadikko2/python-cqrs?logo=codecov&logoColor=white" alt="Coverage">
    </a>
    <a href="https://codspeed.io/vadikko2/python-cqrs?utm_source=badge">
      <img src="https://img.shields.io/endpoint?url=https://codspeed.io/badge.json" alt="CodSpeed">
    </a>
    <a href="https://mkdocs.python-cqrs.dev/">
      <img src="https://img.shields.io/badge/docs-mkdocs-blue?logo=readthedocs" alt="Documentation">
    </a>
    <a href="https://deepwiki.com/vadikko2/python-cqrs">
      <img src="https://deepwiki.com/badge.svg" alt="Ask DeepWiki">
    </a>
    <a href="https://github.com/mjhea0/awesome-fastapi#developer-tools">
      <img src="https://img.shields.io/badge/Awesome%20FastAPI-✓%20in%20list-purple?logo=eye&logoColor=white&style=flat-square" alt="Included in Awesome FastAPI">
    </a>
  </p>
</div>

> [!WARNING]
> **Breaking Changes in v5.0.0**
>
> Starting with version 5.0.0, Pydantic support will become optional. The default implementations of `Request`, `Response`, `DomainEvent`, and `NotificationEvent` will be migrated to dataclasses-based implementations.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Request and Response Types](#request-and-response-types)
- [Request Handlers](#request-handlers)
- [Mapping](#mapping)
- [DI container](#di-container)
- [Bootstrap](#bootstrap)
- [Saga Pattern](#saga-pattern)
- [Producing Notification Events](#producing-notification-events)
- [Kafka broker](#kafka-broker)
- [Transactional Outbox](#transactional-outbox)
- [Producing Events from Outbox to Kafka](#producing-events-from-outbox-to-kafka)
- [Transaction log tailing](#transaction-log-tailing)
- [Event Handlers](#event-handlers)
- [Integration with presentation layers](#integration-with-presentation-layers)
- [Protobuf messaging](#protobuf-messaging)
- [Contributing](#contributing)
- [Changelog](#changelog)
- [License](#license)

## Overview

An event-driven framework for building distributed systems in Python. It centers on CQRS (Command Query Responsibility Segregation) and extends into messaging, sagas, and reliable event delivery — so you can separate read and write flows, react to events from the bus, run distributed transactions with compensation, and publish events via Transaction Outbox. The result is clearer structure, better scalability, and easier evolution of the application.

This package is a fork of the [diator](https://github.com/akhundMurad/diator)
project ([documentation](https://akhundmurad.github.io/diator/)) with several enhancements, ordered by importance:

**Core framework**

1. Redesigned the event and request mapping mechanism to handlers;
2. `EventMediator` for handling `Notification` and `ECST` events coming from the bus;
3. `bootstrap` for easy setup;
4. **Transaction Outbox**, ensuring that `Notification` and `ECST` events are sent to the broker;
5. **Orchestrated Saga** pattern for distributed transactions with automatic compensation and recovery;
6. `StreamingRequestMediator` and `StreamingRequestHandler` for streaming requests with real-time progress updates;
7. **Chain of Responsibility** with `CORRequestHandler` for processing requests through multiple handlers in sequence;
8. **Parallel event processing** with configurable concurrency limits.

**Also**

- **Typing:** Pydantic [v2.*](https://docs.pydantic.dev/2.8/) and `IRequest`/`IResponse` interfaces — use Pydantic-based, dataclass-based, or custom Request/Response implementations.
- **Broker:** Kafka via [aiokafka](https://github.com/aio-libs/aiokafka).
- **Integration:** Ready for integration with FastAPI and FastStream.
- **Documentation:** Built-in Mermaid diagram generation (Sequence and Class diagrams).
- **Protobuf:** Interface-level support for converting Notification events to Protobuf and back.

## Installation

**Python 3.10+** is required.

```bash
pip install python-cqrs
```

Optional dependencies (see [pyproject.toml](https://github.com/vadikko2/python-cqrs/blob/master/pyproject.toml) for full list):

```bash
pip install python-cqrs[kafka]      # Kafka broker (aiokafka)
pip install python-cqrs[examples]    # FastAPI, FastStream, uvicorn, etc.
pip install python-cqrs[aiobreaker]  # Circuit breaker for saga fallbacks
```

## Quick Start

Define a command, a handler, bind them, and run via the mediator:

```python
import di
import cqrs
from cqrs.requests import bootstrap

class CreateOrderCommand(cqrs.Request):
    order_id: str
    amount: float

class CreateOrderHandler(cqrs.RequestHandler[CreateOrderCommand, None]):
    async def handle(self, request: CreateOrderCommand) -> None:
        print(f"Order {request.order_id}, amount {request.amount}")

def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrderCommand, CreateOrderHandler)

container = di.Container()
mediator = bootstrap.bootstrap(di_container=container, commands_mapper=commands_mapper)
await mediator.send(CreateOrderCommand(order_id="ord-1", amount=99.99))
```

For full setup with DI, events, and outbox, see the [documentation](https://mkdocs.python-cqrs.dev/) and the [examples](https://github.com/vadikko2/python-cqrs/tree/master/examples) directory.

## Request and Response Types

The library supports both Pydantic-based (`PydanticRequest`/`PydanticResponse`, aliased as `Request`/`Response`) and Dataclass-based (`DCRequest`/`DCResponse`) implementations. You can also implement custom classes by implementing the `IRequest`/`IResponse` interfaces directly.

```python
import dataclasses

# Pydantic-based (default)
class CreateUserCommand(cqrs.Request):
    username: str
    email: str

class UserResponse(cqrs.Response):
    user_id: str
    username: str

# Dataclass-based
@dataclasses.dataclass
class CreateProductCommand(cqrs.DCRequest):
    name: str
    price: float

@dataclasses.dataclass
class ProductResponse(cqrs.DCResponse):
    product_id: str
    name: str

# Custom implementation
class CustomRequest(cqrs.IRequest):
    def __init__(self, user_id: str, action: str):
        self.user_id = user_id
        self.action = action

    def to_dict(self) -> dict:
        return {"user_id": self.user_id, "action": self.action}

    @classmethod
    def from_dict(cls, **kwargs) -> "CustomRequest":
        return cls(user_id=kwargs["user_id"], action=kwargs["action"])

class CustomResponse(cqrs.IResponse):
    def __init__(self, result: str, status: int):
        self.result = result
        self.status = status

    def to_dict(self) -> dict:
        return {"result": self.result, "status": self.status}

    @classmethod
    def from_dict(cls, **kwargs) -> "CustomResponse":
        return cls(result=kwargs["result"], status=kwargs["status"])
```

A complete example can be found in [request_response_types.py](https://github.com/vadikko2/python-cqrs/blob/master/examples/request_response_types.py)

## Request Handlers

Request handlers can be divided into two main types:

### Command Handler

Command Handler executes the received command. The logic of the handler may include, for example, modifying the state of
the domain model.
As a result of executing the command, an event may be produced to the broker.
> [!TIP]
> By default, the command handler does not return any result, but it is not mandatory.

```python
from cqrs.requests.request_handler import RequestHandler
from cqrs.events.event import Event

class JoinMeetingCommandHandler(RequestHandler[JoinMeetingCommand, None]):

      def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
          self._meetings_api = meetings_api
          self._events: list[Event] = []

      @property
      def events(self) -> typing.List[events.Event]:
          return self._events

      async def handle(self, request: JoinMeetingCommand) -> None:
          await self._meetings_api.join_user(request.user_id, request.meeting_id)
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/request_handler.py)

### Query handler

Query Handler returns a representation of the requested data, for example, from
the [read model](https://radekmaziarka.pl/2018/01/08/cqrs-third-step-simple-read-model/#simple-read-model---to-the-rescue).
> [!TIP]
> The read model can be constructed based on domain events produced by the `Command Handler`.

```python
from cqrs.requests.request_handler import RequestHandler
from cqrs.events.event import Event

class ReadMeetingQueryHandler(RequestHandler[ReadMeetingQuery, ReadMeetingQueryResult]):

      def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
          self._meetings_api = meetings_api
          self._events: list[Event] = []

      @property
      def events(self) -> typing.List[events.Event]:
          return self._events

      async def handle(self, request: ReadMeetingQuery) -> ReadMeetingQueryResult:
          link = await self._meetings_api.get_link(request.meeting_id)
          return ReadMeetingQueryResult(link=link, meeting_id=request.meeting_id)

```

A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/request_handler.py)

### Streaming Request Handler

Streaming Request Handler processes requests incrementally and yields results as they become available.
This is particularly useful for processing large batches of items, file uploads, or any operation that benefits from
real-time progress updates.

`StreamingRequestHandler` works with `StreamingRequestMediator` that streams results to clients in real-time.

```python
import typing
from cqrs.requests.request_handler import StreamingRequestHandler
from cqrs.events.event import Event

class ProcessFilesCommandHandler(StreamingRequestHandler[ProcessFilesCommand, FileProcessedResult]):
    def __init__(self):
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(self, request: ProcessFilesCommand) -> typing.AsyncIterator[FileProcessedResult]:
        for file_id in request.file_ids:
            # Process file
            result = FileProcessedResult(file_id=file_id, status="completed", ...)
            # Emit events
            self._events.append(FileProcessedEvent(file_id=file_id, ...))
            yield result
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/streaming_handler_parallel_events.py)

### Chain of Responsibility Request Handler

Chain of Responsibility Request Handler implements the chain of responsibility pattern, allowing multiple handlers
to process a request in sequence until one successfully handles it. This pattern is particularly useful when you have
multiple processing strategies or need to implement fallback mechanisms.

Each handler in the chain decides whether to process the request or pass it to the next handler. The chain stops
when a handler successfully processes the request or when all handlers have been exhausted.

```python
import typing
from cqrs.requests.cor_request_handler import CORRequestHandler
from cqrs.events.event import Event

class CreditCardPaymentHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    def __init__(self, payment_service: PaymentServiceProtocol) -> None:
        self._payment_service = payment_service
        self._events: typing.List[Event] = []

    @property
    def events(self) -> typing.List[Event]:
        return self._events

    async def handle(self, request: ProcessPaymentCommand) -> PaymentResult | None:
        if request.payment_method == "credit_card":
            # Process credit card payment
            result = await self._payment_service.process_credit_card(request)
            self._events.append(PaymentProcessedEvent(...))
            return PaymentResult(success=True, transaction_id=result.id)

        # Pass to next handler
        return await self.next(request)

class PayPalPaymentHandler(CORRequestHandler[ProcessPaymentCommand, PaymentResult]):
    def __init__(self, paypal_service: PayPalServiceProtocol) -> None:
        self._paypal_service = paypal_service
        self._events: typing.List[Event] = []

    @property
    def events(self) -> typing.List[Event]:
        return self._events

    async def handle(self, request: ProcessPaymentCommand) -> PaymentResult | None:
        if request.payment_method == "paypal":
            # Process PayPal payment
            result = await self._paypal_service.process_payment(request)
            return PaymentResult(success=True, transaction_id=result.id)

        # Pass to next handler
        return await self.next(request)

# Chain registration
def payment_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ProcessPaymentCommand, [
        CreditCardPaymentHandler,
        PayPalPaymentHandler,
        DefaultPaymentHandler  # Fallback handler
    ])
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/cor_request_handler.py)

#### Mermaid Diagram Generation

The package includes built-in support for generating Mermaid diagrams from Chain of Responsibility handler chains.

```python
from cqrs.requests.mermaid import CoRMermaid

# Create Mermaid generator from handler chain
handlers = [CreditCardHandler, PayPalHandler, DefaultHandler]
generator = CoRMermaid(handlers)

# Generate Sequence diagram showing execution flow
sequence_diagram = generator.sequence()

# Generate Class diagram showing type structure
class_diagram = generator.class_diagram()
```

Complete example: [CoR Mermaid Diagrams](https://github.com/vadikko2/python-cqrs/blob/master/examples/cor_mermaid.py)

## Mapping

To bind commands, queries and events with specific handlers, you can use the registries `EventMap`, `RequestMap`, and `SagaMap`.

**Commands, queries and events:**

```python
from cqrs import requests, events

from app import commands, command_handlers
from app import queries, query_handlers
from app import events as event_models, event_handlers


def init_commands(mapper: requests.RequestMap) -> None:
    mapper.bind(commands.JoinMeetingCommand, command_handlers.JoinMeetingCommandHandler)

def init_queries(mapper: requests.RequestMap) -> None:
    mapper.bind(queries.ReadMeetingQuery, query_handlers.ReadMeetingQueryHandler)

def init_events(mapper: events.EventMap) -> None:
    mapper.bind(events.NotificationEvent[event_models.NotificationMeetingRoomClosed], event_handlers.MeetingRoomClosedNotificationHandler)
    mapper.bind(events.NotificationEvent[event_models.ECSTMeetingRoomClosed], event_handlers.UpdateMeetingRoomReadModelHandler)
```

**Chain of Responsibility** — bind a list of handlers (the first one that can handle the request processes it, otherwise the request is passed to the next):

```python
def payment_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(
        ProcessPaymentCommand,
        [
            CreditCardPaymentHandler,
            PayPalPaymentHandler,
            DefaultPaymentHandler,  # Fallback
        ],
    )
```

**Streaming handler** — bind a command to a `StreamingRequestHandler` (results are yielded as they become available):

```python
def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ProcessOrdersCommand, ProcessOrdersCommandHandler)  # StreamingRequestHandler
```

**Saga (including with fallback)** — bind the saga context type to the saga class in `SagaMap`:

```python
def saga_mapper(mapper: cqrs.SagaMap) -> None:
    mapper.bind(OrderContext, OrderSaga)
    mapper.bind(OrderContext, OrderSagaWithFallback)
```

## DI container

Use the following example to set up dependency injection in your command, query and event handlers. This will make
dependency management simpler.

The package supports two DI container libraries:

### di library

```python
import di
...

def setup_di() -> di.Container:
    """
    Binds implementations to dependencies
    """
    container = di.Container()
    container.bind(
        di.bind_by_type(
            dependent.Dependent(cqrs.SqlAlchemyOutboxedEventRepository, scope="request"),
            cqrs.OutboxedEventRepository
        )
    )
    container.bind(
        di.bind_by_type(
            dependent.Dependent(MeetingAPIImplementaion, scope="request"),
            MeetingAPIProtocol
        )
    )
    return container
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/dependency_injection.py)

### dependency-injector library

The package also supports [dependency-injector](https://github.com/ets-labs/python-dependency-injector) library.
You can use `DependencyInjectorCQRSContainer` adapter to integrate dependency-injector containers with python-cqrs.

```python
from dependency_injector import containers, providers
from cqrs.container.dependency_injector import DependencyInjectorCQRSContainer

class ApplicationContainer(containers.DeclarativeContainer):
    # Define your providers
    service = providers.Factory(ServiceImplementation)

# Create CQRS container adapter
cqrs_container = DependencyInjectorCQRSContainer(ApplicationContainer())

# Use with bootstrap
mediator = bootstrap.bootstrap(
    di_container=cqrs_container,
    commands_mapper=commands_mapper,
    ...
)
```

Complete examples can be found in:
- [Simple example](https://github.com/vadikko2/python-cqrs/blob/master/examples/dependency_injector_integration_simple_example.py)
- [Practical example with FastAPI](https://github.com/vadikko2/python-cqrs/blob/master/examples/dependency_injector_integration_practical_example.py)

## Bootstrap

The `python-cqrs` package implements a set of bootstrap utilities designed to simplify the initial configuration of an
application.

```python
import functools

from cqrs.events import bootstrap as event_bootstrap
from cqrs.requests import bootstrap as request_bootstrap

from app import dependencies, mapping, orm


@functools.lru_cache
def mediator_factory():
    return request_bootstrap.bootstrap(
        di_container=dependencies.setup_di(),
        commands_mapper=mapping.init_commands,
        queries_mapper=mapping.init_queries,
        domain_events_mapper=mapping.init_events,
        on_startup=[orm.init_store_event_mapper],
    )


@functools.lru_cache
def event_mediator_factory():
    return event_bootstrap.bootstrap(
        di_container=dependencies.setup_di(),
        events_mapper=mapping.init_events,
        on_startup=[orm.init_store_event_mapper],
    )


@functools.lru_cache
def saga_mediator_factory():
    return saga_bootstrap.bootstrap(
        di_container=dependencies.setup_di(),
        sagas_mapper=mapping.init_sagas,
        domain_events_mapper=mapping.init_events,
        saga_storage=MemorySagaStorage(),
    )
```

## Saga Pattern

The package implements the Orchestrated Saga pattern for managing distributed transactions across multiple services or operations.
Sagas enable eventual consistency by executing a series of steps where each step can be compensated if a subsequent step fails.

### Key Features

- **SagaStorage**: Persists saga state and execution history, enabling recovery of interrupted sagas
- **SagaLog**: Tracks all step executions (act/compensate) with status and timestamps
- **Recovery Mechanism**: Automatically recovers interrupted sagas from storage, ensuring eventual consistency
- **Automatic Compensation**: If any step fails, all previously completed steps are automatically compensated in reverse order
- **Fallback Pattern**: Define alternative steps to execute when primary steps fail, with optional Circuit Breaker protection
- **Mermaid Diagram Generation**: Generate Sequence and Class diagrams for documentation and visualization

### Example

```python
import dataclasses
import uuid
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler

@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str
    user_id: str
    items: list[str]
    total_amount: float
    inventory_reservation_id: str | None = None
    payment_id: str | None = None

# Define saga class with steps
class OrderSaga(Saga[OrderContext]):
    steps = [
        ReserveInventoryStep,
        ProcessPaymentStep,
    ]

# Execute saga via mediator
context = OrderContext(order_id="123", user_id="user_1", items=["item_1"], total_amount=100.0)
saga_id = uuid.uuid4()

async for step_result in mediator.stream(context, saga_id=saga_id):
    print(f"Step completed: {step_result.step_type.__name__}")
    # If any step fails, compensation happens automatically
```

### Fallback Pattern with Circuit Breaker

The saga pattern supports fallback steps that execute automatically when primary steps fail. You can also integrate Circuit Breaker protection to prevent cascading failures:

```python
from cqrs.saga.fallback import Fallback
from cqrs.adapters.circuit_breaker import AioBreakerAdapter
from cqrs.response import Response
from cqrs.saga.step import SagaStepHandler, SagaStepResult

class ReserveInventoryResponse(Response):
    reservation_id: str

class PrimaryStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    async def act(self, context: OrderContext) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        # Primary step that may fail
        raise RuntimeError("Service unavailable")

class FallbackStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    async def act(self, context: OrderContext) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        # Alternative step that executes when primary fails
        reservation_id = f"fallback_reservation_{context.order_id}"
        context.reservation_id = reservation_id
        return self._generate_step_result(ReserveInventoryResponse(reservation_id=reservation_id))

# Define saga with fallback and circuit breaker
class OrderSagaWithFallback(Saga[OrderContext]):
    steps = [
        Fallback(
            step=PrimaryStep,
            fallback=FallbackStep,
            circuit_breaker=AioBreakerAdapter(
                fail_max=2,  # Circuit opens after 2 failures
                timeout_duration=60,  # Wait 60 seconds before retry
            ),
        ),
    ]

# Optional: Using Redis for distributed circuit breaker state
# import redis
# from aiobreaker.storage.redis import CircuitRedisStorage
#
# def redis_storage_factory(name: str):
#     client = redis.from_url("redis://localhost:6379", decode_responses=False)
#     return CircuitRedisStorage(state="closed", redis_object=client, namespace=name)
#
# AioBreakerAdapter(..., storage_factory=redis_storage_factory)
```

When the primary step fails, the fallback step executes automatically. The Circuit Breaker opens after the configured failure threshold, preventing unnecessary load on failing services by failing fast.

The saga state and step history are persisted to `SagaStorage`. The `SagaLog` maintains a complete audit trail
of all step executions (both `act` and `compensate` operations) with timestamps and status information.
This enables the recovery mechanism to restore saga state and ensure eventual consistency even after system failures.

If a saga is interrupted (e.g., due to a crash), you can recover it using the recovery mechanism:

```python
from cqrs.saga.recovery import recover_saga

# Get saga instance from mediator's saga map (or keep reference to saga class)
saga = OrderSaga()

# Recover interrupted saga - will resume from last completed step
# or continue compensation if saga was in compensating state
await recover_saga(
    saga=saga,
    saga_id=saga_id,
    context_builder=OrderContext,
    container=di_container,  # Same container used in bootstrap
    storage=storage,
)

# Access execution history (SagaLog) for monitoring and debugging
history = await storage.get_step_history(saga_id)
for entry in history:
    print(f"{entry.timestamp}: {entry.step_name} - {entry.action} - {entry.status}")
```

The recovery mechanism ensures eventual consistency by:
- Loading the last known saga state from `SagaStorage`
- Checking the `SagaLog` to determine which steps were completed
- Resuming execution from the last completed step, or continuing compensation if the saga was in a compensating state
- Preventing duplicate execution of already completed steps

#### Mermaid Diagram Generation

The package includes built-in support for generating Mermaid diagrams from Saga instances.

```python
from cqrs.saga.mermaid import SagaMermaid

# Create Mermaid generator from saga class
saga = OrderSaga()
generator = SagaMermaid(saga)

# Generate Sequence diagram showing execution flow
sequence_diagram = generator.sequence()

# Generate Class diagram showing type structure
class_diagram = generator.class_diagram()
```

Complete example: [Saga Mermaid Diagrams](https://github.com/vadikko2/python-cqrs/blob/master/examples/saga_mermaid.py)

## Producing Notification Events

During the handling of a command, `cqrs.NotificationEvent` events may be generated and then sent to the broker.

```python
class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    def __init__(self):
        self._events = []

    @property
    def events(self):
        return self._events

    async def handle(self, request: JoinMeetingCommand) -> None:
        print(f"User {request.user_id} joined meeting {request.meeting_id}")
        self._events.append(
            cqrs.NotificationEvent[UserJoinedNotificationPayload](
                event_name="UserJoined",
                topic="user_notification_events",
                payload=UserJoinedNotificationPayload(
                    user_id=request.user_id,
                    meeting_id=request.meeting_id,
                ),
            )
        )
        self._events.append(
            cqrs.NotificationEvent[UserJoinedECSTPayload](
                event_name="UserJoined",
                topic="user_ecst_events",
                payload=UserJoinedECSTPayload(
                    user_id=request.user_id,
                    meeting_id=request.meeting_id,
                ),
            )
        )
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/event_producing.py)

After processing the command/request, if there are any Notification/ECST events,
the EventEmitter is invoked to produce the events via the message broker.

> [!WARNING]
> It is important to note that producing events using the events property parameter does not guarantee message delivery
> to the broker.
> In the event of broker unavailability or an exception occurring during message formation or sending, the message may
> be lost.
> This issue can potentially be addressed by configuring retry attempts for sending messages to the broker, but we
> recommend using the [Transaction Outbox](https://microservices.io/patterns/data/transactional-outbox.html) pattern,
> which is implemented in the current version of the python-cqrs package for this purpose.

## Kafka broker

```python
from cqrs.adapters import kafka as kafka_adapter
from cqrs.message_brokers import kafka as kafka_broker


producer = kafka_adapter.kafka_producer_factory(
    dsn="localhost:9092",
    topics=["test.topic1", "test.topic2"],
)
broker = kafka_broker.KafkaMessageBroker(producer)
await broker.send_message(...)
```

## Transactional Outbox

The package implements the [Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html)
pattern, which ensures that messages are produced to the broker according to the at-least-once semantics.

```python
class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    def __init__(self, outbox: cqrs.OutboxedEventRepository):
        self.outbox = outbox

    @property
    def events(self):
        return []

    async def handle(self, request: JoinMeetingCommand) -> None:
        print(f"User {request.user_id} joined meeting {request.meeting_id}")
        # Outbox repository is bound to a session (e.g. via DI request scope).
        # add() takes only the event; commit() persists the outbox and your changes.
        self.outbox.add(
            cqrs.NotificationEvent[UserJoinedNotificationPayload](
                event_name="UserJoined",
                topic="user_notification_events",
                payload=UserJoinedNotificationPayload(
                    user_id=request.user_id,
                    meeting_id=request.meeting_id,
                ),
            ),
        )
        self.outbox.add(
            cqrs.NotificationEvent[UserJoinedECSTPayload](
                event_name="UserJoined",
                topic="user_ecst_events",
                payload=UserJoinedECSTPayload(
                    user_id=request.user_id,
                    meeting_id=request.meeting_id,
                ),
            ),
        )
        await self.outbox.commit()
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/save_events_into_outbox.py)

> [!TIP]
> You can specify the name of the Outbox table using the environment variable `OUTBOX_SQLA_TABLE`.
> By default, it is set to `outbox`.

> [!TIP]
> If you use the protobuf events you should specify `OutboxedEventRepository`
> by [protobuf serialize](https://github.com/vadikko2/python-cqrs/blob/master/src/cqrs/serializers/protobuf.py). A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/save_proto_events_into_outbox.py)

## Producing Events from Outbox to Kafka

As an implementation of the Transactional Outbox pattern, the SqlAlchemyOutboxedEventRepository is available for use as
an access repository to the Outbox storage.
It can be utilized in conjunction with the KafkaMessageBroker.

```python
import asyncio
import cqrs
from cqrs.message_brokers import kafka
from cqrs.adapters import kafka as kafka_adapters
from cqrs.compressors import zlib

session_factory = async_sessionmaker(
   create_async_engine(
      f"mysql+asyncmy://{USER}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}",
      isolation_level="REPEATABLE READ",
   )
)

broker = kafka.KafkaMessageBroker(
   producer=kafka_adapters.kafka_producer_factory(dsn="localhost:9092"),
)

# SqlAlchemyOutboxedEventRepository expects (session, compressor), not session_factory.
async with session_factory() as session:
    repository = cqrs.SqlAlchemyOutboxedEventRepository(session, zlib.ZlibCompressor())
    producer = cqrs.EventProducer(broker, repository)

    async for messages in producer.event_batch_generator():
        for message in messages:
            await producer.send_message(message)
        await producer.repository.commit()
        await asyncio.sleep(10)
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/kafka_outboxed_event_producing.py)

## Transaction log tailing

If the Outbox polling strategy does not suit your needs, I recommend exploring
the [Transaction Log Tailing](https://microservices.io/patterns/data/transaction-log-tailing.html) pattern.
The current version of the python-cqrs package does not support the implementation of this pattern.

> [!TIP]
> However, it can be implemented
> using [Debezium + Kafka Connect](https://debezium.io/documentation/reference/stable/architecture.html),
> which allows you to produce all newly created events within the Outbox storage directly to the corresponding topic in
> Kafka (or any other broker).

## Event Handlers

Event handlers are designed to process `Notification` and `ECST` events that are consumed from the broker.
To configure event handling, you need to implement a broker consumer on the side of your application.
Below is an example of `Kafka event consuming` that can be used in the Presentation Layer.

```python
class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    def __init__(self):
        self._events = []

    @property
    def events(self):
        return self._events

    async def handle(self, request: JoinMeetingCommand) -> None:
        STORAGE[request.meeting_id].append(request.user_id)
        self._events.append(
            UserJoined(user_id=request.user_id, meeting_id=request.meeting_id),
        )
        print(f"User {request.user_id} joined meeting {request.meeting_id}")


class UserJoinedEventHandler(cqrs.EventHandler[UserJoined]):
    async def handle(self, event: UserJoined) -> None:
        print(f"Handle user {event.user_id} joined meeting {event.meeting_id} event")
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/domain_event_handler.py)

### Parallel Event Processing

Both `RequestMediator` and `StreamingRequestMediator` support parallel processing of domain events. You can control
the number of event handlers that run simultaneously using the `max_concurrent_event_handlers` parameter.

This feature is especially useful when:
- Multiple event handlers need to process events independently
- You want to improve performance by processing events concurrently
- You need to limit resource consumption by controlling concurrency

**Configuration:**

```python
from cqrs.requests import bootstrap

mediator = bootstrap.bootstrap_streaming(
    di_container=container,
    commands_mapper=commands_mapper,
    domain_events_mapper=domain_events_mapper,
    message_broker=broker,
    max_concurrent_event_handlers=3,  # Process up to 3 events in parallel
    concurrent_event_handle_enable=True,  # Enable parallel processing
)
```

> [!TIP]
> - Set `max_concurrent_event_handlers` to limit the number of simultaneously running event handlers
> - Set `concurrent_event_handle_enable=False` to disable parallel processing and process events sequentially
> - The default value for `max_concurrent_event_handlers` is `10` for `StreamingRequestMediator` and `1` for `RequestMediator`

## Integration with presentation layers

The framework is ready for integration with **FastAPI** and **FastStream**.

> [!TIP]
> I recommend reading the useful
> paper [Onion Architecture Used in Software Development](https://www.researchgate.net/publication/371006360_Onion_Architecture_Used_in_Software_Development).
> Separating user interaction and use-cases into Application and Presentation layers is a good practice.
> This can improve the `Testability`, `Maintainability`, `Scalability` of the application. It also provides benefits
> such as `Separation of Concerns`.

### FastAPI requests handling

If your application uses FastAPI (or any other asynchronous framework for creating APIs).
In this case you can use python-cqrs to route requests to the appropriate handlers implementing specific use-cases.

```python
import fastapi
import pydantic

from app import dependencies, commands

router = fastapi.APIRouter(prefix="/meetings")


@router.put("/{meeting_id}/{user_id}", status_code=status.HTTP_200_OK)
async def join_metting(
    meeting_id: pydantic.PositiveInt,
    user_id: typing.Text,
    mediator: cqrs.RequestMediator = fastapi.Depends(dependencies.mediator_factory),
):
    await mediator.send(commands.JoinMeetingCommand(meeting_id=meeting_id, user_id=user_id))
    return {"result": "ok"}
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/fastapi_integration.py)

### Kafka events consuming

If you build interaction by events over broker like `Kafka`, you can to implement an event consumer on your
application's side,
which will call the appropriate handler for each event.
An example of handling events from `Kafka` is provided below.

```python
import cqrs

import pydantic
import faststream
from faststream import kafka

broker = kafka.KafkaBroker(bootstrap_servers=["localhost:9092"])
app = faststream.FastStream(broker)


class HelloWorldPayload(pydantic.BaseModel):
    hello: str = pydantic.Field(default="Hello")
    world: str = pydantic.Field(default="World")


class HelloWorldECSTEventHandler(cqrs.EventHandler[cqrs.NotificationEvent[HelloWorldPayload]]):
    async def handle(self, event: cqrs.NotificationEvent[HelloWorldPayload]) -> None:
        print(f"{event.payload.hello} {event.payload.world}")  # type: ignore


@broker.subscriber(
    "hello_world",
    group_id="examples",
    auto_commit=False,
    value_deserializer=value_deserializer,
    decoder=decoder,
)
async def hello_world_event_handler(
    body: cqrs.NotificationEvent[HelloWorldPayload] | None,
    msg: kafka.KafkaMessage,
    mediator: cqrs.EventMediator = faststream.Depends(mediator_factory),
):
    if body is not None:
        await mediator.send(body)
    await msg.ack()
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/kafka_event_consuming.py)

### FastAPI SSE Streaming

`StreamingRequestMediator` is ready and designed for use with Server-Sent Events (SSE) in FastAPI applications.
This allows you to stream results to clients in real-time as they are processed.

**Example FastAPI endpoint with SSE:**

```python
import fastapi
import json
from cqrs.requests import bootstrap

def streaming_mediator_factory() -> cqrs.StreamingRequestMediator:
    return bootstrap.bootstrap_streaming(
        di_container=container,
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        message_broker=broker,
        max_concurrent_event_handlers=3,
        concurrent_event_handle_enable=True,
    )

@app.post("/process-files")
async def process_files_stream(
    command: ProcessFilesCommand,
    mediator: cqrs.StreamingRequestMediator = fastapi.Depends(streaming_mediator_factory),
) -> fastapi.responses.StreamingResponse:
    async def generate_sse():
        yield f"data: {json.dumps({'type': 'start', 'message': 'Processing...'})}\\n\\n"

        async for result in mediator.stream(command):
            sse_data = {
                "type": "progress",
                "data": result.to_dict(),
            }
            yield f"data: {json.dumps(sse_data)}\\n\\n"

        yield f"data: {json.dumps({'type': 'complete'})}\\n\\n"

    return fastapi.responses.StreamingResponse(
        generate_sse(),
        media_type="text/event-stream",
    )
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/fastapi_sse_streaming.py)

## Protobuf messaging

The `python-cqrs` package supports integration with [protobuf](https://developers.google.com/protocol-buffers/).
Notification events can be serialized to Protobuf and back: implement the `proto()` method (returns a protobuf message) and the class method `from_proto()` (creates an event instance from proto) on your event class.

Example (assuming generated `user_joined_pb2` from your `.proto` with fields `event_id`, `event_timestamp`, `event_name`, `payload`):

```python
import uuid
from datetime import datetime

import cqrs
from app.generated import user_joined_pb2  # generated from .proto


class UserJoinedPayload(cqrs.Response):
    user_id: str
    meeting_id: str


class UserJoinedNotificationEvent(cqrs.NotificationEvent[UserJoinedPayload]):
    """Event with Protobuf serialization support."""

    event_name: str = "UserJoined"

    def proto(self):
        msg = user_joined_pb2.UserJoinedNotification()
        msg.event_id = str(self.event_id)
        msg.event_timestamp = self.event_timestamp.isoformat()
        msg.event_name = self.event_name
        msg.payload.user_id = self.payload.user_id
        msg.payload.meeting_id = self.payload.meeting_id
        return msg

    @classmethod
    def from_proto(cls, proto_msg):
        return cls(
            event_id=uuid.UUID(proto_msg.event_id),
            event_timestamp=datetime.fromisoformat(proto_msg.event_timestamp),
            event_name=proto_msg.event_name,
            topic="user_notification_events",
            payload=UserJoinedPayload(
                user_id=proto_msg.payload.user_id,
                meeting_id=proto_msg.payload.meeting_id,
            ),
        )
```

## Contributing

Contributions are welcome. To develop locally:

1. Clone the repository and create a virtual environment.
2. Install dev dependencies: `pip install -e ".[dev]"`.
3. Run tests: `pytest`.
4. Install pre-commit and run hooks: `pre-commit install && pre-commit run --all-files`.

The project uses [ruff](https://docs.astral.sh/ruff/) for linting and [pyright](https://microsoft.github.io/pyright/) for type checking.

## Changelog

Release notes and migration guides are published on [GitHub Releases](https://github.com/vadikko2/python-cqrs/releases).

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
