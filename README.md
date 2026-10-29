<div align="center">
  <h1>Python CQRS</h1>
  <p><strong>Commands, events, sagas, and reliable messaging for Python services.</strong></p>
  <p>
    <a href="https://pypi.org/project/python-cqrs/"><img src="https://img.shields.io/pypi/v/python-cqrs?label=pypi&logo=pypi" alt="PyPI version"></a>
    <a href="https://pypi.org/project/python-cqrs/"><img src="https://img.shields.io/pypi/pyversions/python-cqrs?logo=python&logoColor=white" alt="Python versions"></a>
    <a href="https://github.com/pypatterns/python-cqrs/actions/workflows/tests.yml"><img src="https://github.com/pypatterns/python-cqrs/actions/workflows/tests.yml/badge.svg" alt="Tests"></a>
    <a href="https://codecov.io/gh/vadikko2/python-cqrs"><img src="https://img.shields.io/codecov/c/github/vadikko2/python-cqrs?logo=codecov&logoColor=white" alt="Coverage"></a>
    <a href="https://pepy.tech/projects/python-cqrs"><img src="https://pepy.tech/badge/python-cqrs/month" alt="Downloads per month"></a>
    <a href="LICENSE"><img src="https://img.shields.io/pypi/l/python-cqrs" alt="License"></a>
    <a href="https://github.com/mjhea0/awesome-fastapi#developer-tools"><img src="https://img.shields.io/badge/Awesome%20FastAPI-listed-purple?style=flat" alt="Included in Awesome FastAPI"></a>
  </p>
  <p>
    <a href="https://mkdocs.python-cqrs.dev/latest/"><strong>Documentation</strong></a> ·
    <a href="https://github.com/pypatterns/python-cqrs/tree/master/examples">Examples</a> ·
    <a href="https://github.com/pypatterns/python-cqrs/releases">Changelog</a>
  </p>
</div>

---

python-cqrs routes every command and query through one mediator, and builds what a service needs once it talks to other services on top of that:

- **Sagas.** An orchestrated saga across services, with automatic compensation and recovery after a crash.
- **Reliable messaging.** A transactional outbox commits the broker message with the business row. A transactional inbox stores the incoming message before the handler runs.
- **Events.** Domain events are handled in this process, optionally in parallel. Notifications go to Kafka or RabbitMQ.
- **Streaming and chains.** A handler can yield results as they are ready, for example over SSE. A chain of handlers passes the request along until one takes it.
- **Testing.** `test_harness` runs the real mediators and records what was handled, published, and stored.

Request and event types are plain dataclasses by default, or Pydantic. Storage is SQLAlchemy or SQLModel on PostgreSQL or MySQL. FastAPI and FastStream plug in at the edge. Python 3.10+.

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Saga](#saga)
- [Outbox and inbox](#outbox-and-inbox)
- [Domain events](#domain-events)
- [FastAPI](#fastapi)
- [Testing](#testing)
- [When NOT to use python-cqrs](#when-not-to-use-python-cqrs)
- [Documentation](#documentation)
- [Agent skills](#agent-skills)
- [Contributing](#contributing)

## Installation

```bash
pip install python-cqrs
```

The core has no Pydantic, SQLAlchemy, or broker dependency. Add the extras you use:

```bash
pip install "python-cqrs[pydantic]"     # Pydantic request, response, and event types
pip install "python-cqrs[sqlalchemy]"   # Outbox, inbox, and saga storage on SQLAlchemy
pip install "python-cqrs[sqlmodel]"     # The same storage on SQLModel
pip install "python-cqrs[kafka]"        # Kafka broker
pip install "python-cqrs[rabbit]"       # RabbitMQ broker
pip install "python-cqrs[aiobreaker]"   # Circuit breaker for fallbacks
```

Protobuf, dishka, and the testing extras are listed on the [installation](https://mkdocs.python-cqrs.dev/latest/installation/) page. Upgrading from 4.x: [migration guide](https://mkdocs.python-cqrs.dev/latest/migration/4-to-5/).

## Quick start

A request declares its result type. The handler implements one use case. The mediator finds the handler and returns the result.

```python
import asyncio

import di

import cqrs
from cqrs.bootstrap import requests as bootstrap


class OrderCreated(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[OrderCreated]):
    sku: str
    amount: float


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder):
        return OrderCreated(order_id=f"ord-{request.sku}")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


async def main() -> None:
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=commands_mapper,
    )
    result = await mediator.send(CreateOrder(sku="book", amount=99.99))
    print(result.order_id)  # ord-book


asyncio.run(main())
```

`send` is typed: `result` is `OrderCreated`. A query is the same shape, bound through `queries_mapper`. A command that returns nothing is `cqrs.Request[None]`. Handler dependencies come from the DI container: `di`, dependency-injector, or dishka. [Request handlers](https://mkdocs.python-cqrs.dev/latest/request_handler/) · [Bootstrap](https://mkdocs.python-cqrs.dev/latest/bootstrap/) · [Dependency injection](https://mkdocs.python-cqrs.dev/latest/di/)

## Saga

Creating the order is one write in one database. Paying for it is not. Stock is reserved in the warehouse service and the card is charged in the payment service, and no SQL transaction spans both. If the charge fails, the reservation has to be released.

A saga is the list of those steps. Each step has `act` and `compensate`. When a step fails, the saga runs `compensate` on every step that already completed, in reverse order.

```python
import asyncio
import dataclasses
import uuid

import di

import cqrs
from cqrs.bootstrap import saga as saga_bootstrap
from cqrs.saga.models import SagaContext
from cqrs.saga.storage.memory import MemorySagaStorage


@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str
    amount: float
    reservation_id: str | None = None


class InventoryClient:
    async def reserve(self, order_id: str) -> str:
        print(f"reserved stock for {order_id}")
        return f"res-{order_id}"

    async def release(self, reservation_id: str) -> None:
        print(f"released {reservation_id}")


class PaymentClient:
    async def charge(self, order_id: str, amount: float) -> None:
        raise RuntimeError("card declined")

    async def refund(self, order_id: str) -> None:
        print(f"refunded {order_id}")


class ReserveStock(cqrs.SagaStepHandler[OrderContext, None]):
    def __init__(self, inventory: InventoryClient) -> None:
        self._inventory = inventory

    async def act(self, context: OrderContext):
        context.reservation_id = await self._inventory.reserve(context.order_id)
        return self._generate_step_result(None)

    async def compensate(self, context: OrderContext) -> None:
        if context.reservation_id:
            await self._inventory.release(context.reservation_id)


class ChargeCard(cqrs.SagaStepHandler[OrderContext, None]):
    def __init__(self, payments: PaymentClient) -> None:
        self._payments = payments

    async def act(self, context: OrderContext):
        await self._payments.charge(context.order_id, context.amount)
        return self._generate_step_result(None)

    async def compensate(self, context: OrderContext) -> None:
        await self._payments.refund(context.order_id)


class OrderSaga(cqrs.Saga[OrderContext]):
    steps = [ReserveStock, ChargeCard]


def sagas_mapper(mapper: cqrs.SagaMap) -> None:
    mapper.bind(OrderContext, OrderSaga)


async def main() -> None:
    mediator = saga_bootstrap.bootstrap(
        di_container=di.Container(),
        sagas_mapper=sagas_mapper,
        saga_storage=MemorySagaStorage(),
    )
    context = OrderContext(order_id="ord-1", amount=99.99)
    try:
        async for step in mediator.execute(context, saga_id=uuid.uuid4()):
            print(f"step done: {step.step_type.__name__}")
    except RuntimeError as error:
        print(f"saga failed: {error}")


asyncio.run(main())
```

```text
reserved stock for ord-1
step done: ReserveStock
released res-ord-1
saga failed: card declined
```

The reservation id travels on the context from `act` to `compensate`. `InventoryClient` and `PaymentClient` are resolved from the container, the same way a request handler gets its dependencies. `ChargeCard` never completed, so it is not compensated.

Persistent saga storage, recovery after a crash, and fallback steps with a circuit breaker are in the [Saga](https://mkdocs.python-cqrs.dev/latest/saga/) guide.

## Outbox and inbox

A broker delivers between services, but neither side can make the database write and the broker call one transaction. The outbox covers sending, the inbox covers receiving. Both keep the message in a table first.

### Outbox: publish with the business write

Other services need to hear that the order was placed. Committing the order and then publishing to Kafka is two writes: a crash or a broker outage between them loses the message, and nothing retries it.

The outbox makes it one write. The handler adds the event to the same transaction as the order row. A separate publisher reads pending rows and sends them to the broker, with at-least-once delivery.

```python
class OrderPlaced(cqrs.NotificationEvent):
    event_name: str = "order_placed"
    topic: str = "orders"


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self, outbox: cqrs.OutboxedEventRepository) -> None:
        self._outbox = outbox

    async def handle(self, request: CreateOrder):
        order_id = f"ord-{request.sku}"
        # write the order row through the same session here
        self._outbox.add(OrderPlaced(payload={"order_id": order_id}))
        await self._outbox.commit()
        return OrderCreated(order_id=order_id)
```

`SqlAlchemyOutboxedEventRepository` and `SqlModelOutboxedEventRepository` are bound to the session the handler writes with. The publisher is `EventProducer` over a Kafka or RabbitMQ broker. JSON is the default format, Protobuf is opt-in per event. [Outbox](https://mkdocs.python-cqrs.dev/latest/outbox/) · [Database support](https://mkdocs.python-cqrs.dev/latest/outbox/databases/) · [`examples/outbox/`](https://github.com/pypatterns/python-cqrs/tree/master/examples/outbox)

### Inbox: accept once, process once

The payment service reports back that the order was paid. A broker can redeliver the same message, and the consumer can crash after acking but before the handler has finished.

The subscriber only stores the message and commits, then acks. `add` returns `False` for a `message_id` that is already in the table, so a redelivery is not stored twice.

```python
class ConfirmPayment(cqrs.Request[None]):
    order_id: str


message_map = cqrs.InboxMessageMap()
message_map.register("ConfirmPayment", ConfirmPayment)


async def on_message(message_id: str, body: dict) -> None:
    async with session_factory() as session:
        inbox = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
        await inbox.add([(message_id, ConfirmPayment(order_id=body["order_id"]))])
        await session.commit()
```

A worker reads stored rows and runs each one through the mediator, where `ConfirmPayment` is bound to its handler like any other request. The handler writes through the same session, and one commit stores its changes together with the `processed` status.

```python
async def process_inbox() -> None:
    async with session_factory() as session:
        inbox = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
        processor = cqrs.InboxProcessor(
            inbox,
            message_map,
            request_mediator=mediator_for(session),
        )
        for row in await inbox.get_many(batch_size=100):
            await processor.process(row)
        await session.commit()
```

`mediator_for(session)` is your bootstrap with that session bound in the container. A failed row stays in the table and is retried up to five times. On PostgreSQL and MySQL, `get_many` locks rows with `SKIP LOCKED`, so several workers can run in parallel. [Inbox](https://mkdocs.python-cqrs.dev/latest/inbox/) · [`examples/inbox/`](https://github.com/pypatterns/python-cqrs/tree/master/examples/inbox)

## Domain events

Not every reaction leaves the service. Updating a read model or sending a confirmation email can follow the order inside the same process. The handler returns domain events through `events`. The mediator dispatches them to every bound handler after `handle` returns.

```python
class OrderPlaced(cqrs.DomainEvent):
    order_id: str


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    def __init__(self) -> None:
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: CreateOrder):
        order_id = f"ord-{request.sku}"
        self._events.append(OrderPlaced(order_id=order_id))
        return OrderCreated(order_id=order_id)


class UpdateOrderList(cqrs.EventHandler[OrderPlaced]):
    async def handle(self, event: OrderPlaced) -> None: ...


class SendConfirmation(cqrs.EventHandler[OrderPlaced]):
    async def handle(self, event: OrderPlaced) -> None: ...


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(OrderPlaced, UpdateOrderList)
    mapper.bind(OrderPlaced, SendConfirmation)


mediator = bootstrap.bootstrap(
    di_container=di.Container(),
    commands_mapper=commands_mapper,
    domain_events_mapper=domain_events_mapper,
    max_concurrent_event_handlers=2,
    concurrent_event_handle_enable=True,
)
```

One request type has exactly one handler. One event type can have many. With `concurrent_event_handle_enable=True` they run in parallel up to the limit.

To keep one database session open for the handler and its event handlers, pass `scope_strategy=cqrs.ScopeStrategy.SEND`. Event handlers then run in order, and `SEND` together with concurrent handlers raises `ValueError`. [Events](https://mkdocs.python-cqrs.dev/latest/event_handler/) · [Parallel processing](https://mkdocs.python-cqrs.dev/latest/event_handler/parallel_processing/) · [Scoped dependencies](https://mkdocs.python-cqrs.dev/latest/scoped_dependencies/)

## FastAPI

The route parses HTTP and sends the request. The use case stays in the handler, with no framework import.

```python
import functools

import fastapi

app = fastapi.FastAPI()


@functools.lru_cache
def mediator_factory() -> cqrs.RequestMediator:
    return bootstrap.bootstrap(di_container=di.Container(), commands_mapper=commands_mapper)


@app.post("/orders")
async def create_order(
    command: CreateOrder,
    mediator: cqrs.RequestMediator = fastapi.Depends(mediator_factory),
) -> dict[str, str]:
    result = await mediator.send(command)
    return {"order_id": result.order_id}
```

A consumer built on FastStream passes the decoded message to `EventMediator` the same way. Streaming handlers can push progress to the client over SSE. [FastAPI](https://mkdocs.python-cqrs.dev/latest/fastapi/) · [FastStream](https://mkdocs.python-cqrs.dev/latest/faststream/) · [SSE streaming](https://mkdocs.python-cqrs.dev/latest/stream_handling/fastapi_integration/)

## Testing

`test_harness` builds the same mediators as production and records what they did, so a test asserts on behavior instead of on mocks.

```python
import di

from cqrs.testing import test_harness


async def test_create_order() -> None:
    async with test_harness(di.Container(), commands_mapper=commands_mapper) as harness:
        result = await harness.mediator.send(CreateOrder(sku="book", amount=99.99))

        assert result.order_id == "ord-book"
        assert harness.requests.contains(CreateOrder)
        harness.invocations.assert_entered(CreateOrderHandler)
        harness.published.assert_empty()
```

`invocations` shows which handlers and saga steps ran, including `compensate`. `handled` holds domain events, `published` holds what reached the broker, and `outbox` holds rows that are not published yet. `sagas` holds status and step history. There is no pytest plugin: the same calls work under unittest. Install with `pip install "python-cqrs[testing]"`. [Testing](https://mkdocs.python-cqrs.dev/latest/testing/) · [`examples/testing/`](https://github.com/pypatterns/python-cqrs/tree/master/examples/testing)

## When NOT to use python-cqrs

- **One service, one database, no broker.** A plain database transaction is simpler. The mediator adds little.
- **One write and one message.** Use the outbox, not a saga.
- **Several services, and a failed step must be undone.** That is a saga.

A saga coordinates steps. The outbox makes one dual write reliable. Neither replaces the other. [When NOT to use python-cqrs](docs/when_not_to_use.md)

## Documentation

Full guides are at **[mkdocs.python-cqrs.dev](https://mkdocs.python-cqrs.dev/latest/)**. Topics not covered above:

- [Request and response types](https://mkdocs.python-cqrs.dev/latest/request_response_types/): dataclasses, Pydantic, msgspec, attrs, and others
- [Streaming](https://mkdocs.python-cqrs.dev/latest/stream_handling/): handlers that yield results as they finish
- [Chain of responsibility](https://mkdocs.python-cqrs.dev/latest/chain_of_responsibility/): several handlers for one request, the first match wins
- [Saga storage and recovery](https://mkdocs.python-cqrs.dev/latest/saga/recovery/): resume a saga after a crash
- [Fallback and circuit breaker](https://mkdocs.python-cqrs.dev/latest/saga/fallback/): a spare step or handler when the primary fails
- [Middlewares](https://mkdocs.python-cqrs.dev/latest/bootstrap/middlewares/) and [message brokers](https://mkdocs.python-cqrs.dev/latest/bootstrap/message_brokers/)
- [Protobuf](https://mkdocs.python-cqrs.dev/latest/protobuf/) and [Mermaid diagrams](https://mkdocs.python-cqrs.dev/latest/mermaid/)

## Agent skills

Skills tell a coding agent which python-cqrs feature fits the task and which API to call. Install them from your service's project root:

```bash
npx skills add pypatterns/python-cqrs --skill cqrs-setup -y
npx skills add pypatterns/python-cqrs --skill cqrs-eda -y
npx skills add pypatterns/python-cqrs --skill cqrs-saga -y
npx skills add pypatterns/python-cqrs --skill cqrs-streaming -y
npx skills add pypatterns/python-cqrs --skill cqrs-cor -y
npx skills add pypatterns/python-cqrs --skill cqrs-testing -y
```

Copy [`templates/AGENTS.md`](templates/AGENTS.md) into the application repository as well. [Agent skills](https://mkdocs.python-cqrs.dev/latest/skills/) · [Rules for agents](https://mkdocs.python-cqrs.dev/latest/agents/)

## Contributing

```bash
pip install -e ".[dev]"
pytest -c ./tests/pytest-config.ini ./tests/unit
pre-commit install
```

Checks, integration tests, and the 4.x maintenance branch are described in [CONTRIBUTING.md](CONTRIBUTING.md). Report vulnerabilities as described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
