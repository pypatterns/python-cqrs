<div align="center">
  <h1>Python CQRS</h1>
  <p><strong>CQRS and event-driven architecture for Python services, from a single API to distributed systems.</strong></p>
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
    <a href="https://github.com/mjhea0/awesome-fastapi#developer-tools">
      <img src="https://img.shields.io/badge/Awesome%20FastAPI-✓%20in%20list-purple?logo=eye&logoColor=white&style=flat-square" alt="Included in Awesome FastAPI">
    </a>
  </p>
  <p>
    <a href="https://mkdocs.python-cqrs.dev/latest/"><strong>Documentation</strong></a> ·
    <a href="https://github.com/pypatterns/python-cqrs/tree/master/examples">Examples</a> ·
    <a href="https://github.com/pypatterns/python-cqrs/releases">Changelog</a>
  </p>
</div>

As a service grows, business logic spreads across routes, consumers, and workers. python-cqrs puts each use case in one handler behind a mediator. Routes and consumers only send requests, and side effects become events with their own handlers.

Once services talk to each other, a database write and a broker message are no longer atomic. The outbox and inbox keep messages from being lost or handled twice, and a saga undoes completed steps when a later one fails.

Dataclasses or Pydantic · SQLAlchemy or SQLModel · Kafka or RabbitMQ · FastAPI and FastStream · Python 3.10+

## Installation

```bash
pip install python-cqrs
pip install "python-cqrs[pydantic,sqlalchemy,kafka]"  # add what you use
```

Extras: `pydantic`, `sqlalchemy`, `sqlmodel`, `kafka`, `rabbit`, `protobuf`, `aiobreaker`, `dishka`, `testing`. Details: [installation](https://mkdocs.python-cqrs.dev/latest/installation/). From 4.x: [migration guide](https://mkdocs.python-cqrs.dev/latest/migration/4-to-5/).

## Quick start

```python
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


mediator = bootstrap.bootstrap(di_container=di.Container(), commands_mapper=commands_mapper)
result = await mediator.send(CreateOrder(sku="book", amount=99.99))  # OrderCreated(order_id="ord-book")
```

`result` is typed as `OrderCreated`. Queries use the same shape. Handler dependencies come from `di`, dependency-injector, or dishka. [Request handlers](https://mkdocs.python-cqrs.dev/latest/request_handler/) · [Bootstrap](https://mkdocs.python-cqrs.dev/latest/bootstrap/) · [Dependency injection](https://mkdocs.python-cqrs.dev/latest/di/)

## Saga

Paying for an order touches the warehouse service and the payment service, and no transaction spans both. If the charge fails, the stock stays reserved. A saga runs the steps in order and, on failure, calls `compensate` on the completed ones in reverse.

```python
import dataclasses
from cqrs.bootstrap import saga as saga_bootstrap
from cqrs.saga.models import SagaContext


@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str
    amount: float
    reservation_id: str | None = None


class ReserveStock(cqrs.SagaStepHandler[OrderContext, None]):
    def __init__(self, inventory: InventoryClient) -> None:
        self._inventory = inventory

    async def act(self, context: OrderContext):
        context.reservation_id = await self._inventory.reserve(context.order_id)
        return self._generate_step_result(None)

    async def compensate(self, context: OrderContext) -> None:
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


mediator = saga_bootstrap.bootstrap(di_container=di.Container(), sagas_mapper=sagas_mapper)

# if ChargeCard raises, ReserveStock.compensate releases the stock
async for step in mediator.execute(OrderContext(order_id="ord-1", amount=99.99)):
    print(step.step_type.__name__)
```

Storage, recovery after a crash, and fallback steps: [Saga](https://mkdocs.python-cqrs.dev/latest/saga/).

## Outbox and inbox

### Outbox

Commit the order, then publish to Kafka: a crash between the two loses the event. The outbox writes the event in the same transaction as the order. A publisher sends it later, at least once.

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

[Outbox](https://mkdocs.python-cqrs.dev/latest/outbox/) · [Database support](https://mkdocs.python-cqrs.dev/latest/outbox/databases/) · [`examples/outbox/`](https://github.com/pypatterns/python-cqrs/tree/master/examples/outbox)

### Inbox

The broker redelivers, and a consumer can crash halfway through a handler. The FastStream subscriber only stores the message. A repeated `payment_id` is skipped.

```python
class ConfirmPayment(cqrs.Request[None]):
    order_id: str


@broker.subscriber("payments")
async def on_payment(
    body: dict,
    inbox: cqrs.InboxRepository = Depends(inbox_repository),  # commits after return, then ack
) -> None:
    await inbox.add([(body["payment_id"], ConfirmPayment(order_id=body["order_id"]))])
```

A worker runs stored rows through the mediator. One commit saves the handler's changes and the `processed` status.

```python
processor = cqrs.InboxProcessor(inbox, message_map, request_mediator=mediator)
for row in await inbox.get_many():
    await processor.process(row)
await session.commit()
```

[Inbox](https://mkdocs.python-cqrs.dev/latest/inbox/) · [`examples/inbox/`](https://github.com/pypatterns/python-cqrs/tree/master/examples/inbox)

## Domain events

A placed order also updates a read model and sends an email. In the command handler, that couples unrelated work. Instead, the handler emits an event, and each reaction is its own handler.

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
```

Pass `domain_events_mapper` to `bootstrap`. Event handlers run after the command, in parallel when enabled. [Events](https://mkdocs.python-cqrs.dev/latest/event_handler/) · [Parallel processing](https://mkdocs.python-cqrs.dev/latest/event_handler/parallel_processing/) · [Scoped dependencies](https://mkdocs.python-cqrs.dev/latest/scoped_dependencies/)

## FastAPI

The route parses HTTP and sends the request. The handler does not import FastAPI.

```python
@app.post("/orders")
async def create_order(
    command: CreateOrder,
    mediator: cqrs.RequestMediator = Depends(mediator_factory),
) -> dict[str, str]:
    result = await mediator.send(command)
    return {"order_id": result.order_id}
```

FastStream consumers and SSE streaming work the same way. [FastAPI](https://mkdocs.python-cqrs.dev/latest/fastapi/) · [FastStream](https://mkdocs.python-cqrs.dev/latest/faststream/) · [SSE streaming](https://mkdocs.python-cqrs.dev/latest/stream_handling/fastapi_integration/)

## Testing

Mocks show that a method was called, not what the service did. `test_harness` runs the real mediators and records what was handled, published, and stored.

```python
from cqrs.testing import test_harness


async def test_create_order() -> None:
    # Arrange
    async with test_harness(di.Container(), commands_mapper=commands_mapper) as harness:

        # Act
        result = await harness.mediator.send(CreateOrder(sku="book", amount=99.99))

        # Assert
        assert result.order_id == "ord-book"
        assert harness.requests.contains(CreateOrder)
        harness.invocations.assert_entered(CreateOrderHandler)
        harness.published.assert_empty()
```

The same journals cover domain events, the outbox, and saga compensation. [Testing](https://mkdocs.python-cqrs.dev/latest/testing/) · [`examples/testing/`](https://github.com/pypatterns/python-cqrs/tree/master/examples/testing)

## When NOT to use python-cqrs

CQRS, Outbox, and Saga are tools for specific problems — not a default for every service.

- **Prefer a local DB transaction** for single-service CRUD (one database, no broker, no compensation).
- **Use [Transactional Outbox](#outbox-and-inbox)** when you dual-write to a database **and** a message broker. Commit the business row and the outbox row together; a publisher drains the outbox later.
- **Use [Saga](#saga)** when a multi-step flow crosses service/database boundaries and a later step must be compensated if it fails.

Do not add a Saga for a single `UPDATE` in one service, and do not use a Saga as a substitute for Outbox. Full write-up: [When NOT to use python-cqrs](docs/when_not_to_use.md).

## Documentation

Full guides: **[mkdocs.python-cqrs.dev](https://mkdocs.python-cqrs.dev/latest/)**. Also there:

- [Request and response types](https://mkdocs.python-cqrs.dev/latest/request_response_types/): dataclasses, Pydantic, msgspec, attrs, and others
- [Streaming](https://mkdocs.python-cqrs.dev/latest/stream_handling/): handlers that yield results as they finish
- [Chain of responsibility](https://mkdocs.python-cqrs.dev/latest/chain_of_responsibility/): several handlers for one request, the first match wins
- [Saga storage and recovery](https://mkdocs.python-cqrs.dev/latest/saga/recovery/): resume a saga after a crash
- [Fallback and circuit breaker](https://mkdocs.python-cqrs.dev/latest/saga/fallback/): a spare step or handler when the primary fails
- [Middlewares](https://mkdocs.python-cqrs.dev/latest/bootstrap/middlewares/) and [message brokers](https://mkdocs.python-cqrs.dev/latest/bootstrap/message_brokers/)
- [Protobuf](https://mkdocs.python-cqrs.dev/latest/protobuf/) and [Mermaid diagrams](https://mkdocs.python-cqrs.dev/latest/mermaid/)

## Agent skills

Skills tell a coding agent which feature fits the task. Install from your service's root:

```bash
npx skills add pypatterns/python-cqrs --skill cqrs-setup -y
npx skills add pypatterns/python-cqrs --skill cqrs-eda -y
npx skills add pypatterns/python-cqrs --skill cqrs-saga -y
npx skills add pypatterns/python-cqrs --skill cqrs-streaming -y
npx skills add pypatterns/python-cqrs --skill cqrs-cor -y
npx skills add pypatterns/python-cqrs --skill cqrs-testing -y
```

Also copy [`templates/AGENTS.md`](templates/AGENTS.md) into the application. [Agent skills](https://mkdocs.python-cqrs.dev/latest/skills/) · [Rules for agents](https://mkdocs.python-cqrs.dev/latest/agents/)

## Contributing

```bash
pip install -e ".[dev]"
pytest -c ./tests/pytest-config.ini ./tests/unit
pre-commit install
```

Details: [CONTRIBUTING.md](CONTRIBUTING.md). Vulnerabilities: [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
