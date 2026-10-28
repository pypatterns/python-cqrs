# Agents using python-cqrs

Copy this file into an application that depends on `python-cqrs` 5.2 or newer. It describes which feature to use, which to skip, and how to wire the frameworks this library supports. It does not describe how to change the library.

Full write-ups live at [mkdocs.python-cqrs.dev](https://mkdocs.python-cqrs.dev/). Examples live under `examples/` in the library repository.

## Pick one feature

| Situation | Use |
| --- | --- |
| Change state in this service and return when the change is done | Command: `cqrs.Request[None]` or `cqrs.Request[Result]`, `RequestMediator.send` |
| Read state and return it | Query: same `cqrs.Request[Result]`, same mediator. The handler does not write |
| Another part of this process must react before `send` returns | `DomainEvent` on the handler `events` property |
| Another service must hear about the change, and losing the message is acceptable | `NotificationEvent` on `events`, plus a real broker on bootstrap |
| The database row and the broker message must commit together | Transactional outbox in the same session. Leave `events` empty for that notification |
| A broker or HTTP endpoint only accepts a command or event, and a separate process must run it and survive a repeat delivery | Transactional inbox. Do not call `mediator.send` in that subscriber or route |
| Several steps cross services or databases and a finished step must be undone | Orchestrated saga |
| Several strategies can handle one request, and the first match wins | Chain of responsibility |
| One request produces many results over time (progress, batch, SSE) | `StreamingRequestHandler` and `StreamingRequestMediator.stream` |
| An HTTP or broker payload comes from outside the process | Validate at that boundary (`PydanticRequest` / `PydanticNotificationEvent`, or the framework model) |
| A message was already validated, or this process built it | Dataclass `Request` / `DomainEvent`. Check business rules in the handler |

Startup is not `cqrs.bootstrap`. Import the submodule:

- `cqrs.bootstrap.requests.bootstrap` builds `RequestMediator` (commands and queries).
- `cqrs.bootstrap.requests.bootstrap_streaming` builds `StreamingRequestMediator`.
- `cqrs.bootstrap.events.bootstrap` builds `EventMediator` (events that arrive from outside).
- `cqrs.bootstrap.saga.bootstrap` builds `SagaMediator`.

A missing `message_broker` becomes a broker that discards messages. Pass a real broker when a notification must leave the process. Logging middleware is added when the list does not already contain it.

### Agent skills

Two registries: [skills.sh](https://skills.sh) and [ClawHub](https://clawhub.ai). From the application root, not a clone of `pypatterns/python-cqrs`, either block is enough. Files land in `<app>/.agents/skills/<name>/`.

```bash
npx skills add pypatterns/python-cqrs --skill cqrs-setup --skill cqrs-eda --skill cqrs-saga --skill cqrs-streaming --skill cqrs-cor -y
```

```bash
clawhub install @vadikko2/cqrs-setup
clawhub install @vadikko2/cqrs-eda
clawhub install @vadikko2/cqrs-saga
clawhub install @vadikko2/cqrs-streaming
clawhub install @vadikko2/cqrs-cor
```

- [`.agents/skills/cqrs-setup/SKILL.md`](.agents/skills/cqrs-setup/SKILL.md) picks the feature, then builds a command and a query.
- [`.agents/skills/cqrs-eda/SKILL.md`](.agents/skills/cqrs-eda/SKILL.md) builds a domain event, a notification, the outbox, the inbox, or parallel domain handlers.
- [`.agents/skills/cqrs-saga/SKILL.md`](.agents/skills/cqrs-saga/SKILL.md) builds a step, its compensation, and recovery.
- [`.agents/skills/cqrs-streaming/SKILL.md`](.agents/skills/cqrs-streaming/SKILL.md) and [`.agents/skills/cqrs-cor/SKILL.md`](.agents/skills/cqrs-cor/SKILL.md) are the short how-tos. Parallel domain handlers stay in `cqrs-eda`. Their Use when and Skip when pairs stay in `cqrs-setup`.

Start with `cqrs-setup`.

## Commands and queries

Commands and queries are both `cqrs.Request`. There is no `Query` class, no `Command` class, and no `CommandBus`. There is no `register_handler` decorator. Register with `mapper.bind(RequestType, Handler)`.

One handler per request type. A second `bind` for the same request raises `KeyError`. A chain of responsibility is still one registration: pass the ordered list of handlers.

`commands_mapper` and `queries_mapper` both fill the same `RequestMap`. Split them so reads and writes stay obvious. The library does not enforce the split. Binding the same request type from both mappers raises `KeyError`.

Use a command when the handler changes state this service owns: create, update, delete, or a state transition. A command with an empty body is `Request[None]`. A command may return a small result (`Request[CreatedOrder]`) when the caller needs the id or status. Record follow-up facts on `events` or in the outbox.

Use a query when the handler only reads. Return `Request[QueryResult]`. Leave `events` empty. Do not insert, update, delete, or publish from a query handler.

Skip a command or a query when the operation is a single local database statement with no handler dependencies, no events, and no second caller. A function on the repository is enough. Add a request when a second entry point (HTTP, worker, test) must call the same use case, or when the use case grows events, an outbox row, or injected ports.

```python
class ReadMeetingQueryResult(cqrs.Response):
    users: list[str]

class ReadMeetingQuery(cqrs.Request[ReadMeetingQueryResult]):
    meeting_id: str

class ReadMeetingQueryHandler(cqrs.RequestHandler[ReadMeetingQuery]):
    def __init__(self, meetings: MeetingReader) -> None:
        self._meetings = meetings

    async def handle(self, request: ReadMeetingQuery):
        return ReadMeetingQueryResult(users=await self._meetings.users(request.meeting_id))

class JoinMeetingCommand(cqrs.Request[None]):
    user_id: str
    meeting_id: str

class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand]):
    def __init__(self, meetings: MeetingWriter) -> None:
        self._meetings = meetings
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        return self._events

    async def handle(self, request: JoinMeetingCommand):
        await self._meetings.join(request.user_id, request.meeting_id)
        self._events.append(UserJoined(user_id=request.user_id, meeting_id=request.meeting_id))
```

Do not annotate `handle` with `-> ReadMeetingQueryResult`. The return type is the request's parameter. `await mediator.send(query)` is `ReadMeetingQueryResult`.

Already written `Request` and `RequestHandler[Request, Result]` stay as they are. Do not rewrite them. `send` on an unparameterized `Request` is `Any`.

Take dependencies in `__init__`. The container resolves the handler. Do not call a mediator from inside `handle`. Do not construct the handler in the route.

## Domain events

A `DomainEvent` is a fact other handlers in this process react to. The mediator emits `events` after `handle` returns, before `send` returns. Several `EventHandler` classes may bind to one event. `EventMap.bind` appends.

Use a domain event when the command should stay small and a separate reaction belongs in the same process and the same unit of work: update a read model, write an audit row, kick a follow-up that must finish before the caller continues.

Skip a domain event when the next step is the rest of the same function, when nothing else listens, or when the work must survive a crash and reach another service. That last case is a notification plus the outbox. Skip a domain event for slow I/O (email, HTTP to another service): the caller of `send` waits for every domain handler.

Register in-process reactions on `domain_events_mapper` of `cqrs.bootstrap.requests.bootstrap`. Events that arrive from a broker use `cqrs.bootstrap.events.bootstrap` and `EventMediator.send`, not the request mediator.

Keep domain handlers idempotent when a command can be retried. With `ScopeStrategy.SEND`, handlers run one after another on the command's session. `ScopeStrategy.SEND` cannot run with `concurrent_event_handle_enable=True`.

## Notification events

A `NotificationEvent` is what another service receives. Set `event_name`, `payload`, and `topic`. Fields are keyword-only.

```python
@dataclasses.dataclass(frozen=True)
class OrderCreatedPayload:
    order_id: str
    amount: float

class OrderCreated(cqrs.NotificationEvent[OrderCreatedPayload]):
    event_name: str = "order_created"
    topic: str = "order_events"
    payload: OrderCreatedPayload
```

The payload class follows the same validation rule as the event: a dataclass payload for messages this process builds, a Pydantic model when the payload crosses a trust boundary. `NotificationEvent[Payload]` needs a payload type that the chosen serializer can turn into a dict.

Use a notification on the handler `events` list only when losing it is acceptable (metrics, a best-effort signal, a dev broker). Bootstrap must receive a real `message_broker`. The default broker drops the message.

Skip a notification when no other service consumes it. A domain event is the in-process tool. Skip publishing with `broker.send` next to `session.commit()` in the handler. A crash between those two writes loses the message.

## Transactional outbox

Use the outbox when this service commits a business row and must publish a `NotificationEvent`. `repository.add(event)` and the business `session.add(...)` share one `AsyncSession`. `await repository.commit()` commits both. A separate publisher process runs `EventProducer`, which reads unpublished rows and sends them. Delivery is at-least-once. Consumers must be idempotent.

Skip the outbox when nothing is published outside the database transaction. Skip it as a replacement for a saga: the outbox stores one message reliably, it does not compensate a later step. Skip calling the publisher from the request handler. The handler only writes the row.

Register every `event_name` before the first read or write. A second registration of the same name raises `KeyError`. Prefer an `OutboxedEventMap()` instance when tests must not share the process-global map. JSON is the default codec. Pass a `ProtobufEventSerializer` only for an event whose stored bytes are protobuf, and implement `proto` / `from_proto` on that event. Changing a codec does not rewrite rows already stored.

```python
event_map = cqrs.OutboxedEventMap()
event_map.register("order_created", OrderCreated)

repository = cqrs.SqlAlchemyOutboxedEventRepository(session, event_map=event_map)
session.add(order_row)
repository.add(
    OrderCreated(
        event_name="order_created",
        topic="order_events",
        payload=OrderCreatedPayload(order_id=order_id, amount=amount),
    ),
)
await repository.commit()
```

`EventProducer(broker, repository)` belongs in a publisher loop. After each batch, `commit` the repository so status updates stick. Rows that fail until `flush_counter` reaches five leave the selectable batch. Inspect those rows. The producer will not yield them again.

On PostgreSQL, MySQL, and MariaDB, `get_many` holds that batch with `FOR UPDATE SKIP LOCKED` until the caller commits or rolls back; SQLite does not lock the rows.

Install one storage extra and use it for both the outbox and saga tables in a process:

| App ORM | Extra | Outbox | Saga storage |
| --- | --- | --- | --- |
| SQLAlchemy 2 async | `python-cqrs[sqlalchemy]` | `cqrs.SqlAlchemyOutboxedEventRepository` | `cqrs.saga.storage.sqlalchemy.SqlAlchemySagaStorage` |
| SQLModel | `python-cqrs[sqlmodel]` | `cqrs.outbox.sqlmodel.SqlModelOutboxedEventRepository` | `cqrs.saga.storage.sqlmodel.SqlModelSagaStorage` |

Outbox takes the session the business write uses. Saga storage takes an `async_sessionmaker`, not one session. SQLModel saga storage needs `async_sessionmaker(..., class_=sqlmodel.ext.asyncio.session.AsyncSession)`. SQLModel models use their own `Base.metadata`. Call `cqrs.rebind_outbox_model` for a SQLAlchemy base, or `cqrs.outbox.sqlmodel.rebind_outbox_model` for a SQLModel base, when Alembic must see the outbox table on the application metadata. On PostgreSQL the constraint name `event_id_unique_index` must stay unique, so drop or rename a previous outbox table before creating the rebound one.

Create tables from the library `Base.metadata` (`cqrs.outbox.sqlalchemy.Base` or `cqrs.outbox.sqlmodel.Base`, and the matching saga `Base`). Production databases are PostgreSQL, MySQL, and MariaDB. SQLite is for examples.

Anti-patterns:

- Commit the business session, then produce to Kafka or RabbitMQ in the same handler.
- Use a SQLAlchemy repository with a SQLModel `AsyncSession`, or the reverse.
- Share one outbox repository instance across concurrent requests. The repository holds the request session.
- Register the event type only in the publisher and not in the writer. Both sides resolve `event_name` through the map they were given.

## Transactional inbox

Use the inbox when a broker or an HTTP endpoint only accepts a command or an event, and a separate process must run the handler. The transport is at-least-once. `add` always takes a batch of `(message_id, message)` and returns `list[bool]` of the same length. `True` means the row was inserted. `False` means `(message_id, message_name)` is already stored, including earlier in the same batch. An empty batch returns `[]`. `add` does not commit. The caller opens a session, calls `add`, and commits.

FastAPI passes a batch of one element and returns 202. FastStream passes the Kafka batch from `batch=True`. Ack, or the HTTP response, happens after that commit. A `False` in the list is still ack or 202: the message is already in the inbox. An exception before the commit rolls the batch back, and the transport delivers it again.

`open_scope` and `bind_scope` are only when that same presentation transaction must also write another row.

`get_many` and `mediator.send` live in another process. Construct `InboxProcessor` with the repository for that batch session, the message map, and the mediators. `process` takes the row. It deserializes each row and sends a `Request` through `RequestMediator` or an event through `EventMediator`. The handler is the one already registered. It does not call `session.commit`. The worker opens one session for the batch, builds the repository and the handler's mediator from that session, and commits the business rows and the status together. A handler exception rolls back only that row's savepoint. `not_processed` increments `flush_counter`. After five failures the row leaves `get_many`.

Register every message name before the first read or write. Requests and domain events use the class name. Notification events use `event_name`, the same string the outbox stores. A second registration of the same name raises `KeyError`. Prefer an `InboxMessageMap()` instance when tests must not share the process-global map. JSON is the default codec.

```python
message_map = cqrs.InboxMessageMap()
message_map.register("ProcessDeposit", ProcessDeposit)

async with session_factory() as session:
    inbox = cqrs.SqlAlchemyInboxRepository(session, message_map=message_map)
    stored = await inbox.add([("msg-1", ProcessDeposit(account_id="acc-1", amount="10.00"))])
    await session.commit()
```

`SqlAlchemyInboxRepository` needs `python-cqrs[sqlalchemy]`. `SqlModelInboxRepository` needs `python-cqrs[sqlmodel]`. Both are lazy on `cqrs`. `get_many` locks rows with `FOR UPDATE SKIP LOCKED` on PostgreSQL and MySQL. Call `cqrs.rebind_inbox_model` when Alembic must see the inbox table on the application metadata.

Anti-patterns:

- A second method that adds one message. `add` already takes a batch of one.
- `ack` or the HTTP response before the session commits.
- `mediator.send` in the route or the subscriber that only accepts work.
- One session shared by presentation and the processor. Presentation commits the inbox row. The processor opens another session later.
- `enter_scope` around a plain `add`. Use `open_scope` only when the presentation transaction also writes another row.

## Saga

Use an orchestrated saga when a flow has several steps across processes or data stores, a later step can fail, and each finished step has a `compensate`. `SagaMediator` runs steps in order, persists progress, and compensates completed steps in reverse. Resume a crashed run with `cqrs.saga.recovery.recover_saga` and the same `scope_strategy`.

Skip a saga for a single `UPDATE` in one service. A database transaction is the tool. Skip a saga as a substitute for the outbox. A step that publishes still writes through the outbox. Skip a saga when a failed step cannot be compensated and the business can accept a manual repair. Skip `MemorySagaStorage` in a process that must recover after a restart. Memory storage is for tests and for a flow that may be lost. Production recovery uses `SqlAlchemySagaStorage` or `SqlModelSagaStorage`.

```python
class ReserveInventory(cqrs.SagaStepHandler[OrderContext, cqrs.Response]):
    async def act(self, context: OrderContext) -> cqrs.SagaStepResult:
        context.reservation_id = await self._inventory.reserve(context.order_id)
        return self._generate_step_result(cqrs.Response())

    async def compensate(self, context: OrderContext) -> None:
        if context.reservation_id:
            await self._inventory.release(context.reservation_id)

class OrderSaga(cqrs.Saga[OrderContext]):
    steps = [ReserveInventory]

def sagas_mapper(mapper: cqrs.SagaMap) -> None:
    mapper.bind(OrderContext, OrderSaga)
```

`cqrs.bootstrap.saga.bootstrap(..., sagas_mapper=sagas_mapper, saga_storage=storage)`. Execute with `async for step in mediator.execute(context)`. Pass `scope_strategy` through to recovery.

A fallback step (`cqrs.saga.fallback.Fallback`) is a degraded path when `act` fails, not a compensation. Compensation undoes a step that already succeeded. Use `cqrs.adapters.circuit_breaker.AioBreakerAdapter` (`python-cqrs[aiobreaker]`) on a step that calls a failing remote dependency and should stop trying. Skip a circuit breaker around a local database update.

## Chain of responsibility

`CORRequestHandler` runs handlers in order until one returns a result. A handler that does not apply returns `await self.next(request)`. Register the ordered list. That list is the one handler for the request type.

```python
class CreditCardHandler(cqrs.handlers.CORRequestHandler[ProcessPayment]):
    async def handle(self, request: ProcessPayment):
        if request.payment_method != "credit_card":
            return await self.next(request)
        return PaymentResult(success=True, transaction_id="cc")

def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(ProcessPayment, [CreditCardHandler, DefaultPaymentHandler])
```

Use a chain when one request has several alternative strategies (payment method, file type, tenant-specific policy) and exactly one should handle it. The last handler is the default.

Skip a chain when every step must run. That is one handler, or a command plus domain events. Skip a chain of one handler. Skip a chain for middleware concerns (logging, metrics). Those are `cqrs.middlewares.Middleware`. `build_chain` is for tests that assemble the chain without the mediator. Application startup uses `bind`.

HTTP CORS is not this feature. A browser calling FastAPI uses `fastapi.middleware.cors.CORSMiddleware` on the FastAPI app. python-cqrs does not configure it.

## Streaming

Use `StreamingRequestHandler` and `bootstrap_streaming` when one request yields many results: a batch, file processing, or progress the client should see before the work finishes. The caller iterates `async for item in mediator.stream(request)`. Events are emitted after each yield. Return a copy from `events` when `clear_events` drops the buffer.

Skip streaming for a single response. `RequestMediator.send` is the tool. Skip streaming to hide a slow domain event. A stream keeps the HTTP connection open. For FastAPI, return a `StreamingResponse` (SSE) from the route. The route still only iterates the mediator.

A streaming command and an ordinary command are different mediators. Build both at startup when the app has both shapes. One request type still has one handler, on one map.

## Validation

Default `Request`, `Response`, `DomainEvent`, and `NotificationEvent` are dataclasses. They check field presence at construction and do not check formats, ranges, or patterns. `PydanticRequest`, `PydanticResponse`, `PydanticDomainEvent`, and `PydanticNotificationEvent` need `python-cqrs[pydantic]` and are lazy: reading the name without the extra raises `ImportError`.

Validate at the trust boundary, once:

- An HTTP body or query string the client sends. Prefer `PydanticRequest` / `PydanticResponse` so FastAPI and the mediator share one model. Field constraints (`pattern`, `gt`, enums) belong on that model.
- A broker payload this service did not produce. Deserialize into `PydanticNotificationEvent[...]` (or FastStream's model) and reject the message before `EventMediator.send`.
- A protobuf contract. Validate by decoding. Add Pydantic only when the decoded message still has business-field rules the schema does not express.

Skip schema validation when:

- This process builds the object (a command constructed in a route from an already validated body, a domain event appended in a handler, a query built from a path id).
- A worker consumes a message this service produced and already stored in the outbox.
- The path is hot and the fields are plain ids and counts that a dataclass already types. Format checks would dominate the work.

Business rules stay in the handler even when the model was validated: the user exists, the balance covers the amount, the status transition is legal. Pydantic does not replace those checks. A handler does not construct a second Pydantic model to re-check a request FastAPI already parsed.

Use the same family for a request and its response. A pydantic command returns a pydantic response. An internal dataclass query returns a dataclass response. A custom `IRequest` is for msgspec, attrs, or a type the two builtins do not cover. Implement `to_dict` and `from_dict`.

## Dependency injection

Handlers receive ports through `__init__`. Bootstrap resolves them. Three containers are supported.

| Container | When | Skip when |
| --- | --- | --- |
| `di.Container` | Default. Small and medium apps. Pass it to `di_container=`. Bootstrap wraps it. | The app already standardized on dishka or dependency-injector |
| `cqrs.container.dishka.DishkaCQRSContainer` | The app already uses dishka. Extra: `python-cqrs[dishka]`. Register every handler with `provide(..., scope=Scope.REQUEST)` | You would add dishka only for CQRS. Use `di` |
| `cqrs.container.dependency_injector.DependencyInjectorCQRSContainer` | The app needs dependency-injector's config, resources, or nested containers. `attach_external_container`, then pass the adapter | You need a CQRS request scope. This adapter does not implement `SupportsScope` |

```python
from di import dependent

container = di.Container()
container.bind(di.bind_by_type(dependent.Dependent(SqlMeetingWriter, scope="request"), MeetingWriter))
mediator = bootstrap.bootstrap(
    di_container=container,
    commands_mapper=commands_mapper,
    queries_mapper=queries_mapper,
    domain_events_mapper=domain_events_mapper,
    scope_strategy=cqrs.ScopeStrategy.SEND,
)
```

`di`'s `scope="request"` does not open a CQRS scope. Without `scope_strategy`, a generator provider finishes before `handle`. Choose a strategy and pass the same value to bootstrap, the mediator, and `recover_saga`:

| Strategy | Session lifetime | Use |
| --- | --- | --- |
| `NONE` (default) | The application opens and closes sessions | Existing code that already wraps a unit of work inside `handle` |
| `SEND` | One scope for `send` / `stream`, including domain handlers and the fallback | Command, its events, and the outbox share one session. Events run sequentially |
| `HANDLER` | A new scope per handler, event handler, or saga step. Fallback runs in a fresh scope after the primary scope rolls back | Each handler needs its own session |

Anti-patterns:

- `scope="request"` and `ScopeStrategy.NONE` together, then expecting the session to live through domain handlers.
- `ScopeStrategy.SEND` with `concurrent_event_handle_enable=True`. Bootstrap raises `ValueError`.
- A dependency-injector container plus `scope_strategy=SEND`, expecting a shared session. Use `di` or dishka for that.
- Resolving a handler with `container.resolve` inside the route and calling `handle` yourself. Call `mediator.send`.
- Binding a new `di.Container()` inside every handler.

## FastAPI

The route authenticates, reads path and body, builds one request, and calls the mediator. Business writes, events, and the outbox stay in the handler.

`mediator.send` stays the path when the response needs the handler result. When the route can answer before the handler runs, accept the command or event into the transactional inbox (`InboxRepository.add` of a one-element batch, status 202) and process it in another process. The route does not call the mediator.

Build the mediator once at startup. A mediator is safe to reuse. The per-request object is the scope (session, unit of work), not a new mediator and not a new container.

```python
import functools
import di
import fastapi
import cqrs
from cqrs.bootstrap import requests as bootstrap

@functools.lru_cache(maxsize=1)
def mediator_factory() -> cqrs.RequestMediator:
    return bootstrap.bootstrap(
        di_container=app_container(),
        commands_mapper=commands_mapper,
        queries_mapper=queries_mapper,
        domain_events_mapper=domain_events_mapper,
        scope_strategy=cqrs.ScopeStrategy.SEND,
    )

@app.post("/orders", status_code=201)
async def create_order(
    command: CreateOrderCommand,
    mediator: cqrs.RequestMediator = fastapi.Depends(mediator_factory),
):
    return await mediator.send(command)
```

`app_container()` returns the process-wide `di.Container` (or a dishka / dependency-injector adapter), created once. For a path id, construct the request in the route (`UpdateOrder(order_id=order_id, **body)`) so the handler sees one immutable value.

Streaming routes depend on a separately cached `StreamingRequestMediator` from `bootstrap_streaming` and iterate `stream`. Saga routes depend on a `SagaMediator`. Do not send a saga context through `RequestMediator`.

Anti-patterns:

- SQL, broker calls, or domain branches in the route.
- `bootstrap.bootstrap` inside the endpoint, so every HTTP request rebuilds maps and the container.
- Caching an `AsyncSession` on the mediator. Cache the mediator. Open the session in the CQRS scope.
- A second Pydantic model in the route that copies every field into the command. Use the command as the body when it is a `PydanticRequest`.
- Returning a domain event or an outbox row as the HTTP response. Return the command's `Response`.

## FastStream

FastStream is the consumer (and, if you publish without the outbox, a place to lose messages). Incoming facts go to `EventMediator`. Incoming work that must run as a use case can be a command, after the subscriber has decoded and accepted the message.

`mediator.send` stays the path when this delivery must finish the handler before ack. When the subscriber only accepts work, open a session, pass the Kafka batch (`batch=True`) to `InboxRepository.add`, commit, and ack after that commit. A duplicate in the batch is still ack. The processor calls the mediator later. `open_scope` is only when that transaction also writes another row.

```python
import di
import faststream
from faststream import kafka
import cqrs
from cqrs.bootstrap import events as events_bootstrap

broker = kafka.KafkaBroker(bootstrap_servers=["localhost:9092"])

@functools.lru_cache(maxsize=1)
def event_mediator_factory() -> cqrs.EventMediator:
    return events_bootstrap.bootstrap(
        di_container=di.Container(),
        events_mapper=events_mapper,
        scope_strategy=cqrs.ScopeStrategy.HANDLER,
    )

@broker.subscriber("order_events", group_id="billing", auto_commit=False)
async def on_order_created(
    body: OrderCreated,
    msg: kafka.KafkaMessage,
    mediator: cqrs.EventMediator = faststream.Depends(event_mediator_factory),
):
    await mediator.send(body)
    await msg.ack()
```

Ack after `send` succeeds. `auto_commit=False`. On a deserialize failure, `nack` or park the message. Do not `ack` a payload the handler did not accept. Handlers for consumed events must be idempotent (at-least-once).

Produce through the outbox. A FastStream publisher used as `await broker.publish` next to a database commit is the dual-write the outbox exists to avoid. A publisher process may use the library `KafkaMessageBroker` (`python-cqrs[kafka]`) or `AMQPMessageBroker` (`python-cqrs[rabbit]`) with `EventProducer`.

Anti-patterns:

- `RequestMediator` for a broker fact that has many listeners. `EventMap` allows many handlers. `RequestMap` allows one.
- Acknowledging the message and then running the handler.
- Building a new mediator per message.
- Treating a consumed notification as a command that appends the same notification again. That loop republishes forever.

## Middleware, fallback, parallel events

Middleware (`middlewares=` on bootstrap) wraps every request: timing, tracing, a transaction boundary the handler does not own. Logging is already appended. Skip middleware for a rule that belongs to one use case. Implement `__call__(self, request, handle)` and return `await handle(request)`.

`RequestHandlerFallback` / `EventHandlerFallback` is a second handler when the primary one raises. Use it for a degraded response, not for compensation and not for a retry loop with no limit.

`concurrent_event_handle_enable=True` runs independent domain handlers with `max_concurrent_event_handlers`. Use it when those handlers do not share a session. Leave it off with `ScopeStrategy.SEND`.

## Extras

Install extras explicitly. They are not core dependencies.

`pydantic`, `sqlalchemy`, `sqlmodel`, `kafka`, `rabbit`, `protobuf`, `dishka`, `aiobreaker`. Names that need an extra are lazy on `cqrs`. Importing one without the extra raises `ImportError`.
