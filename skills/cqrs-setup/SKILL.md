---
name: cqrs-setup
description: >-
  Sets up a service on python-cqrs and picks the feature.
  Use when writing a command, a query, a domain event, a notification,
  an outbox, an inbox, a saga, a chain, or a stream.
---

# python-cqrs

Pick the feature with the Use when and Skip when pairs in this file. Both a command and a query are `cqrs.Request`. Event steps are cqrs-eda. Saga steps are cqrs-saga.

The package is already installed. Do not add an install step. Do not copy `examples/` into the application. Open the linked file when the short shape is not enough.

Say which feature you picked and which neighbor you skipped, in one sentence, before writing the handler.

## Files

Keep one layout. Do not add a directory per pattern.

- `app/commands.py` holds write models: `cqrs.Request[Result]` or `cqrs.Request[None]`.
- `app/queries.py` holds read models: `cqrs.Request[Result]`.
- `app/handlers.py` holds `cqrs.RequestHandler` classes. Chains, streams, and event lists live here too.
- `app/bootstrap.py` holds `commands_mapper`, `queries_mapper`, and one call to `cqrs.bootstrap.requests.bootstrap`.
- `app/sagas.py` exists only when the saga pair is the feature you picked. Step detail is `cqrs-saga`.
- `app/events.py` exists only when you picked a domain event, a notification, the outbox, or the inbox. Detail is `cqrs-eda`.

Outbox rows, streams, chains, and events stay in `handlers.py` and `bootstrap.py`.

## Choose a feature

Read the pair that matches the change. Name the feature you will use and the neighbor that does not fit.

### Command

Use when the handler changes state this service owns: create, update, delete, or a state transition.

Skip when the operation is one local SQL statement with no handler dependencies, no events, and no second caller. A function on the repository is enough.

A command with nothing to return is `cqrs.Request[None]`. A command that must hand back an id or a status is `cqrs.Request[Result]`. Record a follow-up fact on `events` or in the outbox. Do not invent a `Command` base class.

Neighbor: a read is a query. A flow that must undo a finished step in another service is a saga, not a bigger command.

### Query

Use when the handler only reads and returns that read.

Skip when the same call writes, publishes, or stores an event. That call is a command.

The type is `cqrs.Request[Result]`. Leave `events` empty. Do not insert, update, delete, or publish from a query handler. There is no `Query` base class. The same mediator sends the query.

Neighbor: if the caller needs the write to happen, do not hide it inside the query.

### Domain event

Use when a reaction stays in this process and in the same unit of work, and the command should stay short: a read-model update, an audit row, a follow-up that must finish before the caller continues.

Skip when the next step is the rest of the same function, when nothing else listens, or when the work must reach another service. Skip it for slow I/O (email, HTTP to another service). The caller of `send` waits for every domain handler.

Append a `cqrs.DomainEvent` on the handler `events` property. Register each `cqrs.EventHandler` on `domain_events_mapper`. `EventMap.bind` appends, so one event may have many handlers. With `ScopeStrategy.SEND`, those handlers run one after another on the command's session.

Neighbor: another service, or a message that must not be lost, is a notification plus the outbox, not a domain event. Steps are `cqrs-eda`.

### Notification event

Use when another service receives a fact and losing the message is acceptable: a metric, a best-effort signal, a dev broker.

Skip when nothing outside this process listens. A domain event is the in-process tool. Skip when the message must not be lost between `commit` and `broker.send`.

A `cqrs.NotificationEvent` sets keyword-only `event_name`, `topic`, and `payload`. Put it on `events` only for the lossy case, and pass a real `message_broker` into bootstrap. A missing broker becomes a broker that discards messages.

Neighbor: a durable publish is the outbox. An in-process reaction is a domain event. Steps are `cqrs-eda`.

### Transactional outbox

Use when a business row and a `NotificationEvent` commit in one session, and a separate process publishes.

Skip when nothing leaves the database transaction. Skip the outbox as a stand-in for a saga: the outbox stores one message reliably and does not compensate the next step. Skip calling the publisher from the request handler. The handler only writes the row.

`repository.add(event)` and the business `session.add(...)` share one session. `await repository.commit()` commits both. `cqrs.EventProducer` runs in another process. Register every `event_name` before the first read or write. A second registration of the same name raises `KeyError`. Leave `events` empty for that notification so it is not published twice.

Neighbor: a lost message is acceptable only for a notification on `events`. Compensation of a later step is a saga. Steps are `cqrs-eda`.

### Transactional inbox

Use when a broker is at-least-once and the handler must start after a short commit of the accepted message. The route or subscriber only accepts work.

Skip when the message is already inside the process, or when the handler can be called directly. Then call `await mediator.send(request)`.

`InboxRepository.add` takes a batch of `(message_id, message)` and returns `list[bool]` of the same length. `True` means the row was inserted. `False` means `(message_id, message_name)` is already stored. `add` does not commit. The caller commits. Ack, or HTTP 202, happens after that commit. A later process runs `InboxProcessor`. `process` takes the row and calls the mediator that is already registered.

Neighbor: finishing the handler before ack is `mediator.send` in that delivery, not an inbox row. Steps are `cqrs-eda`.

### Saga

Use when several steps in different services or databases must either finish or roll back by compensation. Each finished step has a `compensate`.

Skip when one `UPDATE` in one service is enough. A database transaction is the tool. Skip a saga when a failed step cannot be compensated and a manual repair is acceptable. Skip a saga as a substitute for the outbox. A step that publishes still writes the outbox.

`SagaMediator` runs steps in order, stores progress, and compensates completed steps in reverse. Resume a crashed run with `cqrs.saga.recovery.recover_saga` and the same `scope_strategy`. Do not send a saga context through `RequestMediator`.

Neighbor: one service and one transaction is a command. One reliable message is the outbox. Steps are `cqrs-saga`.

### Chain of responsibility

Use when one request has several independent decisions and exactly one of them should handle it: payment method, file type, tenant policy. The request passes each handler until one returns a result.

Skip when the decision is single and the handler itself makes it. Skip a chain when every step must run. That is one handler, or a command plus domain events. Skip a chain of one handler. Skip a chain for logging or metrics. Those are middleware.

Register the ordered list with one `mapper.bind(Request, [First, Second])`. That list is the one handler for the request type. A handler that does not apply returns `await self.next(request)`. The last handler is the default. HTTP CORS is not this feature.

Neighbor: "run every step" is not a chain. Steps are `cqrs-cor`.

### Streaming

Use when the caller reads progress before the final answer: a batch, file processing, or a stream of results.

Skip when one `send` returns the whole answer. Skip streaming to hide a slow domain event. A stream keeps the connection open.

The handler is `cqrs.StreamingRequestHandler`. Build `cqrs.bootstrap.requests.bootstrap_streaming` once. The caller iterates `async for item in mediator.stream(request)`. A streaming command and an ordinary command are different mediators. One request type still has one handler, on one map.

Neighbor: a single response is `RequestMediator.send`. Steps are `cqrs-streaming`.

### Parallel events

Use when domain handlers do not share a session and the order of those handlers does not matter.

Skip when `ScopeStrategy.SEND` is on, or when the handlers write one session.

Neighbor: handlers that must see the command's session stay sequential. Steps are `cqrs-eda`.

### Validation

Use Pydantic on the boundary of untrusted input: an HTTP body, a query string, or a broker payload this process did not produce. `PydanticRequest` and `PydanticNotificationEvent` need the `pydantic` extra.

Skip Pydantic for a message this process builds: a command constructed from an already validated body, a domain event appended in a handler, a query built from a path id, a row this service already stored in the outbox. That object is a dataclass `Request` or `DomainEvent`.

Business rules stay in the handler either way: the user exists, the balance covers the amount, the status transition is legal. Use the same family for a request and its response.

Neighbor: a second model that re-checks a body the framework already parsed does not belong in the handler.

## Steps

1. Put the write model in `app/commands.py` as `cqrs.Request[Result]` or `cqrs.Request[None]`.
2. Put the read model in `app/queries.py` as `cqrs.Request[Result]`.
3. Add `cqrs.RequestHandler[ThatRequest]` in `app/handlers.py`. Do not annotate `handle` with `-> Result`.
4. `mapper.bind(RequestType, Handler)` in `app/bootstrap.py`. One handler per request type. A second bind raises `KeyError`.
5. Build `cqrs.bootstrap.requests.bootstrap` once at startup. Call `await mediator.send(request)`.

`commands_mapper` and `queries_mapper` both fill the same `RequestMap`. Split them so reads and writes stay obvious. The library does not enforce the split. Binding the same request type from both mappers raises `KeyError`. There is no `register_handler` decorator.

Take ports in `__init__`. The container constructs the handler. The route builds one request and calls the mediator. It does not construct the handler and it does not call `handle`.

`await mediator.send(query)` for `ReadMeetingQuery(cqrs.Request[ReadMeetingQueryResult])` is `ReadMeetingQueryResult`. The return type lives on the request, not on a `->` annotation.

Already written `Request` and `RequestHandler[Request, Result]` stay as they are. Do not rewrite them. `send` on an unparameterized `Request` is `Any`.

Startup is not the package `cqrs.bootstrap`. Import the submodule:

- `cqrs.bootstrap.requests.bootstrap` builds `RequestMediator` for commands and queries.
- `cqrs.bootstrap.requests.bootstrap_streaming` builds `StreamingRequestMediator`. See `cqrs-streaming`.
- `cqrs.bootstrap.events.bootstrap` builds `EventMediator` for events that arrive from outside. See `cqrs-eda`.
- `cqrs.bootstrap.saga.bootstrap` builds `SagaMediator`. See `cqrs-saga`.

Build the mediator once. Reuse it. A missing `message_broker` discards notifications. Pass a real broker only for a lossy notification. A notification that must survive `commit` uses the outbox inside `cqrs-eda`.

Logging middleware is added when the middleware list does not already contain it. Do not add a middleware class for a rule that belongs to one use case.

## First command and query

For a meeting service the write model is `JoinMeetingCommand(cqrs.Request[None])` in `app/commands.py`. The read model is `ReadMeetingQuery(cqrs.Request[ReadMeetingQueryResult])` in `app/queries.py`. `ReadMeetingQueryResult` subclasses `cqrs.Response`.

Handlers live in `app/handlers.py`. `JoinMeetingCommandHandler` takes a writer port and calls `join`. `ReadMeetingQueryHandler` takes a reader port and returns `ReadMeetingQueryResult`. Neither annotates `handle` with a return type. The query handler does not append events.

`app/bootstrap.py` binds each request once and calls `cqrs.bootstrap.requests.bootstrap` with `commands_mapper`, `queries_mapper`, and the process container. The caller then `await mediator.send(...)`.

A chain is still one registration: `mapper.bind` receives the ordered list. A second `bind` of the same request type raises `KeyError`, including when `commands_mapper` and `queries_mapper` both bind it.

Skip the command and the query when the only caller is one repository method, with no second entry point and no event. Add the request when two callers must share the use case, or when the use case grows an event, an outbox row, or an injected port.

## What the caller sees

`send` returns the request's parameter. A `Request[None]` command returns `None`. A query returns the response instance. Do not annotate `handle` to restate that type.

The route, the worker, or the test is the caller. A handler that needs another use case calls the port it was given. It does not call `mediator.send`. A nested `send` hides a second dispatch, and a second set of events, inside the first one.

Build both a `RequestMediator` and a `StreamingRequestMediator` at startup when the service has both shapes. Build a `SagaMediator` only when a saga was the feature you picked. Do not overload one mediator with the other kinds of work.

## Do not

- Add a `Command` or `Query` base class, or a `CommandBus`.
- Call the mediator from inside `handle`.
- Publish to a broker next to `session.commit()`. That path is the outbox inside `cqrs-eda`.
- Roll back several services. That path is `cqrs-saga`.
- Open a new mediator inside a route, a subscriber, or a handler.
- Copy a full example into the application. The files under `examples/` stay runnable demos.

Full example: `examples/events/request_handler.py`.
Short shape: [assets/join_meeting.py](assets/join_meeting.py).
