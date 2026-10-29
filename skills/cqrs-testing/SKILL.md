---
name: cqrs-testing
description: >-
  Asserts a python-cqrs handler through cqrs.testing.test_harness.
  Use when a test must check a command, a query, a domain event, a notification,
  an outbox, an inbox, a saga, a chain, a stream, a fallback, or middleware.
---

# Testing python-cqrs

Read the feature skill before writing the handler. This file only says which journal to assert.

Import `from cqrs.testing import test_harness`. It is not in `cqrs.__all__`. The package is already installed. Do not add an install step. Do not copy `examples/testing/` into the application.

`async with test_harness(...) as harness` builds the mediators. Arguments match bootstrap. The builder passes `compensation_retry_delay=0` unless the test passes a delay. `async with test_harness.around(...) as harness` wraps mediators the application already built and does not change that delay. `around` puts resolve, the broker, saga storage, and the listener back in `finally`.

If the application mediator has a real broker and no `broker=` argument is passed, `around` forwards each message to that real broker and records it. To keep a unit test isolated, pass a `CapturingMessageBroker` with no inner broker: `async with test_harness.around(mediator, broker=CapturingMessageBroker()) as harness`. The real broker's `send_message` is then redirected to the capture, so no message leaves the process.

`test_harness(...)` creates a memory outbox and a memory inbox on `harness.outbox_repository` and `harness.inbox_repository`. It does not bind them. The test binds the port.

For `ScopeStrategy.SEND`, pass `di.Container`. Do not pass `TestContainer`. `TestContainer` is `register(type, instance)` and `async resolve`.

Do not read `DevnullMessageBroker.MESSAGE_BUS`. If the mediator holds that broker, the harness replaces it with `CapturingMessageBroker` for the block and restores it afterwards.

An invocation record has `handler`, `method` (`handle`, `act`, or `compensate`), `argument`, `argument_type`, and `handler_instance`.

`contains` is a boolean and does not raise. `assert_contains`, `assert_one`, and `assert_empty` raise `AssertionError` with the recorded rows. Those methods are not on every journal:

- `requests`, `handled`, `unhandled`, and `published` have `contains`.
- `handled` and `unhandled` have `assert_empty`. They do not have `assert_contains` or `assert_one`.
- `published` has `contains`, `assert_contains`, `assert_one`, and `assert_empty`.
- `outbox` has `assert_empty`. It does not have `contains`, `assert_contains`, or `assert_one`.
- `invocations` has `assert_entered` and `assert_not_entered`. It does not have `contains`, `assert_contains`, `assert_one`, or `assert_empty`.
- `requests` does not have `assert_contains`, `assert_one`, or `assert_empty`.

There is no pytest plugin.

Short shape for a command, a domain event, and a notification: [assets/test_order.py](assets/test_order.py).

## Command

Assert `requests` and `invocations`. `send` returns the same result `requests` stored. `published` stays empty unless the command also emits a notification.

## Query

Assert `requests`. `handled`, `published`, and `outbox` stay empty.

```python
view = await harness.mediator.send(ReadOrder(order_id="1"))
harness.handled.assert_empty()
harness.published.assert_empty()
harness.outbox.assert_empty()
```

## Domain event

Assert `handled` and `invocations`. A follow-up event is also on `handled` and enters the next handler. `published` stays empty.

## Notification

Assert `published` (`topic`, `message_name`, `payload`, `message_id`). The notice is not on `handled`.

`published.contains(OrderCreated)` is false for a domain event. `published.assert_contains` names `handled` or `invocations` in the error when the event was recorded there and was not sent to the broker.

## Parallel domain handlers

`concurrent_event_handle_enable=True` with `ScopeStrategy.HANDLER` or `NONE`. Both classes are on `invocations`. Do not assert order.

## Swallowed domain-handler error

`send` returns the command result. The error is on `handler_errors`. `requests` still holds that result.

```python
result = await harness.mediator.send(CreateOrder(sku="book"))
assert harness.handler_errors.of(OrderCreated, AuditHandler)
```

## Event with no handler

The type is on `unhandled`. `send` does not raise. The missing handler is not on `invocations`.

## EventMediator

`await harness.event_mediator.send(event)` enters `handle` on `invocations`. `EventMediator` goes through `EventDispatcher`, not `EventEmitter`, so this path is not `handled`. A follow-up also goes to the dispatcher.

## Request fallback

Primary raises. Both classes are on `invocations`. The response on `requests` is the fallback's.

## Event fallback

Same for an event from a command and for `EventMediator`. Both classes are on `invocations`.

## Circuit breaker

Use a fake `ICircuitBreaker`. Do not install `aiobreaker`. An open breaker leaves the primary off `invocations` and enters the fallback.

## Middleware

`middlewares` records the type, the request, and whether `handle` was called. If middleware does not call `handle`, `invocations` does not contain the handler, `called_next` is false, and `requests` holds the middleware's response.

```python
harness.invocations.assert_not_entered(CreateOrderHandler)
assert harness.middlewares.of(DenyMiddleware)[0].called_next is False
```

## ScopeStrategy.SEND

Domain handlers run one after another. Assert the order of `invocations` for that event.

## ScopeStrategy.HANDLER and NONE

Parallel mode is allowed. Both handlers are on `invocations`. Do not assert order.

## Chain

Assert `invocations`. The step whose `handle` ran is entered. The step it did not call is absent. There is no separate chain bus. The dispatcher reads `events` from the chain head, so an event added by a later step is not on `handled` unless the head exposes it. Expect `handled` only when the head is the step that returned the result and that event is on the head's `events`.

```python
harness.invocations.assert_entered(CardHandler)
harness.invocations.assert_not_entered(CashHandler)
```

## Stream

`yields` is the values in order. A domain event emitted after a yield is on `handled` between those values. `clear_events` does not erase the harness record.

```python
assert items == harness.yields
assert harness.handled.contains(RowExported)
```

## Streaming fallback

Primary fails in the middle. Fallback starts the stream again. `yields` and `invocations` show both handlers. Values already yielded stay.

## Saga step

`act` is on `invocations`. Inside the loop, after a step that is not the last, `sagas.running_count == 1`. After the full `async for`, the saga is `completed` and `running_count == 0`.

## Compensation

The failed step calls `compensate` on finished steps, in reverse. The saga status is `failed`. The step status is `compensated`. There is no saga status `compensated`.

```python
assert harness.invocations.of(ReserveStock)[-1].method == "compensate"
assert saga.status is SagaStatus.FAILED
assert saga.step_status("ReserveStock") is SagaStepStatus.COMPENSATED
```

`SagaStatus` and `SagaStepStatus` are `cqrs.saga.storage`, not attributes of `cqrs`.

## Saga fallback

`cqrs.saga.fallback.Fallback` runs instead of the failed `act`. The failed step is not compensated. Its last method on `invocations` is `act`. The spare step is entered. The saga is `completed`.

## Saga circuit breaker

Use a fake `ISagaStepCircuitBreaker`. An open breaker leaves the step off `invocations` and enters the fallback.

## Saga recovery

`await harness.sagas.recover(...)` calls `cqrs.saga.recovery.recover_saga` with the same storage and `scope_strategy`. A finished step does not enter `act` again. `COMPENSATING` continues compensation.

## Inbox

`process_available` sends a `Request` through `RequestMediator` or an event through `EventMediator`. The command is on `requests`. The event is on `invocations`. A second `add` of the same `(message_id, message_name)` returns `[False]`. After success the status is `processed`. The caller commits.

## Inbox handler error

Status stays `not_processed`. `flush_counter` grows. The caller commits.

## Outbox

The handler calls `repository.add`. The row is on `outbox`. It is absent from `published` until the test calls `produce_available`. That call does `get_many` and `EventProducer.send_message` once, then `commit`. Status becomes `produced` and the message is on `published`.

```python
assert harness.outbox.pending_count == 1
harness.published.assert_empty()
await harness.outbox.produce_available()
harness.published.assert_one(OrderCreatedNotice, topic="order_events")
```

## Outbox broker error

`harness.broker.fail_next(...)` leaves the row `not_produced`. `published` stays empty.

## Dataclass request

This is the default path. The response is `requests.of(...)[0].response`. The harness adds no validator.

## Pydantic

Marker `pydantic`. A field error is raised by the constructor, before `send`. `send` then returns the response.

## JSON

Default codec for `published`. `content_type` is `None`.

## Protobuf

Marker `protobuf`. The message has `content_type` and `payload_bytes`. The harness does not import protobuf.

## di.Container

`ScopeStrategy.SEND` shares the command session with the domain handler. Read `handler_instance` on `invocations`. Pass `di.Container`, not `TestContainer`.

## FastAPI

The harness does not import FastAPI. `TestClient` hits the route. `dependency_overrides` installs the mediator. `test_harness.around` wraps it. Assert `requests`, `handled`, `published`, or `outbox`.

## FastStream

The harness does not import FastStream. `TestKafkaBroker` delivers. Assert `invocations` or `inbox`. A subscriber that only calls `inbox.add` leaves the handler off `invocations`.

## SQLAlchemy

Marker `sqlalchemy`. The harness does not import SQLAlchemy. `around` wraps `SqlAlchemyOutboxedEventRepository`. sqlite through `aiosqlite`. No Docker. Roll the connection back. `produce_available` runs against that repository.

## SQLModel

Marker `sqlmodel`. Same shape with `cqrs.outbox.sqlmodel.SqlModelOutboxedEventRepository`.

## dishka

`around` wraps a mediator that is already built. Assert `invocations`.

## dependency-injector

`around` wraps a mediator that is already built. Assert `requests`. The adapter does not implement `SupportsScope`, so `scope_strategy` is not `SEND`.

## Kafka and AMQP

A unit test uses `CapturingMessageBroker` and reads `published`. The real adapter stays in the integration test. Do not read `DevnullMessageBroker.MESSAGE_BUS`.

## Mermaid

Not `test_harness`. `CoRMermaid` and `SagaMermaid` draw a diagram. They do not take part in `send`.

## Compressor

Not `test_harness`. `ZlibCompressor` is checked by the serializer tests. The harness sees `payload_bytes` as bytes that are already finished.

## Alembic

Not `test_harness`. `rebind_outbox_model` and `rebind_inbox_model` attach a table. They do not run a handler.

## Transaction log tailing

Not `test_harness` and not `mediator.send`. It is a separate process that reads the database log. The polling path is the outbox section above.
