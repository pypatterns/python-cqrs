---
name: cqrs-event-driven
description: >-
  Wires a python-cqrs domain event, notification, transactional outbox,
  and transactional inbox. Use when a fact stays in this process, must
  reach another service, or must survive at-least-once delivery.
---

# Event-driven delivery

Read cqrs-core-setup before writing these types. This file builds the feature that skill already picked: a domain event, a notification, the outbox, or the inbox.

The package is already installed. Do not add an install step.

Put the types in `app/events.py`. Keep the handler in `app/handlers.py` and the registration in `app/bootstrap.py`.

## Steps

1. A fact inside this process is a `cqrs.DomainEvent`.
   A fact for another service is a `cqrs.NotificationEvent` with keyword-only `event_name`, `topic`, and `payload`.
   A dataclass payload is for a message this process builds.
2. For a domain event, append it on the handler `events` property.
   Register each `cqrs.EventHandler` with `domain_events_mapper` on `cqrs.bootstrap.requests.bootstrap`.
   `EventMap.bind` appends, so several handlers may listen.
   The mediator emits `events` after `handle` returns and before `send` returns.
   Keep those handlers idempotent when the command can be retried.
   With `ScopeStrategy.SEND` they run one after another on the command session.
3. For a notification that may be lost, append the `NotificationEvent` on `events` and pass a real `message_broker` to bootstrap.
   The default broker drops the message.
   Use this for a metric or a best-effort signal.
4. For a notification that must commit with the business row, leave `events` empty for that notification.
   In the handler, `session.add` the business row and `repository.add` the notification on the same session.
   `await repository.commit()` commits both.
   A separate process runs `cqrs.EventProducer`, then commits the repository so status updates stick.
   Register `event_name` on an `OutboxedEventMap` before the first read or write.
   A second registration of the same name raises `KeyError`.
   Prefer `cqrs.SqlAlchemyOutboxedEventRepository` with `python-cqrs[sqlalchemy]`, or `cqrs.outbox.sqlmodel.SqlModelOutboxedEventRepository` with `python-cqrs[sqlmodel]`.
   Use one storage extra for the process.
5. For an inbox, presentation calls `InboxRepository.add` with a batch of `(message_id, message)` and the caller commits.
   `add` returns `list[bool]`.
   `True` means inserted.
   `False` means `(message_id, message_name)` is already stored, including earlier in the same batch.
   An empty batch returns `[]`.
   Ack, or HTTP 202, happens after that commit.
   A `False` is still ack or 202.
   Another process constructs `InboxProcessor` with the repository for that batch session, the `InboxMessageMap`, and the mediators.
   `process` takes the row.
   It sends a `Request` through `RequestMediator` or an event through `EventMediator`.
   The handler is the one already registered.
   The processor does not commit.
   The worker commits the business rows and the status together.
   Requests and domain events register under the class name.
   Notification events register under `event_name`.
6. Events that arrive from a broker and must finish before ack use `cqrs.bootstrap.events.bootstrap` and `await mediator.send(event)` on `EventMediator`.
   That path is not the inbox.
   The inbox is the path that only stores the message and runs it later.

## Where the calls live

The command handler writes the business row and, when the fact must leave the process, the outbox row. It does not publish. It may append a `DomainEvent` for an in-process reaction. It does not append the same notification it already passed to `repository.add`.

`domain_events_mapper` belongs on `cqrs.bootstrap.requests.bootstrap`, next to `commands_mapper`. Broker facts that must run to completion before ack use a separate `EventMediator` from `cqrs.bootstrap.events.bootstrap`.

The publisher process owns `EventProducer`. After each batch it commits the outbox repository. Rows that fail until `flush_counter` reaches five leave the selectable batch. Inspect those rows. The producer will not yield them again. On PostgreSQL, MySQL 8.0 or later, and MariaDB 10.6 or later, `get_many` holds the batch with `FOR UPDATE SKIP LOCKED` until that commit or a rollback.

Presentation owns `add` and the commit. The worker owns `get_many`, `InboxProcessor.process`, and a later commit. Do not share one session between them. `open_scope` is only when the presentation transaction also writes another row. A plain `add` does not need it.

`SqlAlchemyInboxRepository` needs `python-cqrs[sqlalchemy]`. `SqlModelInboxRepository` needs `python-cqrs[sqlmodel]`. Both are lazy on `cqrs`. Register message names on an `InboxMessageMap` instance when tests must not share the process-global map. The same rule applies to `OutboxedEventMap`. JSON is the default codec.

A handler exception in the worker rolls back only that row's savepoint. `not_processed` increments `flush_counter`. After five failures the row leaves `get_many`.

## How the four differ

A domain event finishes before `send` returns. A notification on `events` is a best-effort publish. An outbox row commits with the business row and is published later. An inbox row commits before the handler runs, so a repeat delivery does not run the handler twice.

One command may do two of these. The short shape appends `OrderPlaced` for this process and stores `OrderCreated` in the outbox. The inbox function only accepts `PlaceOrder` from outside. It does not call the mediator.

## Do not

- Publish with `broker.send` next to `session.commit()`. A crash between those writes loses the message. Write the outbox row instead.
- Call `mediator.send` in the route or subscriber that only accepts work. Store the inbox row, commit, then ack.
- Use a domain event for email or HTTP to another service. The caller of `send` waits for every domain handler.
- Use the outbox to compensate a later step. That path is `cqrs-saga-orchestration`.
- Call `EventProducer` from the request handler. The handler only writes the row.
- Share one outbox repository across concurrent requests. The repository holds the request session.
- Register the event type only in the publisher. The writer and the publisher resolve `event_name` through the map they were given.
- Ack before the inbox session commits. An exception before that commit must leave the message for another delivery.

Delivery notes: [references/delivery.md](references/delivery.md).
Short shape: [assets/order_created.py](assets/order_created.py).
Full examples: `examples/events/domain_event_handler.py`, `examples/outbox/fastapi_outbox.py`.
