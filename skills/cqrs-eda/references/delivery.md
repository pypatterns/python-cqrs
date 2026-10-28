# Domain event, notification, outbox, inbox

Four names, four jobs. Pick one in `cqrs-setup`. This note is how the chosen one is delivered.

## Domain event

A `cqrs.DomainEvent` is a fact other handlers in this process react to. The command handler appends it to `events`. `cqrs.bootstrap.requests.bootstrap` takes `domain_events_mapper`. The mediator runs those handlers after `handle` returns, before `send` returns. Several `EventHandler` classes may bind to one event.

The caller waits. Do not put slow I/O here. With `ScopeStrategy.SEND`, handlers share the command session and run one after another. Independent handlers set `concurrent_event_handle_enable=True` only with `ScopeStrategy.HANDLER` or `ScopeStrategy.NONE`.

## Notification event

A `cqrs.NotificationEvent` is what another service receives. Fields `event_name`, `payload`, and `topic` are keyword-only. On the handler `events` list it is best-effort: bootstrap must receive a real `message_broker`, and a crash after `commit` can still drop it. The default broker discards the message.

Skip it when no other service consumes the fact. That reaction is a domain event.

## Transactional outbox

The business row and the `NotificationEvent` commit in one session. `OutboxedEventRepository.add` stages the notification. `commit` writes both. `EventProducer` in a separate process reads unpublished rows and sends them. Delivery is at-least-once. Consumers must be idempotent.

Leave `events` empty for that notification. A second copy on `events` can publish twice.

Register every `event_name` on the `OutboxedEventMap` the repository uses before the first read or write. JSON is the default codec. Rows that fail until `flush_counter` reaches five leave the selectable batch. Inspect those rows. The producer will not yield them again.

On PostgreSQL, MySQL 8.0 or later, and MariaDB 10.6 or later, `get_many` holds the batch with `FOR UPDATE SKIP LOCKED` until the caller commits or rolls back.

The outbox does not compensate a later step in another service.

## Transactional inbox

The transport is at-least-once. Presentation stores the message and commits. The handler runs in another process.

`add` always takes a batch. `True` inserted the row. `False` means the pair `(message_id, message_name)` is already stored. `add` does not commit. Ack or HTTP 202 happens after the commit. A duplicate is still ack or 202.

`InboxProcessor.process` takes one row, deserializes it, and calls the mediator already registered for that request or event. The worker opens the processor session, and commits the business rows and the inbox status together. A request-handler exception rolls back only that row's savepoint. `not_processed` increments `flush_counter`. After five failures the row leaves `get_many`. A domain-handler exception is logged and does not fail `send`, so this path must not be the retry signal for that reaction. A crash after the handler runs and before the status commit can run the handler again.

`open_scope` is only when that same presentation transaction also writes another row. A plain `add` does not need it.

Full demos: `examples/events/domain_event_handler.py`, `examples/outbox/fastapi_outbox.py`.
