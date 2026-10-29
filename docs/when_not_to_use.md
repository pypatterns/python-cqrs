# Which pattern to use

The mediator, commands, queries, and event handlers fit any service, including a single service with one database. Each use case is its own handler, so a large codebase stays a set of small classes wired in one place.

The saga, the outbox, and the inbox are different. Each one solves a specific problem of distributed work. Without that problem, it is extra machinery.

## One service, one database

Use commands, queries, and domain events. The handler writes through its session and commits. A domain event reaches its handlers in the same process, after the command.

There is no saga here: a single `UPDATE` in one database is already atomic. There is no outbox either, while nothing is published to a broker.

## Use the outbox when you dual-write

If you persist business state **and** publish to a broker, do not commit the database and then independently produce to Kafka or RabbitMQ. A crash between those two writes loses the message (or duplicates it on retry).

Put the business row and the outbox row in the **same transaction**. A separate process then drains the outbox with at-least-once delivery. See [Transactional Outbox](https://mkdocs.python-cqrs.dev/latest/outbox/).

Use the outbox when:

- This service owns a database **and** a message broker
- Downstream consumers must eventually see the write
- A short delay between commit and publish is acceptable

Skip the outbox if nothing is published outside the database transaction.

## Use the inbox when a message must be handled once

A broker delivers at least once. If the handler must not run twice for the same message, store the message first and process it from the table. See [Transactional Inbox](https://mkdocs.python-cqrs.dev/latest/inbox/).

Skip the inbox when the handler is idempotent on its own.

## Use a saga when steps cross boundaries and must be compensated

A local SQL transaction cannot include another service's database. If a flow has several steps across processes or data stores, and a later step can fail, you need **compensation** — not a distributed two-phase commit.

See the [Saga pattern](https://mkdocs.python-cqrs.dev/latest/saga/) when:

- Steps cross process or database boundaries
- Each completed step can be undone (or otherwise compensated)
- The flow must recover after a crash mid-way

Do not introduce a saga for a single `UPDATE` in one service. Do not use a saga as a substitute for the outbox: the saga coordinates steps; the outbox makes a dual-write reliable.

## Rule of thumb

| Situation | Use |
|-----------|-----|
| One service, one database, no broker | Commands, queries, and domain events |
| Dual-write to DB + broker | [Transactional Outbox](https://mkdocs.python-cqrs.dev/latest/outbox/) |
| A redelivered message must not run twice | [Transactional Inbox](https://mkdocs.python-cqrs.dev/latest/inbox/) |
| Multi-step work + compensation across boundaries | [Orchestrated Saga](https://mkdocs.python-cqrs.dev/latest/saga/) |

Related:

- [Request handlers](https://mkdocs.python-cqrs.dev/latest/request_handler/) — commands and queries in one service
- [Transactional Outbox](https://mkdocs.python-cqrs.dev/latest/outbox/) — store events with the business write, publish later
- [Saga Pattern](https://mkdocs.python-cqrs.dev/latest/saga/) — orchestrate steps with automatic compensation and recovery
- [FastAPI + Outbox example](../examples/outbox/fastapi_outbox.py) — one command, one transaction, publisher stub
