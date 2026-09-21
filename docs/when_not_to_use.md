# When NOT to use python-cqrs

This library is for command/query separation, reliable broker publishing, and multi-step compensation across boundaries. It is extra machinery when a single database transaction is enough.

## Prefer a local database transaction

Use an ordinary DB transaction (or a thin service layer) when the use case is **single-service CRUD**: one process, one database, nothing else that must observe the same write.

- The row you update is the source of truth
- There is no message broker and no second service to notify
- There is no compensating action if a later step fails

A mediator, outbox table, and publisher loop would not make that code safer — only harder to follow.

## Use the Transactional Outbox when you dual-write

If you persist business state **and** publish to a broker, do not commit the database and then independently produce to Kafka or RabbitMQ. A crash between those two writes loses the message (or duplicates it on retry).

Put the business row and the outbox row in the **same transaction**. A separate process then drains the outbox with at-least-once delivery. See [Transactional Outbox](https://mkdocs.python-cqrs.dev/outbox/).

Use Outbox when:

- This service owns a database **and** a message broker
- Downstream consumers must eventually see the write
- A short delay between commit and publish is acceptable

Skip Outbox if nothing is published outside the database transaction.

## Use a Saga when steps cross boundaries and must be compensated

A local SQL transaction cannot include another service's database. If a flow has several steps across processes or data stores, and a later step can fail, you need **compensation** — not a distributed two-phase commit.

See the [Saga pattern](https://mkdocs.python-cqrs.dev/saga/) when:

- Steps cross process or database boundaries
- Each completed step can be undone (or otherwise compensated)
- The flow must recover after a crash mid-way

Do not introduce a Saga for a single `UPDATE` in one service. Do not use a Saga as a substitute for Outbox: Saga coordinates steps; Outbox makes a dual-write reliable.

## Rule of thumb

| Situation | Use |
|-----------|-----|
| Single-service CRUD, no broker | Local database transaction |
| Dual-write to DB + broker | [Transactional Outbox](https://mkdocs.python-cqrs.dev/outbox/) |
| Multi-step work + compensation across boundaries | [Orchestrated Saga](https://mkdocs.python-cqrs.dev/saga/) |

Related:

- [Transactional Outbox](https://mkdocs.python-cqrs.dev/outbox/) — store events with the business write, publish later
- [Saga Pattern](https://mkdocs.python-cqrs.dev/saga/) — orchestrate steps with automatic compensation and recovery
- [FastAPI + Outbox example](../examples/outbox/fastapi_outbox.py) — one command, one transaction, publisher stub
