---
name: cqrs-saga
description: >-
  Orchestrates a python-cqrs saga whose finished steps compensate on failure,
  with persistent state and recovery. Use when a flow crosses services or
  databases and a completed step must be undone.
---

# Saga orchestration

Read cqrs-setup before writing steps. Use a saga when several steps cross services or databases and a finished step has a compensate. A single UPDATE in one service stays a database transaction. Publishing one message reliably is the outbox inside cqrs-eda, not a saga.

The package is already installed. Do not add an install step.

## Steps

1. Put a context object in `app/sagas.py`. It carries the ids later steps and compensations need.
2. One class per step, subclassing `cqrs.SagaStepHandler[Context, cqrs.Response]`. `act` does the work and returns `self._generate_step_result(...)`. `compensate` undoes that step.
3. Subclass `cqrs.Saga[Context]` and set `steps` to those classes, in forward order.
4. Register with `mapper.bind(Context, SagaClass)` inside `sagas_mapper`.
5. Build `cqrs.bootstrap.saga.bootstrap(..., sagas_mapper=sagas_mapper, saga_storage=storage)` once. Production storage is `SqlAlchemySagaStorage` or `SqlModelSagaStorage`. `MemorySagaStorage` does not survive a restart.
6. Run `async for step in mediator.execute(context)`. Pass the same `scope_strategy` the application uses for `recover_saga`.
7. After a crash, call `cqrs.saga.recovery.recover_saga` with that storage and the same `scope_strategy`. Do not start a second saga for the same business id.

## Do not

- Invent `OrchestratedSaga`, `SagaLog`, or a `Command` base.
- Compensate a step that has not finished `act`.
- Call the broker next to `session.commit()` inside `act`. A step that publishes writes the outbox.
- Open a fresh session inside `act` when `ScopeStrategy.SEND` already provides one.

Short shape: [assets/order_saga.py](assets/order_saga.py).
Recovery: [references/recovery.md](references/recovery.md).
Full example: `examples/saga/saga.py`. Recovery example: `examples/saga/saga_recovery.py`.

## Context

Subclass `cqrs.saga.models.SagaContext` and keep the subclass a dataclass. Storage round-trips it with `to_dict` and `from_dict`. `SagaContext` is not an attribute of `cqrs`. Put the business id and every id a later step or a compensation reads: reservation id, payment id, shipment id. Steps mutate that one instance. They do not return a new context.

`cqrs.Saga[Context]` checks `steps` when the subclass is created. Every step must use that same context type. Order is forward order. Compensation runs completed steps in reverse.

A step takes its ports in `__init__`. The container resolves the step. Do not construct the step inside `execute`.

## Run and resume

Pass a stable `saga_id` into `mediator.execute(context, saga_id=saga_id)` when the run must be resumed. The same id is the argument to `recover_saga`. One business id maps to one saga id. A second `execute` for that id starts a second history.

`recover_saga` takes an instance of the saga class, not the class. Pass the original storage, the original container, and the original `scope_strategy`. Completed steps are skipped. A saga left in compensation continues compensation. Do not call it for a saga that is still running in another worker.

Import it from `cqrs.saga.recovery`. It is not an attribute of `cqrs`.

`MemorySagaStorage` is `cqrs.saga.storage.memory.MemorySagaStorage`. It is for tests and for a flow that may be lost. Production recovery uses `cqrs.saga.storage.sqlalchemy.SqlAlchemySagaStorage` (`python-cqrs[sqlalchemy]`) or `cqrs.saga.storage.sqlmodel.SqlModelSagaStorage` (`python-cqrs[sqlmodel]`). Saga storage takes an `async_sessionmaker`, not the request session the outbox uses.

## Compensation

`compensate` undoes a step whose `act` already finished. It must be safe to run again with the same context. Read the ids `act` wrote. Do not start a new forward action inside `compensate`.

A step that has not finished `act` is not compensated. The failure itself is the stop. Earlier finished steps are compensated in reverse.

A fallback (`cqrs.saga.fallback.Fallback`) is a degraded path when `act` fails. It is not compensation. Compensation undoes a step that already succeeded.

A step that publishes does not call the broker beside `session.commit()`. It writes the outbox in `cqrs-eda`, in the session the scope already opened.

## How to test

Assert `act` and `compensate` on `invocations`. After a full successful run the saga is `completed` and `running_count` is 0. Compensation leaves the saga `failed` and the step `compensated`. A fallback is a different path: the failed step is not compensated. `harness.sagas.recover` resumes without entering a finished `act` again. Detail is `cqrs-testing`.

## Files and scope

`app/sagas.py` holds the context, the step classes, the saga class, and `sagas_mapper`. Do not send that context through `RequestMediator`.

Pass one `scope_strategy` into `cqrs.bootstrap.saga.bootstrap` and the same value into `recover_saga`. `ScopeStrategy.SEND` opens one scope for the run. Saga storage still commits each checkpoint on its own session before that scope closes. If the business session commits only when the scope closes, a crash after the checkpoint and before that commit rolls back the business row and the outbox row, and recovery skips the step it already stored. Keep the step's effect in the same transaction as the checkpoint, or reconcile a skipped step whose effect never committed. `ScopeStrategy.HANDLER` opens a scope per step. `ScopeStrategy.NONE` leaves the unit of work to the step. A step does not open its own session when `SEND` already provided one.

`execute` returns an async iterator. Do not await it. Exhaust the iterator or close it. An abandoned `SEND` iterator can keep its unit of work open until it is collected.

A step may expose `events`. `mediator.execute` emits them after `act` succeeds, before the next step. `recover_saga` runs the transaction directly and does not emit those events. A crash after a checkpoint and before that emission can also drop events from a normal run. A fact that downstream logic needs still goes through the outbox. Compensation reads ids on the context, not that list.

Storage classes are not attributes of `cqrs`. Import `MemorySagaStorage` from `cqrs.saga.storage.memory`, `SqlAlchemySagaStorage` from `cqrs.saga.storage.sqlalchemy`, and `SqlModelSagaStorage` from `cqrs.saga.storage.sqlmodel`. Create tables from the saga `Base.metadata` that matches the extra you installed. Production databases are PostgreSQL, MySQL, and MariaDB. SQLite is for examples.

`act` returns `self._generate_step_result(cqrs.Response())` when the step has no payload for the caller. The transaction stamps `saga_id` on that result. Raise from `act` when the step cannot finish. Do not catch that error and continue to the next step. The mediator compensates the steps that already finished.

Give `compensate` the same context fields `act` wrote. A missing id does not prove the remote call failed: a timeout can happen after the service reserved the resource and before `act` stored the id. The mediator does not compensate a step that has not finished `act`, so that reservation can stay. Give the remote call an idempotency key, and look up or reconcile an uncertain result before you treat the step as undone. Recovery can also repeat `act` when the process stops before the checkpoint. Calling release, refund, or cancel with an empty id creates a new failure during rollback.

Do not retry `act` from inside the step. The mediator records the failure and starts compensation. A circuit breaker belongs on a step that calls a failing remote dependency. It is not a substitute for `compensate`.
