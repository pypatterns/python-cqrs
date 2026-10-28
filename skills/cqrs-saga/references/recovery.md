# Saga recovery

A saga id is the handle for one run. Pass the same `uuid.UUID` to `mediator.execute(context, saga_id=saga_id)` and later to `cqrs.saga.recovery.recover_saga`. The storage checkpoints context and step history under that id. Without it, a crash has nothing to resume, and a new `execute` is a different run.

Use one saga id per business id. Do not start a second saga for an order that already has a run. Recovery continues the stored history. It does not invent a parallel one.

## What recovery does

`recover_saga` loads the last status and context, rebuilds the context, and resumes.

- Forward steps that already finished are skipped.
- A run that stopped mid-flight continues at the next step.
- A run that stopped while compensating continues compensation in reverse.
- A run that already reached `COMPLETED` is left alone.
- `FAILED` is not that stop. A failed compensation also leaves `FAILED`, and `recover_saga` still tries the compensation that is outstanding. `get_sagas_for_recovery` returns `RUNNING` and `COMPENSATING` only, so a `FAILED` saga is not in that scan. Find those rows another way and call `recover_saga`, or the unfinished compensation stays undone.

Call it with an instance of the original saga class, not the class itself. Pass the same storage, the same container, and the same `scope_strategy` as the original `execute`. Import the function from `cqrs.saga.recovery`. It is not an attribute of `cqrs`.

`context_builder` rebuilds the context from the stored dict. A dataclass subclass of `cqrs.saga.models.SagaContext` can be passed as the class when its fields match that dict, or as `Context.from_dict`.

Do not call `recover_saga` for a saga that another worker is still running.

On a failed resume after the context is rebuilt, the storage increments recovery attempts. A failure while loading state or rebuilding the context does not increment that counter, so a `RUNNING` saga can stay eligible for the next scan. Callers do not increment the counter themselves. `get_sagas_for_recovery(max_recovery_attempts=...)` can exclude a run that has failed recovery too many times.

## Memory storage and SQL storage

`cqrs.saga.storage.memory.MemorySagaStorage` keeps the checkpoint in process. A restart drops it. Use it in tests and for a flow the business can lose. Do not use it when `recover_saga` must run after a deploy or a crash.

`cqrs.saga.storage.sqlalchemy.SqlAlchemySagaStorage` needs `python-cqrs[sqlalchemy]`. `cqrs.saga.storage.sqlmodel.SqlModelSagaStorage` needs `python-cqrs[sqlmodel]`. Both take an `async_sessionmaker`, not one request session. SQLModel saga storage needs `async_sessionmaker(..., class_=sqlmodel.ext.asyncio.session.AsyncSession)`. Those classes are not attributes of `cqrs`.

Use the same storage extra the process uses for the outbox. The outbox repository still takes the business session. Saga storage does not.

Full demo: `examples/saga/saga_recovery.py`.
