---
name: cqrs-parallel-events
description: >-
  Runs python-cqrs domain handlers concurrently when they do not share a session.
  Use when handler order does not matter and the scope is HANDLER or NONE.
---

# Parallel domain events

Read cqrs-core-setup before turning concurrency on. Use it when domain handlers do not share a session and the order of those handlers does not matter. The event itself is still built in `cqrs-event-driven`.

The package is already installed. Do not add an install step.

The flag belongs on the bootstrap call in `app/bootstrap.py`. Handlers stay in `app/handlers.py`. Do not add a directory for parallel events.

## Steps

1. Keep the command and its `cqrs.DomainEvent` as `cqrs-event-driven` describes.
   Several `cqrs.EventHandler` classes may `bind` to one event.
   `EventMap.bind` appends.
2. Choose `cqrs.ScopeStrategy.HANDLER` or `cqrs.ScopeStrategy.NONE`.
   `HANDLER` opens a new scope per event handler.
   `NONE` leaves the session to the handler.
3. Pass `concurrent_event_handle_enable=True` and `max_concurrent_event_handlers` together.
   Put both on the same `cqrs.bootstrap.requests.bootstrap` call.
   The limit is how many handlers may run at once.
   Set it above 1 only when the handlers are independent.
4. The same pair is valid on `cqrs.bootstrap.requests.bootstrap_streaming` when a stream emits domain events.
   It is not valid when `scope_strategy` is `ScopeStrategy.SEND`.
5. Leave the flag off when handlers must see the command's session or must run in a fixed order.
   The default on `bootstrap` is `concurrent_event_handle_enable=False`.
   Handlers then run one after another.

`ScopeStrategy.SEND` with `concurrent_event_handle_enable=True` raises `ValueError`. Parallel handlers would share one unit of work. Use `HANDLER` or `NONE`, or leave the flag false.

## What runs together

The command handler still runs alone. Parallelism starts after `handle` returns, when the mediator emits `events`. Two handlers of one event, or handlers of several events from that command, may overlap up to `max_concurrent_event_handlers`.

They must not write one session. Under `HANDLER` each handler's scope is its own. Under `NONE` the handler opens and closes whatever it needs, and it must not borrow the command's session.

Skip this skill when `ScopeStrategy.SEND` is the application's strategy. `cqrs-core-setup` already says that. This file only turns the flag on for the strategies that allow it.

Pass the same `scope_strategy` the rest of this mediator already uses. Do not build a second mediator only to flip the flag. A stream that emits independent domain events sets the same pair on `bootstrap_streaming`.

`max_concurrent_event_handlers` is a cap, not a promise that every handler starts at once. A limit of 2 with four handlers runs them in overlapping pairs.

## Do not

- Enable the flag with `ScopeStrategy.SEND`.
- Set `max_concurrent_event_handlers` above 1 while the flag is false. The limit applies only when handlers actually run together.
- Let two handlers write one session. Each handler that runs in parallel needs its own session, or no session.
- Use parallel events as a saga. There is no compensation here. A flow that must undo a finished step is `cqrs-saga-orchestration`.
- Use parallel events to publish a notification reliably. That path is the outbox inside `cqrs-event-driven`.
- Depend on handler order. The mediator does not promise which independent handler finishes first.

Handlers should be idempotent when the command can be retried. A failure in one handler does not become compensation for the others.

Short shape: [assets/parallel_handlers.py](assets/parallel_handlers.py).
Full example: `examples/streaming/streaming_handler_parallel_events.py`.
