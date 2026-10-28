---
name: cqrs-cor
description: >-
  Routes one python-cqrs request through handlers until one returns.
  Use when several independent decisions exist and exactly one should handle it.
---

# Chain of responsibility

Read cqrs-setup before writing a chain. Use a chain when one request has several alternative strategies and exactly one of them should handle it. The first handler that applies returns the result.

The package is already installed. Do not add an install step.

Put the request in `app/commands.py` and the handlers in `app/handlers.py`. The ordered bind stays in `app/bootstrap.py`. Do not add a directory for the chain.

## Steps

1. Declare one `cqrs.Request[Result]`.
   Every handler in the chain sees that same request and returns that same result type.
2. Each step subclasses `cqrs.handlers.CORRequestHandler[ThatRequest]`.
   Do not annotate `handle` with `-> Result`.
3. A handler that does not apply returns `await self.next(request)`.
   A handler that applies returns the result and does not call `next`.
4. The last handler is the default.
   It returns a result and does not call `next`.
5. Register once: `mapper.bind(RequestType, [First, Second, Default])`.
   That list is the one handler for the request type.
   A second `bind` of the same request raises `KeyError`.
6. Build the ordinary `cqrs.bootstrap.requests.bootstrap` once.
   The caller uses `await mediator.send(request)`.
   There is no stream and no saga in this path.

`build_chain` assembles a chain in a test without the mediator. Application startup uses `bind`.

## What the list means

Order is the order of the list. The first handler whose condition matches returns. Handlers after it do not run. Put the narrow strategies first and the default last.

A handler that forgets `return await self.next(request)` and also returns nothing stops the chain with `None`. That is not a skip. Either return the result or pass the request on.

The list is still one registration. `commands_mapper` and `queries_mapper` both fill one `RequestMap`. Binding the same request from both raises `KeyError`.

Skip this skill when every step must run, or when the handler itself is the only decision. `cqrs-setup` already says that. This file only builds the chain.

A payment method, a file type, or a tenant policy is the usual split. The condition is a field on the request. The handler does not choose by reading a global. Exactly one handler returns. The others must call `next`.

Do not reorder the list to "run the important step last" unless that step is the default. Earlier handlers never see a request the default already answered, because the default is last.

## Do not

- Use a chain when every step must run. That is one handler, or a command plus domain events. See `cqrs-eda`.
- Use a chain of one handler. Bind that handler alone.
- Use a chain for logging or metrics. Those are `cqrs.middlewares.Middleware` on bootstrap.
- Treat HTTP CORS as this feature. A browser preflight is not a `CORRequestHandler`.
- Call the mediator from inside `handle`.
- Add a `Command` base or a `CommandBus`. The request is still `cqrs.Request`.

The chain stops at the first result. Later handlers do not see a request that an earlier handler already answered.

Short shape: [assets/payment_chain.py](assets/payment_chain.py).
Full example: `examples/cor/typed_cor_request_handler.py`.
