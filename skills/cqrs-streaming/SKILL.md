---
name: cqrs-streaming
description: >-
  Streams many results from one python-cqrs request before the work finishes.
  Use when the caller reads progress, a batch, or a sequence instead of one send.
---

# Streaming requests

Read cqrs-setup before writing a stream. Use a stream when the caller reads progress before the final answer. One `send` is the ordinary command and query path.

The package is already installed. Do not add an install step.

Put the request in `app/commands.py` and the handler in `app/handlers.py`. Build the streaming mediator in `app/bootstrap.py`. Do not add a directory for the stream.

## Steps

1. Declare the request as `cqrs.Request[ItemResult]`.
   `ItemResult` subclasses `cqrs.Response`.
   One yield is one `ItemResult`.
2. Subclass `cqrs.StreamingRequestHandler[ThatRequest]`.
   `handle` is an async generator.
   `yield` each result.
   Do not annotate `handle` with the element type.
3. If the handler records events, keep them on a list.
   Return a copy from `events`.
   Implement `clear_events` so the next yield does not emit them again.
   The mediator emits events after each yield.
4. `mapper.bind(RequestType, Handler)` once.
   A stream is still one handler for that request type.
   A second bind raises `KeyError`.
5. Build `cqrs.bootstrap.requests.bootstrap_streaming` once at startup.
   This returns a `StreamingRequestMediator`.
   It is not the `RequestMediator` from `bootstrap`.
6. The caller iterates `async for item in mediator.stream(request)`.
   Do not `await mediator.send` for this request.
   Exhaust the iterator or close it.

Build both mediators at startup when the application has ordinary commands and streams. One request type lives on one map.

## What each yield means

The iterator item is the request's parameter. A `Request[ItemProgress]` yields `ItemProgress`. The handler does not return a list. The caller collects the items if it needs all of them.

Record a domain event before the `yield` it belongs to. `clear_events` runs after that yield's events were emitted. Returning `self._events` without a copy lets `clear_events` empty the list the mediator is still reading.

A streaming command and an ordinary command do not share a mediator. Binding the streaming request on `commands_mapper` of `bootstrap` does not make `send` yield. Binding it only on `bootstrap_streaming` does not make `stream` available on the ordinary mediator.

Skip this skill when the caller needs one answer. The chooser in `cqrs-setup` already says that. This file only builds the stream.

## How to test

Collect the stream and compare it to `yields`, in order. A domain event emitted after a yield is on `handled`. `clear_events` does not remove it from the harness. Detail is `cqrs-testing`.

## Do not

- Use a stream for a single response. `RequestMediator.send` is that path.
- Use a stream to hide a slow domain event. The caller of `send` waits for domain handlers. A stream keeps the call open for a different reason: many results.
- Call `mediator.send` from inside `handle`.
- Annotate `handle` with `-> ItemResult` or `-> AsyncIterator[ItemResult]`. The element type is the request's parameter.
- Share one mediator between `send` and `stream`. They are different types.
- Copy `examples/streaming/typed_streaming_handler.py` into the application. Open it when the short shape is not enough.

A streaming command that also emits domain events still follows `cqrs-eda` for those events. Parallel domain handlers are `cqrs-eda`, and only when they do not share a session.

Short shape: [assets/progress_stream.py](assets/progress_stream.py).
Full example: `examples/streaming/typed_streaming_handler.py`.
