"""
Bootstrap helpers for mediators (requests, events, saga).

[AI Instruction]
These submodules are the application entry points. ``cqrs`` does not export
them.

- ``cqrs.bootstrap.requests.bootstrap`` returns ``RequestMediator``.
  ``bootstrap_streaming`` in the same module returns
  ``StreamingRequestMediator``.
- ``cqrs.bootstrap.events.bootstrap`` returns ``EventMediator``.
- ``cqrs.bootstrap.saga.bootstrap`` returns ``SagaMediator``.

Import the submodule (``from cqrs.bootstrap import requests``) and call
``bootstrap`` on it. ``setup_*`` functions in those modules are for callers
that already hold an emitter or a container.
"""

from cqrs.bootstrap import events, requests, saga

__all__ = ("requests", "events", "saga")
