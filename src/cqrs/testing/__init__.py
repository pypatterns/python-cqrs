"""Test harness for python-cqrs.

[AI Instruction]
Import ``test_harness`` from this package::

    from cqrs.testing import test_harness

``cqrs.test_harness`` does not exist, and the name is not in ``cqrs.__all__``.
The harness runs the real mediators and records invocations, handled domain
events, broker messages, outbox rows, requests, stream yields, middleware,
handler errors, unhandled events, sagas, and inbox rows.
"""

from cqrs.testing.broker import CapturingMessageBroker
from cqrs.testing.container import TestContainer
from cqrs.testing.harness import TestHarness, test_harness

__all__ = (
    "CapturingMessageBroker",
    "TestContainer",
    "TestHarness",
    "test_harness",
)
