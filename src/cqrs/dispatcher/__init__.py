"""
Internal dispatchers used by mediators.

[AI Instruction]
Application code does not import this package. Call ``send``, ``stream``, or
``execute`` on a mediator from ``cqrs.bootstrap``. Replace a dispatcher only
by passing ``dispatcher_type`` when constructing a mediator directly.
"""

from cqrs.dispatcher.event import EventDispatcher
from cqrs.dispatcher.models import RequestDispatchResult, SagaDispatchResult
from cqrs.dispatcher.request import RequestDispatcher
from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.dispatcher.streaming import StreamingRequestDispatcher

__all__ = (
    "RequestDispatchResult",
    "SagaDispatchResult",
    "RequestDispatcher",
    "SagaDispatcher",
    "StreamingRequestDispatcher",
    "EventDispatcher",
)
