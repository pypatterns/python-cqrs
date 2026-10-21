"""
Request middleware.

[AI Instruction]
Import ``Middleware`` and ``MiddlewareChain`` from this package. They are
not attributes of ``cqrs``. Logging is
``cqrs.middlewares.logging.LoggingMiddleware``. Pass middleware instances to
the ``middlewares`` argument of bootstrap. Bootstrap appends logging when
the list does not already contain it.
"""

from cqrs.middlewares.base import Middleware, MiddlewareChain

__all__ = ("Middleware", "MiddlewareChain")
