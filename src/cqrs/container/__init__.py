"""
Dependency-injection containers and scope helpers.

[AI Instruction]
``Container``, ``SimpleContainer``, ``ScopeStrategy``, ``bind_scope``,
``enter_scope``, and ``current_container`` are also on ``cqrs``.
``SimpleContainer`` is the default and has no extra. ``cqrs.DIContainer``
lives in ``cqrs.container.di`` and wraps the ``di`` package (extra ``di``).
The other adapters are not re-exported here:
``DependencyInjectorCQRSContainer`` is ``cqrs.container.dependency_injector``
(extra ``dependency-injector``), and ``DishkaCQRSContainer`` is
``cqrs.container.dishka`` (extra ``dishka``).
"""

from cqrs.container.protocol import Container, SupportsScope
from cqrs.container.simple import SimpleContainer
from cqrs.container.scope import (
    ScopeAwareContainer,
    ScopeStrategy,
    bind_scope,
    current_container,
    enter_scope,
)

__all__ = (
    "Container",
    "SupportsScope",
    "SimpleContainer",
    "ScopeAwareContainer",
    "ScopeStrategy",
    "bind_scope",
    "current_container",
    "enter_scope",
)
