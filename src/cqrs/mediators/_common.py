import logging

from cqrs.container.scope import ScopeStrategy
from cqrs.events.event_emitter import EventEmitter

logger = logging.getLogger("cqrs")


def _warn_on_emitter_strategy_mismatch(
    event_emitter: EventEmitter | None,
    scope_strategy: ScopeStrategy,
) -> None:
    """Warn when a hand-built emitter scopes differently than the mediator."""
    if event_emitter is None:
        return
    emitter_strategy = getattr(event_emitter, "_scope_strategy", None)
    if emitter_strategy is None or emitter_strategy == scope_strategy:
        return
    logger.warning(
        "EventEmitter scope_strategy (%s) differs from mediator scope_strategy (%s). "
        "Command and event handlers may resolve different scoped instances "
        "(e.g. separate units of work). Pass the same strategy to both, or build "
        "the mediator via cqrs.bootstrap.",
        getattr(emitter_strategy, "value", emitter_strategy),
        getattr(scope_strategy, "value", scope_strategy),
    )
