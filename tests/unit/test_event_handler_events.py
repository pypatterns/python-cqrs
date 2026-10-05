"""Unit tests for EventHandler.events() method (sync, default returns ())."""

import pydantic

from cqrs.events import DomainEvent, EventHandler
from cqrs.models.event import IEvent


class _TestEvent(DomainEvent, frozen=True):
    """Test domain event."""

    item_id: str = pydantic.Field()


class _DefaultEventsHandler(EventHandler[_TestEvent]):
    """Handler that does not override events()."""

    async def handle(self, event: _TestEvent) -> None:
        pass


class _CustomEventsHandler(EventHandler[_TestEvent]):
    """Handler that overrides events property and returns follow-up events."""

    def __init__(self) -> None:
        self._follow_ups: list[IEvent] = []

    @property
    def events(self) -> tuple[IEvent, ...]:
        return tuple(self._follow_ups)

    async def handle(self, event: _TestEvent) -> None:
        self._follow_ups.append(_TestEvent(item_id=f"follow_{event.item_id}"))


async def test_event_handler_events_default_returns_empty() -> None:
    """Arrange: handler with default events. Act: access events. Assert: returns ()."""
    handler = _DefaultEventsHandler()
    result = handler.events
    assert result == ()


async def test_event_handler_events_custom_returns_follow_ups() -> None:
    """Arrange: handler that overrides events. Act: handle then access events. Assert: returns follow-ups."""
    handler = _CustomEventsHandler()
    await handler.handle(_TestEvent(item_id="1"))
    result = handler.events
    assert len(result) == 1
    assert result[0].item_id == "follow_1"  # type: ignore[attr-defined]
