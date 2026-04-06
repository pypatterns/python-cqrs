import asyncio
import typing
from collections import deque

from cqrs.events.event import IEvent
from cqrs.events.event_emitter import EventEmitter
from cqrs.events.map import EventMap


class EventProcessor:
    """
    Processes events in parallel or sequentially via an event emitter.

    Emits events through the configured :class:`~cqrs.events.event_emitter.EventEmitter`.
    Follow-up events returned by handlers (via :attr:`~cqrs.events.event_handler.EventHandler.events`)
    are processed in the same pipeline: BFS in sequential mode, under the same
    semaphore in parallel mode. Can be reused across different mediators.

    Example::

        event_map = EventMap()
        event_map.bind(OrderCreatedEvent, OrderCreatedEventHandler)
        emitter = EventEmitter(event_map=event_map, container=container)
        processor = EventProcessor(
            event_map=event_map,
            event_emitter=emitter,
            max_concurrent_event_handlers=4,
            concurrent_event_handle_enable=True,
        )
        await processor.emit_events([OrderCreatedEvent(order_id="1")])
    """

    def __init__(
        self,
        event_map: EventMap,
        event_emitter: typing.Optional[EventEmitter] = None,
        max_concurrent_event_handlers: int = 1,
        concurrent_event_handle_enable: bool = True,
    ) -> None:
        """
        Initialize the event processor.

        Args:
            event_map: Map of event types to handler types.
            event_emitter: Emitter used to publish events; if None, :meth:`emit_events`
                is a no-op.
            max_concurrent_event_handlers: Semaphore limit for parallel mode.
            concurrent_event_handle_enable: If True, process events in parallel
                (with semaphore); if False, process sequentially (BFS over events
                and follow-ups).
        """
        self._event_emitter = event_emitter
        self._event_map = event_map
        self._max_concurrent_event_handlers = max_concurrent_event_handlers
        self._concurrent_event_handle_enable = concurrent_event_handle_enable
        self._event_semaphore = asyncio.Semaphore(max_concurrent_event_handlers)

    async def emit_events(self, events: typing.Sequence[IEvent]) -> None:
        """
        Emit all events and process follow-ups in the same pipeline.

        In sequential mode, events and follow-ups are processed in BFS order.
        In parallel mode, events are processed under the same semaphore limit;
        as soon as any event completes, its follow-ups are queued and started
        (FIRST_COMPLETED), without waiting for siblings. Returns when all work
        is finished.

        Args:
            events: Events to emit (e.g. domain events). Handlers may return
                follow-up events via :attr:`~cqrs.events.event_handler.EventHandler.events`.

        Example::

            await processor.emit_events([
                OrderCreatedEvent(order_id="1"),
                OrderCreatedEvent(order_id="2"),
            ])
        """
        if not events:
            return

        if not self._event_emitter:
            return

        if not self._concurrent_event_handle_enable:
            # Process events sequentially (BFS: follow-ups re-queued, O(1) popleft)
            to_process: deque[IEvent] = deque(events)
            while to_process:
                event = to_process.popleft()
                follow_ups = await self._event_emitter.emit(event)
                to_process.extend(follow_ups)
        else:
            # Process events in parallel: start follow-ups as soon as any task completes
            # (FIRST_COMPLETED), all under the same semaphore
            await self._emit_events_parallel_first_completed(deque(events))

    async def _emit_one_event(self, event: IEvent) -> typing.Sequence[IEvent]:
        """
        Emit one event under the semaphore. Returns follow-up events from the handler.

        Args:
            event: The event to emit.

        Returns:
            Follow-up events to process next, or empty sequence.
        """
        if not self._event_emitter:
            return ()
        async with self._event_semaphore:
            follow_ups = await self._event_emitter.emit(event)
        if follow_ups is None:
            return ()
        return follow_ups

    async def _emit_events_parallel_first_completed(
        self,
        initial_events: deque[IEvent],
    ) -> None:
        """
        Process events in parallel under the semaphore; as soon as any task completes,
        its follow-up events are queued and started, without waiting for siblings.
        Uses deque for O(1) popleft when taking the next event.
        """
        pending_events: deque[IEvent] = initial_events
        running_tasks: set[asyncio.Task[typing.Sequence[IEvent]]] = set()

        while pending_events or running_tasks:
            # Start a task for each pending event (semaphore limits concurrency)
            while pending_events:
                event = pending_events.popleft()
                task = asyncio.create_task(self._emit_one_event(event))
                running_tasks.add(task)

            if not running_tasks:
                break

            done, running_tasks = await asyncio.wait(
                running_tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                follow_ups = task.result()
                pending_events.extend(follow_ups)
