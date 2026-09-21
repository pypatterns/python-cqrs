"""
Example: Streaming Request Handler with Parallel Event Processing

This example demonstrates a practical use case of processing orders in batches
with parallel event handling. The system processes orders one by one (streaming),
and for each processed order, it triggers multiple event handlers in parallel.

Use case: Processing batch operations with real-time progress and parallel side effects.
Orders are processed sequentially (streaming), but events triggered by each order
are processed in parallel. This improves performance while maintaining order processing
sequence and providing real-time progress updates.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/streaming/streaming_handler_parallel_events.py

The example will:
- Process a batch of orders one by one (streaming)
- For each order, emit multiple domain events
- Process events in parallel (up to 3 concurrent handlers)
- Display progress updates as orders are processed
- Show summary statistics after processing completes

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Streaming Request Handler:
   - Process items one by one using StreamingRequestHandler
   - Yield results as each item is processed
   - Provide real-time progress updates
   - Emit multiple domain events for each processed item

2. Parallel Event Processing:
   - Events are processed in parallel (max_concurrent_event_handlers=3)
   - Multiple event handlers run simultaneously:
     * OrderProcessedEventHandler - sends email notifications
     * OrderAnalyticsEventHandler - updates analytics
     * InventoryUpdateEventHandler - updates inventory
     * AuditLogEventHandler - logs audit events

3. Event Handler Coordination:
   - Parallel processing improves performance
   - Controlled concurrency prevents resource exhaustion
   - Events are processed asynchronously without blocking order processing

4. Real-time Progress:
   - Results are yielded as orders are processed
   - Progress can be streamed to clients in real-time
   - Enables responsive user interfaces for long-running operations

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - di (dependency injection)
   - pydantic (for typed models)

================================================================================
"""

import asyncio
import logging
import typing
from collections import defaultdict
from datetime import datetime

import di
import pydantic

import cqrs
from cqrs.message_brokers import devnull
from cqrs.requests import bootstrap

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In-memory storage for demonstration purposes
ORDER_STORAGE: dict[str, dict] = {}
ANALYTICS_STORAGE: defaultdict[str, int] = defaultdict(int)
INVENTORY_STORAGE: defaultdict[str, int] = defaultdict(lambda: 100)
AUDIT_LOG: list[dict] = []
EMAIL_SENT_LOG: list[dict] = []


# ============================================================================
# Domain Models
# ============================================================================


class ProcessOrdersCommand(cqrs.Request):
    """Command to process a batch of orders."""

    order_ids: list[str] = pydantic.Field(description="List of order IDs to process")


class OrderProcessedResult(cqrs.Response):
    """Response for each processed order."""

    order_id: str = pydantic.Field()
    status: str = pydantic.Field()
    processed_at: datetime = pydantic.Field()
    items_count: int = pydantic.Field()


# ============================================================================
# Domain Events
# ============================================================================


class OrderProcessedEvent(cqrs.DomainEvent, frozen=True):
    """Event emitted when an order is processed."""

    order_id: str
    customer_id: str
    total_amount: float
    items_count: int
    processed_at: datetime


class OrderAnalyticsEvent(cqrs.DomainEvent, frozen=True):
    """Event for analytics tracking."""

    order_id: str
    customer_id: str
    total_amount: float
    category: str


class InventoryUpdateEvent(cqrs.DomainEvent, frozen=True):
    """Event for inventory updates."""

    order_id: str
    items: list[dict[str, typing.Union[str, int]]]  # [{"product_id": "prod1", "quantity": 2}]


class AuditLogEvent(cqrs.DomainEvent, frozen=True):
    """Event for audit logging."""

    order_id: str
    action: str
    timestamp: datetime
    metadata: dict


# ============================================================================
# Request Handler (Streaming)
# ============================================================================


class ProcessOrdersCommandHandler(
    cqrs.StreamingRequestHandler[ProcessOrdersCommand, OrderProcessedResult],
):
    """
    Streaming handler that processes orders one by one.

    After processing each order, it emits multiple domain events that will be
    processed in parallel by different event handlers.
    """

    def __init__(self):
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        """Return a copy of events collected during processing."""
        return self._events.copy()

    def clear_events(self) -> None:
        """Clear events after they have been processed and emitted."""
        self._events.clear()

    async def handle(
        self,
        request: ProcessOrdersCommand,
    ) -> typing.AsyncIterator[OrderProcessedResult]:
        """
        Process orders one by one, yielding results after each order.

        For each order, multiple events are generated that will be processed
        in parallel by different event handlers.
        """
        logger.info(f"Starting to process {len(request.order_ids)} orders")

        for order_id in request.order_ids:
            # Simulate order processing (e.g., validate, calculate totals, etc.)
            await asyncio.sleep(0.1)  # Simulate processing time

            # Mock order data
            order_data = {
                "order_id": order_id,
                "customer_id": f"customer_{order_id[-1]}",
                "total_amount": 100.0 + float(order_id[-1]) * 10,
                "items": [{"product_id": f"prod_{i}", "quantity": i + 1} for i in range(3)],
                "category": "electronics" if int(order_id[-1]) % 2 == 0 else "clothing",
            }

            ORDER_STORAGE[order_id] = order_data

            # Create result
            result = OrderProcessedResult(
                order_id=order_id,
                status="processed",
                processed_at=datetime.now(),
                items_count=len(order_data["items"]),
            )

            # Emit multiple domain events that will be processed in parallel
            self._events.append(
                OrderProcessedEvent(
                    order_id=order_id,
                    customer_id=order_data["customer_id"],
                    total_amount=order_data["total_amount"],
                    items_count=len(order_data["items"]),
                    processed_at=datetime.now(),
                ),
            )

            self._events.append(
                OrderAnalyticsEvent(
                    order_id=order_id,
                    customer_id=order_data["customer_id"],
                    total_amount=order_data["total_amount"],
                    category=order_data["category"],
                ),
            )

            self._events.append(
                InventoryUpdateEvent(
                    order_id=order_id,
                    items=[
                        {"product_id": item["product_id"], "quantity": item["quantity"]} for item in order_data["items"]
                    ],
                ),
            )

            self._events.append(
                AuditLogEvent(
                    order_id=order_id,
                    action="order_processed",
                    timestamp=datetime.now(),
                    metadata={"total_amount": order_data["total_amount"]},
                ),
            )

            logger.info(
                f"Processed order {order_id}, emitted {len(self._events)} events",
            )
            yield result


# ============================================================================
# Event Handlers
# ============================================================================


class OrderProcessedEventHandler(cqrs.EventHandler[OrderProcessedEvent]):
    """
    Handler for OrderProcessedEvent - sends email notifications.

    This handler simulates sending email notifications to customers.
    """

    async def handle(self, event: OrderProcessedEvent) -> None:
        """Send email notification to customer."""
        # Simulate email sending (network call)
        await asyncio.sleep(0.05)  # Simulate network delay

        email_data = {
            "to": f"customer_{event.customer_id}@example.com",
            "subject": f"Order {event.order_id} Processed",
            "body": f"Your order for ${event.total_amount} has been processed.",
            "sent_at": datetime.now(),
        }

        EMAIL_SENT_LOG.append(email_data)
        logger.info(
            f"📧 Email sent for order {event.order_id} " f"to customer {event.customer_id}",
        )


class OrderAnalyticsEventHandler(cqrs.EventHandler[OrderAnalyticsEvent]):
    """
    Handler for OrderAnalyticsEvent - updates analytics.

    This handler updates analytics counters for different categories.
    """

    async def handle(self, event: OrderAnalyticsEvent) -> None:
        """Update analytics counters."""
        # Simulate analytics processing (database update)
        await asyncio.sleep(0.03)  # Simulate database delay

        ANALYTICS_STORAGE[f"{event.category}_orders"] += 1
        ANALYTICS_STORAGE[f"{event.category}_revenue"] += int(event.total_amount)
        ANALYTICS_STORAGE["total_orders"] += 1

        logger.info(
            f"📊 Analytics updated for order {event.order_id} " f"in category {event.category}",
        )


class InventoryUpdateEventHandler(cqrs.EventHandler[InventoryUpdateEvent]):
    """
    Handler for InventoryUpdateEvent - updates inventory.

    This handler updates product inventory levels.
    """

    async def handle(self, event: InventoryUpdateEvent) -> None:
        """Update inventory levels."""
        # Simulate inventory update (database transaction)
        await asyncio.sleep(0.04)  # Simulate database delay

        for item in event.items:
            product_id: str = item["product_id"]  # type: ignore[assignment]
            quantity: int = item["quantity"]  # type: ignore[assignment]
            INVENTORY_STORAGE[product_id] -= quantity

        logger.info(
            f"📦 Inventory updated for order {event.order_id}, " f"items: {len(event.items)}",
        )


class AuditLogEventHandler(cqrs.EventHandler[AuditLogEvent]):
    """
    Handler for AuditLogEvent - logs audit information.

    This handler writes audit logs for compliance and tracking.
    """

    async def handle(self, event: AuditLogEvent) -> None:
        """Write audit log entry."""
        # Simulate audit log writing (file/database write)
        await asyncio.sleep(0.02)  # Simulate I/O delay

        audit_entry = {
            "order_id": event.order_id,
            "action": event.action,
            "timestamp": event.timestamp.isoformat(),
            "metadata": event.metadata,
        }

        AUDIT_LOG.append(audit_entry)
        logger.info(f"📝 Audit log written for order {event.order_id}")


# ============================================================================
# Mappers
# ============================================================================


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    """Map commands to handlers."""
    mapper.bind(ProcessOrdersCommand, ProcessOrdersCommandHandler)


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    """Map domain events to handlers."""
    mapper.bind(OrderProcessedEvent, OrderProcessedEventHandler)
    mapper.bind(OrderAnalyticsEvent, OrderAnalyticsEventHandler)
    mapper.bind(InventoryUpdateEvent, InventoryUpdateEventHandler)
    mapper.bind(AuditLogEvent, AuditLogEventHandler)


# ============================================================================
# Main Example
# ============================================================================


async def main():
    """
    Demonstrate streaming handler with parallel event processing.

    This example shows:
    1. Processing orders one by one (streaming)
    2. For each order, emitting multiple events
    3. Processing events in parallel (with max_concurrent_event_handlers=3)
    4. All event handlers run concurrently, improving performance
    """
    logger.info("=" * 80)
    logger.info("Streaming Handler with Parallel Event Processing Example")
    logger.info("=" * 80)

    # Create mediator with parallel event processing enabled
    # max_concurrent_event_handlers=3 means up to 3 event handlers can run simultaneously
    mediator = bootstrap.bootstrap_streaming(
        di_container=di.Container(),
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        message_broker=devnull.DevnullMessageBroker(),
        max_concurrent_event_handlers=3,  # Process up to 3 events in parallel
        concurrent_event_handle_enable=True,  # Enable parallel processing
    )

    # Process a batch of orders
    order_ids = ["order_001", "order_002", "order_003", "order_004", "order_005"]
    command = ProcessOrdersCommand(order_ids=order_ids)

    logger.info(f"\nProcessing {len(order_ids)} orders...")
    logger.info("Each order will trigger 4 events processed in parallel\n")

    start_time = datetime.now()
    results = []

    # Stream results as orders are processed
    async for result in mediator.stream(command):
        if result is not None and isinstance(result, OrderProcessedResult):
            results.append(result)
            logger.info(
                f"✅ Received result: Order {result.order_id} "
                f"({result.items_count} items) - Status: {result.status}",
            )

    end_time = datetime.now()
    processing_time = (end_time - start_time).total_seconds()

    # Allow fire-and-forget parallel event handlers to finish (EventProcessor
    # uses create_task when concurrent_event_handle_enable=True and does not await)
    await asyncio.sleep(0.3)

    # Print summary
    logger.info("\n" + "=" * 80)
    logger.info("Processing Summary")
    logger.info("=" * 80)
    logger.info(f"Total orders processed: {len(results)}")
    logger.info(f"Total processing time: {processing_time:.2f} seconds")
    logger.info(f"Emails sent: {len(EMAIL_SENT_LOG)}")
    logger.info(f"Analytics entries: {len(ANALYTICS_STORAGE)}")
    logger.info(f"Audit log entries: {len(AUDIT_LOG)}")
    logger.info("\nAnalytics breakdown:")
    for key, value in sorted(ANALYTICS_STORAGE.items()):
        logger.info(f"  {key}: {value}")

    logger.info("\nInventory levels:")
    for product_id, quantity in sorted(INVENTORY_STORAGE.items()):
        logger.info(f"  {product_id}: {quantity} units")

    logger.info("\n" + "=" * 80)
    logger.info("Example completed successfully!")
    logger.info("=" * 80)

    # Verify results: one event-handler invocation per order per handler type
    assert len(results) == len(order_ids)
    assert len(EMAIL_SENT_LOG) == len(order_ids)
    assert len(AUDIT_LOG) == len(order_ids)
    assert ANALYTICS_STORAGE["total_orders"] == len(order_ids)


if __name__ == "__main__":
    asyncio.run(main())
