"""
Example: FastAPI Integration with Saga Pattern and Server-Sent Events (SSE)

This example demonstrates how to integrate Saga pattern with FastAPI using
Server-Sent Events (SSE) to stream saga step results to clients in real-time.

Use case: Long-running distributed transactions (e.g., order processing) where
each step of the saga needs to be reported to the client as it completes.
The client receives real-time updates for each step: inventory reservation,
payment processing, shipping, etc. If any step fails, compensation events
are also streamed.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Option 1: Run Server Only (Interactive via Swagger UI)
-------------------------------------------------------
1. Start the server:
   python examples/fastapi_saga_sse.py

2. Open your browser and go to:
   http://localhost:8000/docs

3. Find the POST /process-order endpoint and click "Try it out"

4. Enter request body:
   {
     "order_id": "order_123",
     "user_id": "user_456",
     "items": ["item_1", "item_2"],
     "total_amount": 99.99,
     "shipping_address": "123 Main St, City, Country"
   }

5. Click "Execute" to see the streaming response with saga steps

Option 2: Use curl to test SSE endpoint
----------------------------------------
Start the server first, then run:
   curl -N -X POST http://localhost:8000/process-order \
     -H "Content-Type: application/json" \
     -d '{
       "order_id": "order_123",
       "user_id": "user_456",
       "items": ["item_1", "item_2"],
       "total_amount": 99.99,
       "shipping_address": "123 Main St"
     }'

The -N flag disables buffering so you'll see events as they arrive.

Option 3: Use Python requests library
---------------------------------------
   import requests

   response = requests.post(
       "http://localhost:8000/process-order",
       json={
           "order_id": "order_123",
           "user_id": "user_456",
           "items": ["item_1", "item_2"],
           "total_amount": 99.99,
           "shipping_address": "123 Main St"
       },
       stream=True
   )

   for line in response.iter_lines():
       if line:
           print(line.decode('utf-8'))

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Saga Pattern with SSE:
   - Execute saga steps sequentially
   - Stream each step result to client via SSE as it completes
   - Real-time progress updates for long-running transactions
   - Automatic compensation when steps fail

2. Saga Step Execution:
   - SagaMediator is created using bootstrap.bootstrap()
   - Each step is executed and its result is immediately sent via SSE
   - Step metadata (step name, response, status) is included
   - Progress tracking (current step, total steps, percentage)
   - Steps are executed in order: ReserveInventory -> ProcessPayment -> ShipOrder
   - Saga state and execution history are persisted to SagaStorage

3. Saga Storage and Logging:
   - MemorySagaStorage/SqlAlchemySagaStorage support create_run(): one session per saga,
     checkpoint commits (fewer commits, better performance)
   - SagaStorage persists saga state and execution history
   - Each step execution is logged (act/compensate, status, timestamp)
   - Storage enables recovery of interrupted sagas
   - Use GET /saga/{saga_id}/history endpoint to view execution log

4. Saga Step Handlers:
   - Each step handler inherits from SagaStepHandler[Context, Response]
   - act() method performs the step's action and returns SagaStepResult
   - compensate() method undoes the action if saga fails
   - Step handlers receive services via dependency injection

5. Server-Sent Events (SSE):
   - Real-time streaming of step results to clients
   - Progress updates sent as each step completes
   - Client receives events as they happen
   - SSE format: "data: {json}\n\n"

6. Error Handling and Compensation:
   - If a step fails, compensation is triggered automatically
   - Completed steps are compensated in reverse order
   - Error events are streamed via SSE
   - Original exception is re-raised after compensation
   - Compensation steps are also logged to storage

7. Dependency Injection:
   - Step handlers are resolved from DI container
   - Services (InventoryService, PaymentService, ShippingService) are injected
   - Enables testability and loose coupling

8. SSE Event Types:
   - "start" - Saga execution started with total steps count and saga_id
   - "step_progress" - A step completed successfully with response data
   - "complete" - All steps completed successfully
   - "error" - Saga failed with error details

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - fastapi
   - uvicorn
   - cqrs (this package)
   - pydantic (for context models)

================================================================================
"""

import asyncio
import dataclasses
import json
import logging
import typing
import uuid

import di
import fastapi
import pydantic
import uvicorn
from di import dependent

import cqrs
from cqrs.response import Response
from cqrs.saga import bootstrap
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.memory import MemorySagaStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = fastapi.FastAPI(title="Saga SSE Example")


# ============================================================================
# Models
# ============================================================================


@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str
    user_id: str
    items: list[str]
    total_amount: float
    shipping_address: str

    inventory_reservation_id: typing.Optional[str] = None
    payment_id: typing.Optional[str] = None
    shipment_id: typing.Optional[str] = None


class ProcessOrderRequest(pydantic.BaseModel):
    order_id: str
    user_id: str
    items: list[str]
    total_amount: float
    shipping_address: str

    model_config = pydantic.ConfigDict(
        json_schema_extra={
            "example": {
                "order_id": "order_123",
                "user_id": "user_456",
                "items": ["item_1", "item_2"],
                "total_amount": 99.99,
                "shipping_address": "123 Main St",
            },
        },
    )


class ReserveInventoryResponse(Response):
    reservation_id: str
    items_reserved: list[str]


class ProcessPaymentResponse(Response):
    payment_id: str
    amount_charged: float
    transaction_id: str


class ShipOrderResponse(Response):
    shipment_id: str
    tracking_number: str


# ============================================================================
# Mock Services
# ============================================================================


class InventoryService:
    def __init__(self) -> None:
        self._available_items = {"item_1": 10, "item_2": 5, "item_3": 8}

    async def reserve_items(self, order_id: str, items: list[str]) -> str:
        await asyncio.sleep(0.5)
        reservation_id = f"reservation_{order_id}"
        for item_id in items:
            if item_id not in self._available_items:
                raise ValueError(f"Item {item_id} not found")
            self._available_items[item_id] -= 1
        return reservation_id

    async def release_items(self, reservation_id: str) -> None:
        await asyncio.sleep(0.2)
        logger.info(f"Released reservation {reservation_id}")


class PaymentService:
    def __init__(self) -> None:
        self._counter = 0

    async def charge(self, order_id: str, amount: float) -> tuple[str, str]:
        await asyncio.sleep(0.5)
        if amount > 10000:
            raise ValueError("Payment amount exceeds limit")
        self._counter += 1
        return f"payment_{order_id}", f"txn_{self._counter:06d}"

    async def refund(self, payment_id: str) -> None:
        await asyncio.sleep(0.2)
        logger.info(f"Refunded {payment_id}")


class ShippingService:
    def __init__(self) -> None:
        self._counter = 0

    async def create_shipment(
        self,
        order_id: str,
        items: list[str],
        address: str,
    ) -> tuple[str, str]:
        await asyncio.sleep(0.5)
        if not address:
            raise ValueError("Shipping address required")
        self._counter += 1
        return f"shipment_{order_id}", f"TRACK{self._counter:08d}"

    async def cancel_shipment(self, shipment_id: str) -> None:
        await asyncio.sleep(0.2)
        logger.info(f"Cancelled {shipment_id}")


# ============================================================================
# Saga Step Handlers
# ============================================================================


class ReserveInventoryStep(
    SagaStepHandler[OrderContext, ReserveInventoryResponse],
):
    def __init__(self, inventory_service: InventoryService) -> None:
        self._inventory_service = inventory_service
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        reservation_id = await self._inventory_service.reserve_items(
            context.order_id,
            context.items,
        )
        context.inventory_reservation_id = reservation_id
        return self._generate_step_result(
            ReserveInventoryResponse(
                reservation_id=reservation_id,
                items_reserved=context.items,
            ),
        )

    async def compensate(self, context: OrderContext) -> None:
        if context.inventory_reservation_id:
            await self._inventory_service.release_items(
                context.inventory_reservation_id,
            )


class ProcessPaymentStep(
    SagaStepHandler[OrderContext, ProcessPaymentResponse],
):
    def __init__(self, payment_service: PaymentService) -> None:
        self._payment_service = payment_service
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ProcessPaymentResponse]:
        payment_id, transaction_id = await self._payment_service.charge(
            context.order_id,
            context.total_amount,
        )
        context.payment_id = payment_id
        return self._generate_step_result(
            ProcessPaymentResponse(
                payment_id=payment_id,
                amount_charged=context.total_amount,
                transaction_id=transaction_id,
            ),
        )

    async def compensate(self, context: OrderContext) -> None:
        if context.payment_id:
            await self._payment_service.refund(context.payment_id)


class ShipOrderStep(SagaStepHandler[OrderContext, ShipOrderResponse]):
    def __init__(self, shipping_service: ShippingService) -> None:
        self._shipping_service = shipping_service
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ShipOrderResponse]:
        shipment_id, tracking_number = await self._shipping_service.create_shipment(
            context.order_id,
            context.items,
            context.shipping_address,
        )
        context.shipment_id = shipment_id
        return self._generate_step_result(
            ShipOrderResponse(
                shipment_id=shipment_id,
                tracking_number=tracking_number,
            ),
        )

    async def compensate(self, context: OrderContext) -> None:
        if context.shipment_id:
            await self._shipping_service.cancel_shipment(context.shipment_id)


# ============================================================================
# Saga Class Definition
# ============================================================================


class OrderSaga(Saga[OrderContext]):
    """Order processing saga with three steps."""

    steps = [
        ReserveInventoryStep,
        ProcessPaymentStep,
        ShipOrderStep,
    ]


# ============================================================================
# DI Container Setup
# ============================================================================

# Shared storage (MemorySagaStorage uses create_run(): scoped run, checkpoint commits)
saga_storage = MemorySagaStorage()

# Setup DI container
di_container = di.Container()

# Register services using bind_by_type with lambda functions that return instances
di_container.bind(
    di.bind_by_type(
        dependent.Dependent(InventoryService, scope="request"),
        InventoryService,
    ),
)
di_container.bind(
    di.bind_by_type(
        dependent.Dependent(PaymentService, scope="request"),
        PaymentService,
    ),
)
di_container.bind(
    di.bind_by_type(
        dependent.Dependent(ShippingService, scope="request"),
        ShippingService,
    ),
)


# ============================================================================
# FastAPI Routes
# ============================================================================


def saga_mapper(mapper: cqrs.SagaMap) -> None:
    """Register sagas in SagaMap."""
    mapper.bind(OrderContext, OrderSaga)


def mediator_factory() -> cqrs.SagaMediator:
    """Create saga mediator using bootstrap."""
    return bootstrap.bootstrap(
        di_container=di_container,
        sagas_mapper=saga_mapper,
        saga_storage=saga_storage,
    )


def serialize_response(response: typing.Optional[Response]) -> dict[str, typing.Any]:
    if response is None:
        return {}
    return response.to_dict()


@app.post("/process-order")
async def process_order(
    request: ProcessOrderRequest,
    mediator: cqrs.SagaMediator = fastapi.Depends(mediator_factory),
) -> fastapi.responses.StreamingResponse:
    """Process order using saga pattern, stream step results via SSE."""

    async def generate_sse():
        saga_id = uuid.uuid4()  # Generate unique saga ID for persistence
        context = OrderContext(
            order_id=request.order_id,
            user_id=request.user_id,
            items=request.items,
            total_amount=request.total_amount,
            shipping_address=request.shipping_address,
        )

        # Get total steps count from saga (we know it's 3 steps)
        total_steps = 3
        completed_steps = 0

        try:
            # Start event
            start_data = {
                "type": "start",
                "saga_id": str(saga_id),
                "total_steps": total_steps,
                "order_id": context.order_id,
            }
            yield f"data: {json.dumps(start_data)}\n\n"

            # Execute saga with saga_id for persistence using mediator.stream()
            async for step_result in mediator.stream(context, saga_id=saga_id):
                completed_steps += 1
                step_name = step_result.step_type.__name__

                step_data = {
                    "type": "step_progress",
                    "step": {
                        "name": step_name,
                        "number": completed_steps,
                        "total": total_steps,
                    },
                    "response": serialize_response(step_result.response),
                }
                yield f"data: {json.dumps(step_data)}\n\n"

            # Completion event
            complete_data = {
                "type": "complete",
                "saga_id": str(saga_id),
                "order_id": context.order_id,
                "total_steps": completed_steps,
            }
            yield f"data: {json.dumps(complete_data)}\n\n"

        except Exception as e:
            # Error event
            error_data = {
                "type": "error",
                "saga_id": str(saga_id),
                "message": str(e),
                "error_type": type(e).__name__,
                "completed_steps": completed_steps,
            }
            yield f"data: {json.dumps(error_data)}\n\n"
            logger.error(f"Saga {saga_id} failed: {e}", exc_info=True)

    return fastapi.responses.StreamingResponse(
        generate_sse(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


@app.get("/saga/{saga_id}/history")
async def get_saga_history(saga_id: uuid.UUID) -> dict[str, typing.Any]:
    """Get execution history (SagaLog) for a saga."""
    try:
        status, context_data, _ = await saga_storage.load_saga_state(saga_id)
        history = await saga_storage.get_step_history(saga_id)

        return {
            "saga_id": str(saga_id),
            "status": status.value,
            "context": context_data,
            "history": [
                {
                    "step_name": entry.step_name,
                    "action": entry.action,
                    "status": entry.status.value,
                    "timestamp": entry.timestamp.isoformat(),
                    "details": entry.details,
                }
                for entry in history
            ],
        }
    except ValueError as e:
        raise fastapi.HTTPException(status_code=404, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
