"""
FastAPI route calls mediator.send. TestClient uses dependency_overrides.

Run:
   pytest -c tests/pytest-config.ini --rootdir=. examples/testing/test_fastapi_app.py
"""

import di
import fastapi
from fastapi import testclient

import cqrs
import cqrs.outbox.mock
from cqrs.bootstrap import requests as bootstrap
from cqrs.testing import test_harness


class CreatedOrder(cqrs.Response):
    order_id: str


class CreateOrder(cqrs.Request[CreatedOrder]):
    sku: str


class OrderCreatedNotice(cqrs.NotificationEvent):
    event_name: str = "order_created"
    topic: str = "order_events"


outbox = cqrs.outbox.mock.MockOutboxedEventRepository(session_factory=dict)


class CreateOrderHandler(cqrs.RequestHandler[CreateOrder]):
    async def handle(self, request: CreateOrder):
        outbox.add(OrderCreatedNotice(payload={"sku": request.sku}))
        return CreatedOrder(order_id="1")


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(CreateOrder, CreateOrderHandler)


def mediator_factory() -> cqrs.RequestMediator:
    raise RuntimeError("dependency_overrides replaces mediator_factory")


app = fastapi.FastAPI()


@app.post("/orders", status_code=201)
async def create_order(
    sku: str = fastapi.Body(embed=True),
    mediator: cqrs.RequestMediator = fastapi.Depends(mediator_factory),
):
    result = await mediator.send(CreateOrder(sku=sku))
    return {"order_id": result.order_id}


async def test_route_sends_the_command_and_stores_the_row() -> None:
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=commands_mapper,
    )
    app.dependency_overrides[mediator_factory] = lambda: mediator
    client = testclient.TestClient(app)
    async with test_harness.around(mediator, outbox=outbox) as harness:
        response = client.post("/orders", json={"sku": "book"})

        assert response.status_code == 201
        assert harness.requests.contains(CreateOrder)
        assert harness.outbox.pending_count == 1
