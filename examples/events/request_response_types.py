"""
Example: Different Request and Response Types

This example demonstrates the flexibility of the CQRS library in supporting different
types of Request and Response implementations. The library supports both Pydantic-based
and Dataclass-based implementations, allowing you to choose the best fit for your needs.

Use case: Flexibility in choosing request/response implementations. You can use:
- PydanticRequest/PydanticResponse for validation and serialization features
- DCRequest/DCResponse for lightweight implementations without Pydantic dependency
- Mix and match different types based on your requirements

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Run the example:
   python examples/events/request_response_types.py

The example will:
- Demonstrate Pydantic-based requests and responses
- Demonstrate Dataclass-based requests and responses
- Show mixed usage (Pydantic request with Dataclass response, etc.)
- Verify that all types work correctly with the mediator

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. PydanticRequest and PydanticResponse:
   - Use Pydantic models for automatic validation
   - Benefit from Pydantic's serialization features
   - Type-safe with runtime validation

2. DCRequest and DCResponse:
   - Use Python dataclasses for lightweight implementations
   - No Pydantic dependency required
   - Simple and straightforward

3. Mixed Usage:
   - Combine Pydantic requests with Dataclass responses
   - Combine Dataclass requests with Pydantic responses
   - Flexibility to choose the best type for each use case

4. Type Compatibility:
   - All request types implement IRequest interface
   - All response types implement IResponse interface
   - Mediator works seamlessly with all types

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - di (dependency injection)
   - pydantic (for PydanticRequest/PydanticResponse)

================================================================================
"""

import asyncio
import dataclasses
import logging
import typing

import di
import pydantic

import cqrs
from cqrs.requests import bootstrap

logging.basicConfig(level=logging.INFO)

# Storage for demonstration
USER_STORAGE: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
PRODUCT_STORAGE: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
ORDER_STORAGE: typing.Dict[str, typing.Dict[str, typing.Any]] = {}

# ============================================================================
# Pydantic-based Request and Response
# ============================================================================


class CreateUserCommand(cqrs.PydanticRequest):
    """Pydantic-based command with automatic validation."""

    username: str
    email: str
    age: int = pydantic.Field(gt=0, le=120)


class UserResponse(cqrs.PydanticResponse):
    """Pydantic-based response with validation."""

    user_id: str
    username: str
    email: str
    age: int


class CreateUserCommandHandler(cqrs.RequestHandler[CreateUserCommand, UserResponse]):
    """Handler using Pydantic request and response."""

    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return []

    async def handle(self, request: CreateUserCommand) -> UserResponse:
        user_id = f"user_{len(USER_STORAGE) + 1}"
        user_data = {
            "user_id": user_id,
            "username": request.username,
            "email": request.email,
            "age": request.age,
        }
        USER_STORAGE[user_id] = user_data
        print(f"Created user with Pydantic: {user_data}")
        return UserResponse(**user_data)


# ============================================================================
# Dataclass-based Request and Response
# ============================================================================


@dataclasses.dataclass
class CreateProductCommand(cqrs.DCRequest):
    """Dataclass-based command - lightweight, no Pydantic dependency."""

    name: str
    price: float
    category: str


@dataclasses.dataclass
class ProductResponse(cqrs.DCResponse):
    """Dataclass-based response - simple and straightforward."""

    product_id: str
    name: str
    price: float
    category: str


class CreateProductCommandHandler(
    cqrs.RequestHandler[CreateProductCommand, ProductResponse],
):
    """Handler using Dataclass request and response."""

    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return []

    async def handle(self, request: CreateProductCommand) -> ProductResponse:
        product_id = f"product_{len(PRODUCT_STORAGE) + 1}"
        product_data = {
            "product_id": product_id,
            "name": request.name,
            "price": request.price,
            "category": request.category,
        }
        PRODUCT_STORAGE[product_id] = product_data
        print(f"Created product with Dataclass: {product_data}")
        return ProductResponse(**product_data)


# ============================================================================
# Mixed: Pydantic Request with Dataclass Response
# ============================================================================


class CreateOrderCommand(cqrs.PydanticRequest):
    """Pydantic request with validation."""

    user_id: str
    product_id: str
    quantity: int = pydantic.Field(gt=0)


@dataclasses.dataclass
class OrderResponse(cqrs.DCResponse):
    """Dataclass response - lightweight."""

    order_id: str
    user_id: str
    product_id: str
    quantity: int
    total_price: float


class CreateOrderCommandHandler(
    cqrs.RequestHandler[CreateOrderCommand, OrderResponse],
):
    """Handler mixing Pydantic request with Dataclass response."""

    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return []

    async def handle(self, request: CreateOrderCommand) -> OrderResponse:
        if request.user_id not in USER_STORAGE:
            raise ValueError(f"User {request.user_id} not found")
        if request.product_id not in PRODUCT_STORAGE:
            raise ValueError(f"Product {request.product_id} not found")

        order_id = f"order_{len(ORDER_STORAGE) + 1}"
        product = PRODUCT_STORAGE[request.product_id]
        total_price = product["price"] * request.quantity

        order_data = {
            "order_id": order_id,
            "user_id": request.user_id,
            "product_id": request.product_id,
            "quantity": request.quantity,
            "total_price": total_price,
        }
        ORDER_STORAGE[order_id] = order_data
        print(f"Created order (Pydantic request + Dataclass response): {order_data}")
        return OrderResponse(**order_data)


# ============================================================================
# Mixed: Dataclass Request with Pydantic Response
# ============================================================================


@dataclasses.dataclass
class GetUserQuery(cqrs.DCRequest):
    """Dataclass query - simple and lightweight."""

    user_id: str


class UserDetailsResponse(cqrs.PydanticResponse):
    """Pydantic response with validation."""

    user_id: str
    username: str
    email: str
    age: int
    total_orders: int = 0


class GetUserQueryHandler(
    cqrs.RequestHandler[GetUserQuery, UserDetailsResponse],
):
    """Handler mixing Dataclass request with Pydantic response."""

    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return []

    async def handle(self, request: GetUserQuery) -> UserDetailsResponse:
        if request.user_id not in USER_STORAGE:
            raise ValueError(f"User {request.user_id} not found")

        user = USER_STORAGE[request.user_id]
        total_orders = sum(1 for order in ORDER_STORAGE.values() if order["user_id"] == request.user_id)

        return UserDetailsResponse(
            user_id=user["user_id"],
            username=user["username"],
            email=user["email"],
            age=user["age"],
            total_orders=total_orders,
        )


# ============================================================================
# Mapping and Bootstrap
# ============================================================================


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    """Register all command handlers."""
    mapper.bind(CreateUserCommand, CreateUserCommandHandler)
    mapper.bind(CreateProductCommand, CreateProductCommandHandler)
    mapper.bind(CreateOrderCommand, CreateOrderCommandHandler)


def queries_mapper(mapper: cqrs.RequestMap) -> None:
    """Register all query handlers."""
    mapper.bind(GetUserQuery, GetUserQueryHandler)


# ============================================================================
# Main Execution
# ============================================================================


async def main():
    """Demonstrate different request/response type combinations."""
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=commands_mapper,
        queries_mapper=queries_mapper,
    )

    print("=" * 80)
    print("Demonstrating Different Request/Response Types")
    print("=" * 80)
    print()

    # 1. Pydantic Request + Pydantic Response
    print("1. Pydantic Request + Pydantic Response")
    print("-" * 80)
    user_response = await mediator.send(
        CreateUserCommand(username="john_doe", email="john@example.com", age=30),
    )
    print(f"Response type: {type(user_response).__name__}")
    print(f"Response data: {user_response.to_dict()}")
    print()

    # 2. Dataclass Request + Dataclass Response
    print("2. Dataclass Request + Dataclass Response")
    print("-" * 80)
    product_response = await mediator.send(
        CreateProductCommand(name="Laptop", price=999.99, category="Electronics"),
    )
    print(f"Response type: {type(product_response).__name__}")
    print(f"Response data: {product_response.to_dict()}")
    print()

    # 3. Pydantic Request + Dataclass Response
    print("3. Pydantic Request + Dataclass Response")
    print("-" * 80)
    order_response = await mediator.send(
        CreateOrderCommand(
            user_id=user_response.user_id,
            product_id=product_response.product_id,
            quantity=2,
        ),
    )
    print(f"Response type: {type(order_response).__name__}")
    print(f"Response data: {order_response.to_dict()}")
    print()

    # 4. Dataclass Request + Pydantic Response
    print("4. Dataclass Request + Pydantic Response")
    print("-" * 80)
    user_details = await mediator.send(GetUserQuery(user_id=user_response.user_id))
    print(f"Response type: {type(user_details).__name__}")
    print(f"Response data: {user_details.to_dict()}")
    print()

    # Demonstrate serialization/deserialization
    print("=" * 80)
    print("Serialization/Deserialization Demo")
    print("=" * 80)
    print()

    # Serialize Pydantic response
    user_dict = user_response.to_dict()
    print(f"Pydantic response serialized: {user_dict}")
    restored_user = UserResponse.from_dict(**user_dict)
    print(f"Pydantic response restored: {restored_user}")
    print()

    # Serialize Dataclass response
    product_dict = product_response.to_dict()
    print(f"Dataclass response serialized: {product_dict}")
    restored_product = ProductResponse.from_dict(**product_dict)
    print(f"Dataclass response restored: {restored_product}")
    print()

    # Validation example with Pydantic
    print("=" * 80)
    print("Pydantic Validation Example")
    print("=" * 80)
    try:
        # This should fail validation (age > 120)
        await mediator.send(
            CreateUserCommand(username="invalid", email="test@example.com", age=150),
        )
    except pydantic.ValidationError as e:
        print(f"Validation error caught (expected): {e}")
    print()

    print("=" * 80)
    print("All examples completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
