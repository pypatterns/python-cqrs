from __future__ import annotations

import abc
import dataclasses
import typing
import uuid

from cqrs.events.event import IEvent
from cqrs.response import IResponse
from cqrs.saga.models import ContextT

Resp = typing.TypeVar("Resp", bound=typing.Optional[IResponse], covariant=True)


@dataclasses.dataclass(frozen=True)
class SagaStepResult(typing.Generic[ContextT, Resp]):
    """
    Result of a saga step execution.

    Contains the response from the step's act method and metadata about the step.

    This is an internal data structure used by the saga pattern implementation.

    Args:
        response: The response object from the step (can be None)
        step_type: Type of the step handler that produced this result
        with_error: Whether the step resulted in an error
        error_message: Error message if with_error is True
        error_traceback: Error traceback lines if with_error is True
        error_type: Type of exception if with_error is True
        saga_id: ID of the saga this step belongs to (set by execution layer).
                 Enables client code to trigger compensation immediately if the saga fails.

    Example::

        result = SagaStepResult(
            response=response,
            step_type=ReserveInventoryStep,
            with_error=False
        )
    """

    response: Resp
    step_type: type[SagaStepHandler[ContextT, Resp]]
    with_error: bool = False
    error_message: typing.Optional[str] = None
    error_traceback: typing.Optional[list[str]] = None
    error_type: typing.Optional[typing.Type[Exception]] = None
    saga_id: typing.Optional[uuid.UUID] = None


class SagaStepHandler(abc.ABC, typing.Generic[ContextT, Resp]):
    """
    The saga step handler interface.

    A saga step handler represents a single step in a distributed transaction saga.
    Each step can perform an action and provide compensation logic to rollback
    the action if a subsequent step fails. This enables distributed transactions
    without requiring two-phase commit.

    The saga pattern is useful when you need to coordinate multiple operations
    across different services or systems, where each operation can be compensated
    if the overall transaction fails.

    Usage::

      class ReserveInventoryStep(SagaStepHandler[OrderContext]):
          def __init__(self, inventory_api: InventoryAPIProtocol) -> None:
              self._inventory_api = inventory_api
              self._events: list[Event] = []

          @property
          def events(self) -> list[Event]:
              return self._events.copy()

          async def act(self, context: OrderContext) -> None:
              await self._inventory_api.reserve_items(
                  order_id=context.order_id,
                  items=context.items
              )
              self._events.append(InventoryReservedEvent(order_id=context.order_id))

          async def compensate(self, context: OrderContext) -> None:
              await self._inventory_api.release_items(
                  order_id=context.order_id,
                  items=context.items
              )

      class ProcessPaymentStep(SagaStepHandler[OrderContext]):
          def __init__(self, payment_api: PaymentAPIProtocol) -> None:
              self._payment_api = payment_api
              self._events: list[Event] = []

          @property
          def events(self) -> list[Event]:
              return self._events.copy()

          async def act(self, context: OrderContext) -> None:
              await self._payment_api.charge(
                  order_id=context.order_id,
                  amount=context.total_amount
              )
              self._events.append(PaymentProcessedEvent(order_id=context.order_id))

          async def compensate(self, context: OrderContext) -> None:
              await self._payment_api.refund(
                  order_id=context.order_id,
                  amount=context.total_amount
              )

    """

    def _generate_step_result(
        self,
        response: typing.Optional[IResponse],
        with_error: bool = False,
        error_message: typing.Optional[str] = None,
        error_traceback: typing.Optional[list[str]] = None,
        error_type: typing.Optional[typing.Type[Exception]] = None,
    ) -> SagaStepResult[ContextT, Resp]:
        """
        Generate a SagaStepResult with proper typing from the class.

        Args:
            response: The response object (can be None)
            with_error: Whether the step resulted in an error
            error_message: Error message if with_error is True
            error_traceback: Error traceback lines if with_error is True
            error_type: Type of exception if with_error is True

        Returns:
            A properly typed SagaStepResult instance
        """
        return SagaStepResult(
            response=typing.cast(Resp, response),
            step_type=typing.cast(
                typing.Type[SagaStepHandler[ContextT, Resp]],
                self.__class__,
            ),
            with_error=with_error,
            error_message=error_message,
            error_traceback=error_traceback,
            error_type=error_type,
        )

    @property
    def events(self) -> typing.Sequence[IEvent]:
        """
        Get the list of domain events produced by this step.

        Override in subclasses to return events generated during :meth:`act`.
        By default returns an empty sequence.

        Returns:
            A sequence of domain events that were generated during the execution
            of the act method. These events can be emitted after the step
            completes successfully.
        """
        return ()

    @abc.abstractmethod
    async def act(self, context: ContextT) -> SagaStepResult[ContextT, Resp]:
        """
        Execute the action for this saga step.

        This method performs the actual work of the step. If this step or any
        subsequent step fails, the compensate method will be called for all
        completed steps in reverse order.

        Args:
            context: The saga context object that contains shared state
                     across all steps in the saga.

        Raises:
            Exception: Any exception raised during execution will cause the
                      saga to fail and trigger compensation for all completed steps.

        """
        raise NotImplementedError

    @abc.abstractmethod
    async def compensate(self, context: ContextT) -> None:
        """
        Compensate (rollback) the action performed by this step.

        This async method is called when a saga fails and needs to rollback completed
        steps. It should undo the effects of the act method. Compensation is
        called in reverse order of step execution.

        Args:
            context: The saga context object that contains shared state
                     across all steps in the saga.

        Note:
            Compensation should be idempotent - it should be safe to call
            multiple times with the same context.

        """
        raise NotImplementedError


__all__ = (
    "SagaStepResult",
    "SagaStepHandler",
    "Resp",
)
