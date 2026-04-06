"""Validation utilities for Saga steps and context types."""

import logging
import types
import typing

from cqrs.saga.fallback import Fallback
from cqrs.saga.step import SagaStepHandler

logger = logging.getLogger("cqrs.saga")


class SagaContextTypeExtractor:
    """Extracts context type from Generic type parameters."""

    @staticmethod
    def extract_from_class(klass: type, saga_base_class: type) -> typing.Optional[type]:
        """
        Extract context type from a class that inherits from a Generic base.

        Args:
            klass: The class to extract context type from
            saga_base_class: The base Generic class (e.g., Saga)

        Returns:
            The context type if found, None otherwise
        """
        # Try to get context type from the class's generic parameters
        # Check __orig_bases__ for Generic type
        if hasattr(klass, "__orig_bases__"):
            for base in klass.__orig_bases__:  # type: ignore[attr-defined]
                # Check if this is a GenericAlias for Saga
                if isinstance(base, types.GenericAlias) and base.__origin__ is saga_base_class:  # type: ignore[attr-defined]
                    args = typing.get_args(base)
                    if args:
                        return args[0]  # type: ignore[return-value]
                # Fallback for older Python versions or different typing implementations
                elif hasattr(base, "__origin__") and base.__origin__ is saga_base_class:  # type: ignore[attr-defined]
                    args = typing.get_args(base)
                    if args:
                        return args[0]  # type: ignore[return-value]

        # If we couldn't determine context type from Generic, try alternative methods
        # Try to get it from __class_getitem__ result
        if hasattr(klass, "__args__") and klass.__args__:  # type: ignore[attr-defined]
            return klass.__args__[0]  # type: ignore[return-value,index]

        return None

    @staticmethod
    def extract_from_step(step_type: type) -> typing.Optional[type]:
        """
        Extract context type from a SagaStepHandler class.

        Args:
            step_type: The step handler class

        Returns:
            The context type if found, None otherwise
        """
        # Try to get step's context type from its generic bases
        if hasattr(step_type, "__orig_bases__"):
            for base in step_type.__orig_bases__:  # type: ignore[attr-defined]
                # Check if this is a GenericAlias for SagaStepHandler
                if isinstance(base, types.GenericAlias) and base.__origin__ is SagaStepHandler:  # type: ignore[attr-defined]
                    args = typing.get_args(base)
                    if args:
                        return args[0]
                # Fallback
                elif hasattr(base, "__origin__") and base.__origin__ is SagaStepHandler:  # type: ignore[attr-defined]
                    args = typing.get_args(base)
                    if args:
                        return args[0]

        return None


class ContextTypeValidator:
    """Validates context type compatibility between saga and steps."""

    @staticmethod
    def validate(
        saga_context_type: type,
        step_context_type: type,
        saga_name: str,
        step_name: str,
        step_index: int,
        raise_on_mismatch: bool = True,
    ) -> None:
        """
        Validate that step's context type is compatible with saga's context type.

        Args:
            saga_context_type: The context type expected by the saga
            step_context_type: The context type expected by the step
            saga_name: Name of the saga class (for error messages)
            step_name: Name of the step (for error messages)
            step_index: Index of the step in the steps list (for error messages)
            raise_on_mismatch: If True, raise TypeError on mismatch. If False, log warning.

        Raises:
            TypeError: If types are incompatible and raise_on_mismatch is True
        """
        # Get origin types to handle type variables and unions
        origin_context = getattr(saga_context_type, "__origin__", saga_context_type)
        origin_step_context = getattr(
            step_context_type,
            "__origin__",
            step_context_type,
        )

        # Check if types match exactly
        if origin_context != origin_step_context:
            # Check if they're compatible types (subclass relationship)
            # This allows subclasses of the expected context type
            if isinstance(origin_context, type) and isinstance(
                origin_step_context,
                type,
            ):
                if not issubclass(origin_context, origin_step_context):
                    if raise_on_mismatch:
                        raise TypeError(
                            f"{saga_name} steps[{step_index}] ({step_name}) "
                            f"expects context type {step_context_type.__name__}, "
                            f"but saga expects {saga_context_type.__name__}. "
                            "Steps must handle the same context type as the saga.",
                        )
                    else:
                        logger.warning(
                            f"{saga_name} steps[{step_index}] ({step_name}) "
                            f"may have incompatible context type. "
                            f"Saga expects {saga_context_type.__name__}, "
                            f"step expects {step_context_type.__name__}.",
                        )
            else:
                # For non-type origins (like TypeVar), we can't validate at runtime
                # but we log a warning
                step_context_name = getattr(
                    step_context_type,
                    "__name__",
                    str(step_context_type),
                )
                if raise_on_mismatch:
                    logger.warning(
                        f"{saga_name} steps[{step_index}] ({step_name}) "
                        f"may have incompatible context type. "
                        f"Saga expects {saga_context_type.__name__}, "
                        f"step expects {step_context_name}.",
                    )
                else:
                    logger.warning(
                        f"{saga_name} steps[{step_index}] ({step_name}) "
                        f"may have incompatible context type. "
                        f"Saga expects {saga_context_type.__name__}, "
                        f"step expects {step_context_name}.",
                    )


class SagaStepValidator:
    """Validates saga steps structure and types."""

    def __init__(
        self,
        saga_name: str,
        context_type: typing.Optional[type] = None,
    ) -> None:
        """
        Initialize validator.

        Args:
            saga_name: Name of the saga class (for error messages)
            context_type: Optional context type to validate against
        """
        self._saga_name = saga_name
        self._context_type = context_type
        self._context_type_extractor = SagaContextTypeExtractor()
        self._context_type_validator = ContextTypeValidator()

    def validate_steps(
        self,
        steps: list[typing.Union[type[SagaStepHandler], Fallback]],
    ) -> None:
        """
        Validate saga steps.

        Ensures that:
        1. Steps is a list
        2. All steps are valid step handler types or Fallback instances
        3. All steps handle the correct context type

        Args:
            steps: List of steps to validate

        Raises:
            TypeError: If steps are invalid or don't match the context type
        """
        if not isinstance(steps, list):
            raise TypeError(
                f"{self._saga_name} steps must be a list of step handler types, " f"got {type(steps).__name__}",
            )

        if not steps:
            # Empty steps list is allowed (though unusual)
            return

        # Validate each step
        for i, step_item in enumerate(steps):
            if isinstance(step_item, Fallback):
                self._validate_fallback(step_item, i)
            else:
                self._validate_regular_step(step_item, i)

    def _validate_fallback(self, fallback_item: Fallback, index: int) -> None:
        """Validate a Fallback wrapper."""
        # Validate Fallback structure
        if not isinstance(fallback_item.step, type):
            raise TypeError(
                f"{self._saga_name} steps[{index}].step must be a class type, "
                f"got {type(fallback_item.step).__name__}",
            )
        if not isinstance(fallback_item.fallback, type):
            raise TypeError(
                f"{self._saga_name} steps[{index}].fallback must be a class type, "
                f"got {type(fallback_item.fallback).__name__}",
            )

        # Check that step and fallback are SagaStepHandler subclasses
        if not issubclass(fallback_item.step, SagaStepHandler):
            raise TypeError(
                f"{self._saga_name} steps[{index}].step ({fallback_item.step.__name__}) "
                "must be a subclass of SagaStepHandler",
            )
        if not issubclass(fallback_item.fallback, SagaStepHandler):
            raise TypeError(
                f"{self._saga_name} steps[{index}].fallback ({fallback_item.fallback.__name__}) "
                "must be a subclass of SagaStepHandler",
            )

        # No nested Fallback support
        if isinstance(fallback_item.fallback, Fallback):
            raise TypeError(
                f"{self._saga_name} steps[{index}].fallback cannot be a Fallback instance. "
                "Nested Fallback is not supported.",
            )

        # Validate context types for both step and fallback
        if self._context_type is not None:
            for step_type, step_name in [
                (fallback_item.step, "step"),
                (fallback_item.fallback, "fallback"),
            ]:
                step_context_type = self._context_type_extractor.extract_from_step(
                    step_type,
                )
                if step_context_type is not None:
                    self._context_type_validator.validate(
                        saga_context_type=self._context_type,
                        step_context_type=step_context_type,
                        saga_name=self._saga_name,
                        step_name=step_type.__name__,
                        step_index=index,
                        raise_on_mismatch=True,
                    )

    def _validate_regular_step(
        self,
        step_item: type[SagaStepHandler],
        index: int,
    ) -> None:
        """Validate a regular step handler."""
        if not isinstance(step_item, type):
            raise TypeError(
                f"{self._saga_name} steps[{index}] must be a class type or Fallback instance, "
                f"got {type(step_item).__name__}",
            )

        # Check if step is a subclass of SagaStepHandler
        if not issubclass(step_item, SagaStepHandler):
            raise TypeError(
                f"{self._saga_name} steps[{index}] ({step_item.__name__}) " "must be a subclass of SagaStepHandler",
            )

        # Validate context type compatibility
        if self._context_type is not None:
            step_context_type = self._context_type_extractor.extract_from_step(
                step_item,
            )
            if step_context_type is not None:
                self._context_type_validator.validate(
                    saga_context_type=self._context_type,
                    step_context_type=step_context_type,
                    saga_name=self._saga_name,
                    step_name=step_item.__name__,
                    step_index=index,
                    raise_on_mismatch=True,
                )
