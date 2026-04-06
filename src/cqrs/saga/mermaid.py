"""Mermaid diagram generator for Saga."""

import inspect
import typing

from cqrs.saga.fallback import Fallback
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler


class SagaMermaid:
    """
    Generator for Mermaid diagrams from Saga instances.

    This class can generate:
    - Sequence diagrams showing execution flow and compensation
    - Class diagrams showing types, relationships, and structure

    Usage::

        class MySaga(Saga[MyContext]):
            steps = [Step1, Step2, Step3]

        saga = MySaga()
        generator = SagaMermaid(saga)

        # Generate sequence diagram
        sequence_diagram = generator.sequence()

        # Generate class diagram
        class_diagram = generator.class_diagram()
    """

    def __init__(self, saga: Saga[typing.Any]) -> None:
        """
        Initialize Mermaid diagram generator.

        Args:
            saga: The saga instance to generate diagram for.
        """
        self._saga = saga

    def sequence(self) -> str:
        """
        Generate a Mermaid Sequence diagram showing all saga steps and compensations.

        Returns:
            A string containing the Mermaid Sequence diagram code.
        """
        steps = self._saga.steps

        if not steps:
            return "sequenceDiagram\n    participant S as Saga\n    Note over S: No steps configured"

        # Generate participant aliases for better readability
        participants = ["S as Saga"]
        step_aliases: dict[str, str] = {}
        fallback_aliases: dict[str, str] = {}
        step_idx = 1

        # Process steps and extract participants (including fallback steps)
        for step_item in steps:
            if isinstance(step_item, Fallback):
                # Handle Fallback wrapper
                primary_name = step_item.step.__name__
                fallback_name = step_item.fallback.__name__

                # Create aliases for primary and fallback
                primary_alias = f"S{step_idx}"
                fallback_alias = f"F{step_idx}"
                step_aliases[primary_name] = primary_alias
                fallback_aliases[fallback_name] = fallback_alias

                # Truncate long names
                primary_display = primary_name if len(primary_name) <= 30 else primary_name[:27] + "..."
                fallback_display = fallback_name if len(fallback_name) <= 30 else fallback_name[:27] + "..."

                participants.append(f"{primary_alias} as {primary_display}")
                participants.append(
                    f"{fallback_alias} as {fallback_display} (fallback)",
                )
                step_idx += 1
            else:
                # Regular step
                step_name = step_item.__name__
                alias = f"S{step_idx}"
                step_aliases[step_name] = alias
                display_name = step_name if len(step_name) <= 30 else step_name[:27] + "..."
                participants.append(f"{alias} as {display_name}")
                step_idx += 1

        lines = ["sequenceDiagram"]
        lines.extend(f"    participant {p}" for p in participants)
        lines.append("")

        # Generate successful execution flow
        lines.append("    Note over S: Successful Execution Flow")
        for step_item in steps:
            if isinstance(step_item, Fallback):
                # Fallback: show primary step succeeding
                primary_name = step_item.step.__name__
                primary_alias = step_aliases[primary_name]
                lines.append(f"    S->>{primary_alias}: act()")
                lines.append(f"    {primary_alias}-->>S: success")
            else:
                # Regular step
                step_name = step_item.__name__
                alias = step_aliases[step_name]
                lines.append(f"    S->>{alias}: act()")
                lines.append(f"    {alias}-->>S: success")
        lines.append("    Note over S: Saga Completed")
        lines.append("")

        # Generate failure and compensation flow with fallback
        lines.append("    Note over S: Failure & Compensation Flow")
        if len(steps) > 1:
            # Show all steps except the last one succeeding
            for step_item in steps[:-1]:
                if isinstance(step_item, Fallback):
                    primary_name = step_item.step.__name__
                    primary_alias = step_aliases[primary_name]
                    lines.append(f"    S->>{primary_alias}: act()")
                    lines.append(f"    {primary_alias}-->>S: success")
                else:
                    step_name = step_item.__name__
                    alias = step_aliases[step_name]
                    lines.append(f"    S->>{alias}: act()")
                    lines.append(f"    {alias}-->>S: success")

            # Show last step failing
            last_step = steps[-1]
            if isinstance(last_step, Fallback):
                # Fallback step: show primary failing, then fallback succeeding
                primary_name = last_step.step.__name__
                fallback_name = last_step.fallback.__name__
                primary_alias = step_aliases[primary_name]
                fallback_alias = fallback_aliases[fallback_name]

                lines.append(f"    S->>{primary_alias}: act()")
                lines.append(f"    {primary_alias}-->>S: error")
                lines.append("    Note over S: Fallback triggered")
                lines.append(f"    S->>{fallback_alias}: act()")
                lines.append(f"    {fallback_alias}-->>S: success")
            else:
                # Regular step failing
                last_alias = step_aliases[last_step.__name__]
                lines.append(f"    S->>{last_alias}: act()")
                lines.append(f"    {last_alias}-->>S: error")

            lines.append("")

            # Compensate completed steps in reverse order
            lines.append("    Note over S: Compensation (reverse order)")
            # Compensate all steps before the failing one (in reverse order)
            for step_item in reversed(steps[:-1]):
                if isinstance(step_item, Fallback):
                    # For fallback steps, compensate the step that actually executed
                    # In success case, it's the primary; in failure case, it could be fallback
                    # For simplicity, show primary compensation
                    primary_name = step_item.step.__name__
                    primary_alias = step_aliases[primary_name]
                    lines.append(f"    S->>{primary_alias}: compensate()")
                    lines.append(f"    {primary_alias}-->>S: success")
                else:
                    step_name = step_item.__name__
                    alias = step_aliases[step_name]
                    lines.append(f"    S->>{alias}: compensate()")
                    lines.append(f"    {alias}-->>S: success")
        else:
            # Single step scenario
            single_step = steps[0]
            if isinstance(single_step, Fallback):
                # Fallback step: show primary failing, then fallback succeeding
                primary_name = single_step.step.__name__
                fallback_name = single_step.fallback.__name__
                primary_alias = step_aliases[primary_name]
                fallback_alias = fallback_aliases[fallback_name]

                lines.append(f"    S->>{primary_alias}: act()")
                lines.append(f"    {primary_alias}-->>S: error")
                lines.append("    Note over S: Fallback triggered")
                lines.append(f"    S->>{fallback_alias}: act()")
                lines.append(f"    {fallback_alias}-->>S: success")
                lines.append("    Note over S: Saga Completed (via fallback)")
            else:
                single_alias = step_aliases[single_step.__name__]
                lines.append(f"    S->>{single_alias}: act()")
                lines.append(f"    {single_alias}-->>S: error")
                lines.append(
                    "    Note over S: No compensation needed (step failed before completion)",
                )

        lines.append("    Note over S: Saga Failed")

        return "\n".join(lines)

    def class_diagram(self) -> str:
        """
        Generate a Mermaid Class diagram showing saga structure, types, and relationships.

        The diagram includes:
        - Saga class with its steps
        - Step handler classes with their context and response types
        - Context classes
        - Response classes
        - Event classes produced by steps (if detectable)

        Returns:
            A string containing the Mermaid Class diagram code.
        """
        steps = self._saga.steps

        if not steps:
            return "classDiagram\n    class Saga\n    Note for Saga: No steps configured"

        # Collect all types
        context_types: set[type] = set()
        response_types: set[type] = set()
        event_types: set[type] = set()
        step_info: list[
            tuple[
                str,
                typing.Optional[type],
                typing.Optional[type],
                list[type],
            ]
        ] = []

        # Extract type information from each step
        for step_item in steps:
            if isinstance(step_item, Fallback):
                # Handle Fallback wrapper - extract info from both primary and fallback steps
                primary_step = step_item.step
                fallback_step = step_item.fallback

                # Process primary step
                primary_name = primary_step.__name__
                primary_context_type: typing.Optional[type] = None
                primary_response_type: typing.Optional[type] = None
                primary_events: list[type] = []

                # Extract generic type parameters from primary step
                orig_bases = getattr(primary_step, "__orig_bases__", ())
                for base in orig_bases:
                    origin = typing.get_origin(base)
                    if origin is SagaStepHandler:
                        args = typing.get_args(base)
                        if len(args) >= 1 and args[0] is not typing.Any:
                            primary_context_type = args[0]
                            if inspect.isclass(primary_context_type):
                                context_types.add(primary_context_type)
                        if len(args) >= 2 and args[1] is not typing.Any:
                            primary_response_type = args[1]
                            if inspect.isclass(primary_response_type):
                                response_types.add(primary_response_type)
                        break

                # Process fallback step
                fallback_name = fallback_step.__name__
                fallback_context_type: typing.Optional[type] = None
                fallback_response_type: typing.Optional[type] = None
                fallback_events: list[type] = []

                # Extract generic type parameters from fallback step
                orig_bases = getattr(fallback_step, "__orig_bases__", ())
                for base in orig_bases:
                    origin = typing.get_origin(base)
                    if origin is SagaStepHandler:
                        args = typing.get_args(base)
                        if len(args) >= 1 and args[0] is not typing.Any:
                            fallback_context_type = args[0]
                            if inspect.isclass(fallback_context_type):
                                context_types.add(fallback_context_type)
                        if len(args) >= 2 and args[1] is not typing.Any:
                            fallback_response_type = args[1]
                            if inspect.isclass(fallback_response_type):
                                response_types.add(fallback_response_type)
                        break

                # Add both primary and fallback steps to step_info
                step_info.append(
                    (
                        primary_name,
                        primary_context_type,
                        primary_response_type,
                        primary_events,
                    ),
                )
                step_info.append(
                    (
                        fallback_name,
                        fallback_context_type,
                        fallback_response_type,
                        fallback_events,
                    ),
                )
            else:
                # Regular step
                step_name = step_item.__name__
                context_type: typing.Optional[type] = None
                response_type: typing.Optional[type] = None
                step_events: list[type] = []

                # Extract generic type parameters from __orig_bases__
                orig_bases = getattr(step_item, "__orig_bases__", ())
                for base in orig_bases:
                    origin = typing.get_origin(base)
                    # Check if this base is SagaStepHandler or a subclass
                    if origin is SagaStepHandler:
                        args = typing.get_args(base)
                        if len(args) >= 1 and args[0] is not typing.Any:
                            context_type = args[0]
                            if inspect.isclass(context_type):
                                context_types.add(context_type)
                        if len(args) >= 2 and args[1] is not typing.Any:
                            response_type = args[1]
                            if inspect.isclass(response_type):
                                response_types.add(response_type)
                        break  # Found the right base, no need to continue

                # If not found in __orig_bases__, try __bases__
                if context_type is None and response_type is None:
                    for base in step_item.__bases__:
                        if issubclass(base, SagaStepHandler):
                            # Try to get type hints from the class itself
                            if hasattr(step_item, "__annotations__"):
                                # Check if we can infer from class definition
                                pass
                            break

                step_info.append((step_name, context_type, response_type, step_events))

        # Build class diagram
        lines = ["classDiagram"]

        # Add Saga class
        lines.append("    class Saga {")
        lines.append("        +steps: List[SagaStepHandler]")
        lines.append("        +transaction(context) SagaTransaction")
        lines.append("    }")
        lines.append("")

        # Add step handler classes
        for step_name, context_type, response_type, _ in step_info:
            lines.append(f"    class {step_name} {{")
            lines.append("        +act(context) SagaStepResult")
            lines.append("        +compensate(context) void")
            lines.append("        +events: List[Event]")
            lines.append("    }")
            lines.append("")

        # Add context classes
        for context_type in sorted(context_types, key=lambda x: x.__name__):
            class_name = context_type.__name__
            lines.append(f"    class {class_name} {{")
            # Try to get fields if it's a dataclass
            if hasattr(context_type, "__dataclass_fields__"):
                fields = context_type.__dataclass_fields__
                for field_name, field_info in fields.items():
                    field_type = (
                        field_info.type.__name__ if hasattr(field_info.type, "__name__") else str(field_info.type)
                    )
                    lines.append(f"        +{field_name}: {field_type}")
            lines.append("    }")
            lines.append("")

        # Add response classes
        for response_type in sorted(response_types, key=lambda x: x.__name__):
            class_name = response_type.__name__
            lines.append(f"    class {class_name} {{")
            # Try to get fields from dataclass or model
            if hasattr(response_type, "__dataclass_fields__"):
                fields = response_type.__dataclass_fields__
                for field_name, field_info in fields.items():
                    field_type = (
                        field_info.type.__name__ if hasattr(field_info.type, "__name__") else str(field_info.type)
                    )
                    lines.append(f"        +{field_name}: {field_type}")
            elif hasattr(response_type, "model_fields"):  # Pydantic v2
                fields = response_type.model_fields
                for field_name, field_info in fields.items():
                    field_type = (
                        field_info.annotation.__name__
                        if hasattr(field_info.annotation, "__name__")
                        else str(field_info.annotation)
                    )
                    lines.append(f"        +{field_name}: {field_type}")
            elif hasattr(response_type, "__fields__"):  # Pydantic v1
                fields = response_type.__fields__
                for field_name, field_info in fields.items():
                    field_type = (
                        field_info.type_.__name__ if hasattr(field_info.type_, "__name__") else str(field_info.type_)
                    )
                    lines.append(f"        +{field_name}: {field_type}")
            lines.append("    }")
            lines.append("")

        # Add relationships
        lines.append("    %% Saga relationships")
        for step_name, _, _, _ in step_info:
            lines.append(f"    Saga --> {step_name} : contains")

        lines.append("")
        lines.append("    %% Step to Context relationships")
        for step_name, context_type, _, _ in step_info:
            if context_type and inspect.isclass(context_type):
                lines.append(f"    {step_name} ..> {context_type.__name__} : uses")

        lines.append("")
        lines.append("    %% Step to Response relationships")
        for step_name, _, response_type, _ in step_info:
            if response_type and inspect.isclass(response_type):
                lines.append(f"    {step_name} ..> {response_type.__name__} : returns")

        # Note about events
        if event_types:
            lines.append("")
            lines.append("    %% Event classes")
            for event_type in sorted(event_types, key=lambda x: x.__name__):
                class_name = event_type.__name__
                lines.append(f"    class {class_name} {{")
                lines.append("        +Event fields")
                lines.append("    }")
                lines.append("")

            lines.append("    %% Step to Event relationships")
            for step_name, _, _, step_events in step_info:
                for event_type in step_events:
                    lines.append(
                        f"    {step_name} ..> {event_type.__name__} : produces",
                    )

        return "\n".join(lines)
