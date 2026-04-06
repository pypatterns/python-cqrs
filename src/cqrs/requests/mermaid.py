"""Mermaid diagram generator for Chain of Responsibility Request Handlers."""

import inspect
import typing

from cqrs.requests.cor_request_handler import CORRequestHandler


class CoRMermaid:
    """
    Generator for Mermaid diagrams from Chain of Responsibility handlers.

    This class can generate:
    - Sequence diagrams showing execution flow through the chain
    - Class diagrams showing types, relationships, and structure

    Usage::

        handlers = [Handler1, Handler2, Handler3]
        generator = CoRMermaid(handlers)

        # Generate sequence diagram
        sequence_diagram = generator.sequence()

        # Generate class diagram
        class_diagram = generator.class_diagram()
    """

    def __init__(
        self,
        handlers: typing.Sequence[type[CORRequestHandler]],
    ) -> None:
        """
        Initialize Mermaid diagram generator.

        Args:
            handlers: List of handler classes in chain order.
        """
        self._handlers = handlers

    def sequence(self) -> str:
        """
        Generate a Mermaid Sequence diagram showing chain execution flow.

        Returns:
            A string containing the Mermaid Sequence diagram code.
        """
        handlers = self._handlers

        if not handlers:
            return "sequenceDiagram\n    participant C as Chain\n    Note over C: No handlers configured"

        # Generate participant aliases for better readability
        participants = ["C as Chain"]
        handler_aliases: dict[str, str] = {}

        for idx, handler_type in enumerate(handlers, start=1):
            handler_name = handler_type.__name__
            # Create short alias (H1, H2, H3, etc.)
            alias = f"H{idx}"
            handler_aliases[handler_name] = alias
            # Truncate long handler names for better diagram readability
            display_name = handler_name if len(handler_name) <= 30 else handler_name[:27] + "..."
            participants.append(f"{alias} as {display_name}")

        lines = ["sequenceDiagram"]
        lines.extend(f"    participant {p}" for p in participants)
        lines.append("")

        # Generate single flow showing that each handler can either process or pass to next
        lines.append("    Note over C: Chain Execution Flow")

        # Start with Chain calling first handler
        if len(handlers) > 0:
            first_handler = handlers[0]
            first_alias = handler_aliases[first_handler.__name__]
            lines.append(f"    C->>{first_alias}: handle(request)")
            lines.append("")

        # Build nested alt blocks showing decision at each handler
        # Each handler can either process (return result) or pass to next
        def build_handler_flow(handler_idx: int, indent_level: int = 0) -> None:
            """Build nested alt blocks for handler chain."""
            if handler_idx >= len(handlers):
                return

            handler_type = handlers[handler_idx]
            handler_name = handler_type.__name__
            alias = handler_aliases[handler_name]
            indent = "    " * (1 + indent_level)

            # Show that handler can process and return result (stops chain)
            # Use handler name instead of alias in alt condition
            lines.append(f"{indent}alt {handler_name} can handle")
            lines.append(f"{indent}    {alias}-->>C: result")
            lines.append(
                f"{indent}    Note over {alias}: Handler processed, chain stops",
            )
            lines.append(f"{indent}else")

            # Show that handler passes to next (if not last)
            if handler_idx < len(handlers) - 1:
                next_alias = handler_aliases[handlers[handler_idx + 1].__name__]
                lines.append(f"{indent}    {alias}->>{next_alias}: next(request)")
                lines.append(
                    f"{indent}    Note over {alias}: Cannot handle, passing to next",
                )
                # Recursively add next handler's decision
                build_handler_flow(handler_idx + 1, indent_level + 1)
            else:
                # Last handler must process (default handler)
                lines.append(f"{indent}    {alias}-->>C: result")
                lines.append(
                    f"{indent}    Note over {alias}: Handler processed (default)",
                )

            lines.append(f"{indent}end")

        # Start building flow from first handler
        build_handler_flow(0)

        return "\n".join(lines)

    def class_diagram(self) -> str:
        """
        Generate a Mermaid Class diagram showing chain structure, types, and relationships.

        The diagram includes:
        - CORRequestHandler base class
        - Handler classes with their request and response types
        - Request classes
        - Response classes
        - Relationships between handlers (chain links)
        - Relationships between handlers and request/response types

        Returns:
            A string containing the Mermaid Class diagram code.
        """
        handlers = self._handlers

        if not handlers:
            return "classDiagram\n    class CORRequestHandler\n    Note for CORRequestHandler: No handlers configured"

        # Collect all types
        request_types: set[type] = set()
        response_types: set[type] = set()
        handler_info: list[
            tuple[
                str,
                typing.Optional[type],
                typing.Optional[type],
            ]
        ] = []

        # Extract type information from each handler
        for handler_type in handlers:
            handler_name = handler_type.__name__
            request_type: typing.Optional[type] = None
            response_type: typing.Optional[type] = None

            # Extract generic type parameters from __orig_bases__
            orig_bases = getattr(handler_type, "__orig_bases__", ())
            for base in orig_bases:
                origin = typing.get_origin(base)
                # Check if this base is CORRequestHandler or a subclass
                if origin is CORRequestHandler:
                    args = typing.get_args(base)
                    if len(args) >= 1 and args[0] is not typing.Any:
                        request_type = args[0]
                        if inspect.isclass(request_type):
                            request_types.add(request_type)
                    if len(args) >= 2 and args[1] is not typing.Any:
                        response_type = args[1]
                        if inspect.isclass(response_type):
                            response_types.add(response_type)
                    break  # Found the right base, no need to continue

            # If not found in __orig_bases__, try __bases__
            if request_type is None and response_type is None:
                for base in handler_type.__bases__:
                    if issubclass(base, CORRequestHandler):
                        # Try to get type hints from the class itself
                        if hasattr(handler_type, "__annotations__"):
                            # Check if we can infer from class definition
                            pass
                        break

            handler_info.append((handler_name, request_type, response_type))

        # Build class diagram
        lines = ["classDiagram"]

        # Add CORRequestHandler base class
        lines.append("    class CORRequestHandler {")
        lines.append("        <<abstract>>")
        lines.append("        +handle(request) typing.Optional[Response ]")
        lines.append("        +next(request) typing.Optional[Response ]")
        lines.append("        +set_next(handler) CORRequestHandler")
        lines.append("        +events: List[Event]")
        lines.append("    }")
        lines.append("")

        # Add handler classes
        for handler_name, request_type, response_type in handler_info:
            lines.append(f"    class {handler_name} {{")
            lines.append("        +handle(request) typing.Optional[Response ]")
            lines.append("        +next(request) typing.Optional[Response ]")
            lines.append("        +events: List[Event]")
            lines.append("    }")
            lines.append("")

        # Add request classes
        for request_type in sorted(request_types, key=lambda x: x.__name__):
            class_name = request_type.__name__
            lines.append(f"    class {class_name} {{")
            # Try to get fields if it's a dataclass
            if hasattr(request_type, "__dataclass_fields__"):
                fields = request_type.__dataclass_fields__
                for field_name, field_info in fields.items():
                    field_type = (
                        field_info.type.__name__ if hasattr(field_info.type, "__name__") else str(field_info.type)
                    )
                    lines.append(f"        +{field_name}: {field_type}")
            elif hasattr(request_type, "model_fields"):  # Pydantic v2
                fields = request_type.model_fields
                for field_name, field_info in fields.items():
                    field_type = (
                        field_info.annotation.__name__
                        if hasattr(field_info.annotation, "__name__")
                        else str(field_info.annotation)
                    )
                    lines.append(f"        +{field_name}: {field_type}")
            elif hasattr(request_type, "__fields__"):  # Pydantic v1
                fields = request_type.__fields__
                for field_name, field_info in fields.items():
                    field_type = (
                        field_info.type_.__name__ if hasattr(field_info.type_, "__name__") else str(field_info.type_)
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

        # Add inheritance relationships
        lines.append("    %% Inheritance relationships")
        for handler_name, _, _ in handler_info:
            lines.append(f"    CORRequestHandler <|-- {handler_name}")

        # Add chain relationships (set_next)
        lines.append("")
        lines.append("    %% Chain relationships (set_next)")
        for idx in range(len(handler_info) - 1):
            current_handler = handler_info[idx][0]
            next_handler = handler_info[idx + 1][0]
            lines.append(f"    {current_handler} --> {next_handler} : set_next")

        # Add handler to request relationships
        lines.append("")
        lines.append("    %% Handler to Request relationships")
        for handler_name, request_type, _ in handler_info:
            if request_type and inspect.isclass(request_type):
                lines.append(f"    {handler_name} ..> {request_type.__name__} : uses")

        # Add handler to response relationships
        lines.append("")
        lines.append("    %% Handler to Response relationships")
        for handler_name, _, response_type in handler_info:
            if response_type and inspect.isclass(response_type):
                lines.append(
                    f"    {handler_name} ..> {response_type.__name__} : returns",
                )

        return "\n".join(lines)
