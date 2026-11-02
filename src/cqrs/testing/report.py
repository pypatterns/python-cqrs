"""Full harness dump attached to journal ``AssertionError`` messages.

[AI Instruction]
Journals call ``render_failure`` when ``assert_*`` or ``one`` fails. The text
goes in ``AssertionError.args[0]`` only. Do not print it. ``rich`` is optional:
tables if it imports and renders, the same sections as plain text if it is
missing or if rendering fails. A failed dump stays an ``AssertionError``.
"""

from __future__ import annotations

import dataclasses
import io
import os
import typing


def _looked_name(looked_for: typing.Any) -> str:
    if looked_for is None:
        return "(none)"
    if isinstance(looked_for, tuple):
        return ", ".join(_looked_name(item) for item in looked_for)
    name = getattr(looked_for, "__name__", None)
    if isinstance(name, str):
        return name
    return str(looked_for)


def _call_title(
    journal: str,
    method: str,
    looked_for: typing.Any,
    filters: dict[str, typing.Any],
) -> str:
    args: list[str] = []
    if looked_for is not None:
        if isinstance(looked_for, tuple):
            args.extend(_looked_name(item) for item in looked_for)
        elif isinstance(looked_for, type):
            args.append(looked_for.__name__)
        elif isinstance(looked_for, str):
            args.append(looked_for)
        else:
            args.append(_looked_name(looked_for))
    for key, value in filters.items():
        args.append(f"{key}={value}")
    return f"{journal}.{method}({', '.join(args)}) failed"


def _format_filters(filters: dict[str, typing.Any], has_where: bool) -> str:
    parts = [f"{key}={value}" for key, value in filters.items()]
    if has_where:
        parts.append("where")
    return ", ".join(parts) if parts else "(none)"


def _brief(value: typing.Any, limit: int = 120) -> str:
    if value is None:
        return ""
    text = repr(value)
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def _fields_brief(value: typing.Any) -> str:
    if value is None:
        return ""
    text = repr(value)
    name = type(value).__name__
    prefix = name + "("
    if text.startswith(prefix) and text.endswith(")"):
        return text[len(prefix) : -1]
    return text


def _want_color() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    return True


def _load_rich() -> typing.Any | None:
    try:
        from rich.console import Console
        from rich.markup import escape
        from rich.panel import Panel
        from rich.pretty import Pretty
        from rich.table import Table
    except Exception:
        return None
    return Console, Panel, Pretty, Table, escape


@dataclasses.dataclass
class _Section:
    title: str
    columns: list[str]
    rows: list[list[typing.Any]]
    empty_note: str = "empty"


class HarnessReport:
    """Live view of every journal, rendered when an assertion fails."""

    def __init__(
        self,
        *,
        requests: typing.Any,
        invocations: typing.Any,
        handled: typing.Any,
        unhandled: typing.Any,
        published: typing.Any,
        outbox: typing.Any,
        inbox: typing.Any,
        sagas: typing.Any,
        middlewares: typing.Any,
        handler_errors: typing.Any,
        yields: list[typing.Any],
    ) -> None:
        self._requests = requests
        self._invocations = invocations
        self._handled = handled
        self._unhandled = unhandled
        self._published = published
        self._outbox = outbox
        self._inbox = inbox
        self._sagas = sagas
        self._middlewares = middlewares
        self._handler_errors = handler_errors
        self._yields = yields

    def render(
        self,
        *,
        journal: str,
        method: str,
        looked_for: typing.Any = None,
        filters: dict[str, typing.Any] | None = None,
        has_where: bool = False,
        matches: int = 0,
    ) -> str:
        filters = filters or {}
        title = _call_title(journal, method, looked_for, filters)
        looked = _looked_name(looked_for)
        filter_text = _format_filters(filters, has_where)
        hints = self._hints(journal, looked_for)
        sections = self._sections()
        rich = _load_rich()
        if rich is not None:
            try:
                return self._render_rich(rich, title, looked, filter_text, matches, hints, sections)
            except Exception:
                pass
        return self._render_plain(title, looked, filter_text, matches, hints, sections)

    def _hints(self, journal: str, looked_for: typing.Any) -> list[str]:
        hints: list[str] = []
        pending = getattr(self._outbox, "pending_count", 0)
        published_count = len(self._published)
        if journal == "published" and pending > 0 and published_count == 0:
            hints.append("outbox has pending rows — call produce_available to publish them")
        if looked_for is None:
            return hints
        if isinstance(looked_for, tuple):
            event_type = looked_for[0] if looked_for else None
            handler_type = looked_for[1] if len(looked_for) > 1 else None
            if isinstance(event_type, type) and self._handled.contains(event_type):
                hints.append(f"{event_type.__name__} is in handled")
            if isinstance(handler_type, type) and not self._invocations.of(handler_type):
                hints.append(f"{handler_type.__name__} was not entered")
            return hints
        if not isinstance(looked_for, type):
            return hints
        name = looked_for.__name__
        if journal == "published":
            if self._handled.contains(looked_for):
                hints.append(f"{name} is in handled, not on the broker")
            if self._unhandled.contains(looked_for):
                hints.append(f"{name} is in unhandled")
            for item in self._invocations:
                if item.argument_type is looked_for:
                    hints.append(f"{name} was an argument to {item.handler.__name__}.{item.method}")
                    break
            if pending == 0 and published_count == 0:
                hints.append("outbox is empty — this was not waiting for produce_available")
        elif journal == "handled":
            if self._published.contains(looked_for):
                hints.append(f"{name} is on the broker, not in handled")
            if self._unhandled.contains(looked_for):
                hints.append(f"{name} is in unhandled")
        elif journal == "unhandled":
            if self._handled.contains(looked_for):
                hints.append(f"{name} is in handled")
            if self._published.contains(looked_for):
                hints.append(f"{name} is on the broker")
        elif journal == "invocations":
            if len(self._requests) and not self._invocations.of(looked_for):
                hints.append(f"{name} was not entered, but a request was sent")
        elif journal == "outbox":
            if pending > 0 and published_count == 0:
                if "produce_available" not in "".join(hints):
                    hints.append("outbox has pending rows — call produce_available to publish them")
            if published_count == 0 and pending == 0 and self._handled.contains(looked_for):
                hints.append(f"{name} is in handled, not in the outbox")
        elif journal == "requests":
            if not self._requests.contains(looked_for) and self._invocations.of(looked_for):
                hints.append(f"{name} is in invocations")
        return hints

    def _sections(self) -> list[_Section]:
        request_rows: list[list[typing.Any]] = []
        for item in self._requests:
            response = item.exception if item.exception is not None else item.response
            request_rows.append(
                [
                    f"{type(item.request).__name__}\n{_fields_brief(item.request)}",
                    _brief(response),
                ],
            )
        invocation_rows = [
            [
                item.handler.__name__,
                item.method,
                _brief(item.argument),
            ]
            for item in self._invocations
        ]
        handled_rows = [[type(event).__name__, _fields_brief(event)] for event in self._handled]
        unhandled_rows = [[type(event).__name__, _fields_brief(event)] for event in self._unhandled]
        published_rows: list[list[typing.Any]] = []
        for message in self._published:
            nbytes = len(message.payload_bytes) if message.payload_bytes is not None else 0
            published_rows.append(
                [
                    message.message_name,
                    message.topic,
                    message.content_type or "",
                    message.payload,
                    f"{nbytes} bytes",
                ],
            )
        outbox_events = list(getattr(self._outbox, "events", ()))
        outbox_rows = [[type(event).__name__, _fields_brief(event)] for event in outbox_events]
        pending = getattr(self._outbox, "pending_count", 0)
        produced = getattr(self._outbox, "produced_count", 0)
        not_produced = getattr(self._outbox, "not_produced_count", 0)
        inbox_rows = [
            [
                row.message_id,
                str(row.status),
                str(row.flush_counter),
                row.message_name,
            ]
            for row in self._inbox
        ]
        saga_rows = [
            [
                snapshot.name,
                str(snapshot.status),
                ", ".join(snapshot.completed_steps),
            ]
            for snapshot in self._sagas
        ]
        middleware_rows = [[item.middleware.__name__, str(item.called_next)] for item in self._middlewares]
        error_rows = [
            [
                type(item.event).__name__,
                item.handler.__name__,
                type(item.error).__name__,
                str(item.error),
            ]
            for item in self._handler_errors
        ]
        yield_rows = [[str(index), _brief(item)] for index, item in enumerate(self._yields)]
        return [
            _Section(f"requests ({len(self._requests)})", ["request", "response"], request_rows),
            _Section(
                f"invocations ({len(self._invocations)})",
                ["handler", "method", "argument"],
                invocation_rows,
            ),
            _Section(f"handled ({len(self._handled)})", ["type", "event"], handled_rows),
            _Section(f"unhandled ({len(self._unhandled)})", ["type", "event"], unhandled_rows),
            _Section(
                f"published ({len(self._published)})",
                ["message_name", "topic", "content_type", "payload", "payload_bytes"],
                published_rows,
            ),
            _Section(
                f"outbox pending={pending} produced={produced} not_produced={not_produced}",
                ["type", "event"],
                outbox_rows,
                empty_note="",
            ),
            _Section(
                f"inbox ({len(self._inbox)})",
                ["message_id", "status", "flush_counter", "message_name"],
                inbox_rows,
            ),
            _Section(f"sagas ({len(self._sagas)})", ["name", "status", "steps"], saga_rows),
            _Section(
                f"middlewares ({len(self._middlewares)})",
                ["type", "called_next"],
                middleware_rows,
            ),
            _Section(
                f"handler_errors ({len(self._handler_errors)})",
                ["event", "handler", "error", "message"],
                error_rows,
            ),
            _Section(
                "yields" if not self._yields else f"yields ({len(self._yields)})",
                ["index", "value"],
                yield_rows,
                empty_note="[]",
            ),
        ]

    def _render_plain(
        self,
        title: str,
        looked: str,
        filter_text: str,
        matches: int,
        hints: list[str],
        sections: list[_Section],
    ) -> str:
        lines = [
            "cqrs.testing",
            title,
            "",
            f"looked for  {looked}",
            f"filters     {filter_text}",
            f"matches     {matches}",
        ]
        if hints:
            lines.append("")
            lines.append("Hints")
            for hint in hints:
                lines.append(f"  {hint}")
        for section in sections:
            lines.append("")
            if not section.rows:
                if section.empty_note:
                    lines.append(f"{section.title}   {section.empty_note}")
                else:
                    lines.append(section.title)
                continue
            lines.append(section.title)
            for row in section.rows:
                cells = [_plain_cell(cell) for cell in row]
                paired = [f"{column}={cell}" for column, cell in zip(section.columns, cells)]
                lines.append("  " + "  ".join(paired))
        return "\n".join(lines)

    def _render_rich(
        self,
        rich: typing.Any,
        title: str,
        looked: str,
        filter_text: str,
        matches: int,
        hints: list[str],
        sections: list[_Section],
    ) -> str:
        Console, Panel, Pretty, Table, escape = rich
        color = _want_color()
        buffer = io.StringIO()
        console = Console(
            record=True,
            width=120,
            file=buffer,
            no_color=not color,
            force_terminal=True if os.environ.get("FORCE_COLOR") and color else None,
        )
        header = f"{title}\n\n" f"looked for  {looked}\n" f"filters     {filter_text}\n" f"matches     {matches}"
        console.print(Panel(escape(header), title="cqrs.testing"))
        if hints:
            console.print("Hints")
            for hint in hints:
                console.print(escape(f"  {hint}"))
        for section in sections:
            if not section.rows:
                if section.empty_note:
                    console.print(escape(f"{section.title}   {section.empty_note}"))
                else:
                    console.print(escape(section.title))
                continue
            table = Table(title=escape(section.title), expand=True)
            for column in section.columns:
                table.add_column(column)
            for row in section.rows:
                table.add_row(*[_rich_cell(cell, Pretty, escape) for cell in row])
            console.print(table)
        return console.export_text(styles=color)


def _plain_cell(cell: typing.Any) -> str:
    if isinstance(cell, str):
        return cell.replace("\n", " ")
    return _brief(cell)


def _rich_cell(
    cell: typing.Any,
    pretty: typing.Any,
    escape: typing.Callable[[str], str],
) -> typing.Any:
    if isinstance(cell, (dict, list)):
        return pretty(cell)
    return escape(str(cell))


def render_failure(
    report: HarnessReport | None,
    *,
    journal: str,
    method: str,
    looked_for: typing.Any = None,
    filters: dict[str, typing.Any] | None = None,
    has_where: bool = False,
    matches: int = 0,
) -> str:
    if report is None:
        filters = filters or {}
        return (
            f"{_call_title(journal, method, looked_for, filters)}\n"
            f"looked for  {_looked_name(looked_for)}\n"
            f"filters     {_format_filters(filters, has_where)}\n"
            f"matches     {matches}"
        )
    return report.render(
        journal=journal,
        method=method,
        looked_for=looked_for,
        filters=filters,
        has_where=has_where,
        matches=matches,
    )


__all__ = (
    "HarnessReport",
    "render_failure",
)
