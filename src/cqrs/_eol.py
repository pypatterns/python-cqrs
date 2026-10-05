"""Package-level EOL notice for the 4.x maintenance line."""

from __future__ import annotations

import logging
import os
import warnings

_EOL_MESSAGE = (
    "python-cqrs 4.x is in maintenance mode after 5.0.0. "
    "Please migrate to 5.x when ready "
    "(docs: https://mkdocs.python-cqrs.dev/latest/). "
    'To stay on 4.x: pip install "python-cqrs>=4,<5". '
    "Suppress this optional log with CQRS_SUPPRESS_V4_EOL_WARNING=1 "
    "(DeprecationWarning is still emitted)."
)

_TRUTHY = frozenset({"1", "true", "yes"})


def _suppress_log() -> bool:
    value = os.environ.get("CQRS_SUPPRESS_V4_EOL_WARNING", "")
    return value.strip().lower() in _TRUTHY


def emit_v4_eol_warning() -> None:
    """Emit DeprecationWarning always; optional logger warning unless suppressed."""
    warnings.warn(_EOL_MESSAGE, DeprecationWarning, stacklevel=2)
    if not _suppress_log():
        logging.getLogger("cqrs").warning(_EOL_MESSAGE)
