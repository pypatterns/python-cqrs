"""
Errors raised by transports.

[AI Instruction]
Catch ``TransportError`` for a failed ``create_transport`` or a rejected
option. ``TransportClosed`` means ``send_message`` or ``subscribe`` ran
after ``close``. ``UnsupportedCapability`` means this backend cannot do
what the caller asked.
"""


class TransportError(Exception):
    """A transport URL, option, or backend could not be used."""


class TransportClosed(TransportError):
    """``send_message`` or ``subscribe`` was called after ``close``."""


class UnsupportedCapability(TransportError):
    """The backend's capabilities do not include the requested option."""


__all__ = (
    "TransportClosed",
    "TransportError",
    "UnsupportedCapability",
)
