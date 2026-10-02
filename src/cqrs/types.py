"""
Type definitions for CQRS framework.

This module re-exports common type variables (ReqT, ResT) from
cqrs.requests.request for backward compatibility. Defining ReqT/ResT in
request.py avoids circular import with request_handler.
"""

from cqrs.models.request import ReqT, ResT

__all__ = ("ReqT", "ResT")
