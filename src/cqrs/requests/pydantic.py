"""Pydantic-based request model (optional ``python-cqrs[pydantic]`` extra)."""

from __future__ import annotations

import sys

import pydantic

from cqrs.requests.request import IRequest

if sys.version_info >= (3, 11):
    from typing import Self  # novm
else:
    from typing_extensions import Self


class PydanticRequest(pydantic.BaseModel, IRequest):
    """
    Pydantic-based request implementation.

    Requires ``pip install python-cqrs[pydantic]``. The default ``Request``
    alias points at ``DCRequest``.
    """

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        return cls.model_validate(kwargs)

    def to_dict(self) -> dict:
        return self.model_dump(mode="python")


__all__ = ("PydanticRequest",)
