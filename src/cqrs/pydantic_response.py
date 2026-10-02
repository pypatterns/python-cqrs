"""Pydantic-based response model (optional ``python-cqrs[pydantic]`` extra)."""

from __future__ import annotations

import sys

import pydantic

from cqrs.response import IResponse

if sys.version_info >= (3, 11):
    from typing import Self  # novm
else:
    from typing_extensions import Self


class PydanticResponse(pydantic.BaseModel, IResponse):
    """
    Pydantic-based response implementation.

    Requires ``pip install python-cqrs[pydantic]``. The default ``Response``
    alias points at ``DCResponse``.
    """

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        return cls.model_validate(kwargs)

    def to_dict(self) -> dict:
        return self.model_dump(mode="python")


__all__ = ("PydanticResponse",)
