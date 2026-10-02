"""S1: optional pydantic/sqlalchemy extras and dataclass defaults."""

from __future__ import annotations

import dataclasses
import importlib.util

import pytest


def test_request_response_event_defaults_are_dataclass_based() -> None:
    import cqrs

    assert cqrs.Request is cqrs.DCRequest
    assert cqrs.Response is cqrs.DCResponse
    assert cqrs.Event is cqrs.DCEvent
    assert cqrs.DomainEvent is cqrs.DCDomainEvent
    assert cqrs.NotificationEvent is cqrs.DCNotificationEvent


def test_dataclass_subclass_without_decorator() -> None:
    import cqrs

    class Cmd(cqrs.Request):
        user_id: str

    class Res(cqrs.Response):
        ok: bool

    class Dom(cqrs.DomainEvent, frozen=True):
        user_id: str

    cmd = Cmd(user_id="u1")
    assert cmd.to_dict() == {"user_id": "u1"}
    assert dataclasses.is_dataclass(cmd)
    assert Res(ok=True).to_dict() == {"ok": True}
    assert Dom(user_id="u1").user_id == "u1"


def test_root_package_does_not_eager_import_sqlalchemy() -> None:
    """cqrs/__init__.py must not import sqlalchemy modules at import time."""
    source = importlib.util.find_spec("cqrs")
    assert source is not None and source.origin is not None
    text = open(source.origin, encoding="utf-8").read()
    # Strip TYPE_CHECKING blocks (type-checker only; not executed at runtime).
    import re

    runtime_text = re.sub(
        r"if typing\.TYPE_CHECKING:.*?(?=\n(?:[^\s#]|$))",
        "",
        text,
        flags=re.S,
    )
    assert "from cqrs.outbox.sqlalchemy" not in runtime_text
    assert "from cqrs.sqlalchemy_types" not in runtime_text
    import cqrs

    assert cqrs.EventProducer is not None


@pytest.mark.pydantic
def test_pydantic_types_available_with_extra() -> None:
    import cqrs

    assert cqrs.PydanticRequest is not None
    assert cqrs.PydanticResponse is not None
    assert cqrs.PydanticEvent is not None


@pytest.mark.sqlalchemy
def test_sqlalchemy_types_available_with_extra() -> None:
    import cqrs

    assert cqrs.SqlAlchemyOutboxedEventRepository is not None
    assert cqrs.UUIDBinary is not None


def test_producer_module_has_no_sqlalchemy_import() -> None:
    source = importlib.util.find_spec("cqrs.message_brokers.producer")
    assert source is not None and source.origin is not None
    text = open(source.origin, encoding="utf-8").read()
    assert "import sqlalchemy" not in text
    assert "from sqlalchemy" not in text
    assert "SessionFactory" not in text


def test_sqlalchemy_module_documents_extra() -> None:
    import cqrs.outbox.sqlalchemy as sa_outbox

    assert sa_outbox.SqlAlchemyOutboxedEventRepository is not None
    source = importlib.util.find_spec("cqrs.outbox.sqlalchemy")
    assert source is not None and source.origin is not None
    text = open(source.origin, encoding="utf-8").read()
    assert "python-cqrs[sqlalchemy]" in text
