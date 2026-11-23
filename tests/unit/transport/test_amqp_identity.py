"""AMQP queue identity, header view, and per-group retry state. No broker."""

from __future__ import annotations

import pytest

pytest.importorskip("aio_pika")

from cqrs.message_brokers.amqp import AMQPTransport, _queue_name, _view  # noqa: E402


def test_queue_name_is_unique_per_topic_and_group() -> None:
    assert _queue_name("orders", "workers") == _queue_name("orders", "workers")
    assert _queue_name("orders", "workers") != _queue_name("payments", "workers")
    assert _queue_name("a.b", "c") != _queue_name("a", "b.c")
    assert _queue_name("orders", "workers") != _queue_name("orders", "billing")


def test_user_key_header_is_not_the_ordering_key() -> None:
    headers, key, attempt, wire_id = _view(
        {
            "key": "business-value",
            "cqrs-key": "order-1",
            "cqrs-message-id": "abc",
            "cqrs-delivery-attempt": "2",
            "other": "kept",
        },
    )
    assert key == "order-1"
    assert headers == {"key": "business-value", "other": "kept"}
    assert attempt == 2
    assert wire_id == "abc"


def test_missing_transport_key_stays_none() -> None:
    headers, key, attempt, wire_id = _view({"key": "business-value"})
    assert headers == {"key": "business-value"}
    assert key is None
    assert attempt is None
    assert wire_id is None


def test_attempt_state_is_scoped_to_the_group() -> None:
    transport = AMQPTransport("amqp://guest:guest@localhost:5672/")
    transport._attempts[("orders", "billing", "same")] = 2
    assert transport._attempt_for("orders", "billing", "same", None) == 2
    assert transport._attempt_for("orders", "warehouse", "same", None) == 1
    assert transport._attempt_for("payments", "billing", "same", None) == 1
