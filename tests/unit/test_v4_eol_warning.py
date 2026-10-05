import logging
import warnings

import pytest

from cqrs import _eol


def test_emit_v4_eol_warning_always_emits_deprecation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CQRS_SUPPRESS_V4_EOL_WARNING", raising=False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _eol.emit_v4_eol_warning()

    assert any(issubclass(w.category, DeprecationWarning) for w in caught)
    assert any("python-cqrs 4.x" in str(w.message) for w in caught)


@pytest.mark.parametrize("value", ["1", "true", "yes", "TRUE", " Yes "])
def test_suppress_env_skips_logger_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    value: str,
) -> None:
    monkeypatch.setenv("CQRS_SUPPRESS_V4_EOL_WARNING", value)

    with caplog.at_level(logging.WARNING, logger="cqrs"), warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        _eol.emit_v4_eol_warning()

    assert not [r for r in caplog.records if r.name == "cqrs" and "python-cqrs 4.x" in r.getMessage()]


def test_logger_warning_emitted_when_not_suppressed(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.delenv("CQRS_SUPPRESS_V4_EOL_WARNING", raising=False)

    with caplog.at_level(logging.WARNING, logger="cqrs"), warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        _eol.emit_v4_eol_warning()

    assert any(r.name == "cqrs" and "python-cqrs 4.x" in r.getMessage() for r in caplog.records)
