"""Tests for pxaudit._output (status/warn/detail/error and color contract)."""

from __future__ import annotations

import io
import sys
from collections.abc import Generator
from pathlib import Path

import pytest

from pxaudit import _output


@pytest.fixture(autouse=True)
def _reset_output() -> Generator[None, None, None]:
    _output.configure(quiet=False, verbose=False, no_color=False)
    yield
    _output.configure(quiet=False, verbose=False, no_color=False)


def test_status_writes_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    """status() emits on stdout only."""
    _output.status("hello-status")
    captured = capsys.readouterr()
    assert "hello-status" in captured.out
    assert captured.err == ""


def test_warn_and_error_write_stderr(capsys: pytest.CaptureFixture[str]) -> None:
    """warn/error emit on stderr only."""
    _output.warn("w")
    _output.error("e")
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "w" in captured.err
    assert "e" in captured.err


def test_detail_default_silent(capsys: pytest.CaptureFixture[str]) -> None:
    """detail() is silent when not verbose."""
    _output.detail("secret")
    assert capsys.readouterr().out == ""


def test_detail_verbose_emits(capsys: pytest.CaptureFixture[str]) -> None:
    """detail() emits when verbose is on."""
    _output.configure(verbose=True)
    _output.detail("verbose-line")
    assert "verbose-line" in capsys.readouterr().out


def test_detail_quiet_suppresses_even_if_verbose(capsys: pytest.CaptureFixture[str]) -> None:
    """Quiet wins over verbose for detail lines."""
    _output.configure(quiet=True, verbose=True)
    _output.detail("nope")
    assert capsys.readouterr().out == ""


def test_status_still_emits_when_quiet(capsys: pytest.CaptureFixture[str]) -> None:
    """Quiet mode still allows status one-liners (gate O3)."""
    _output.configure(quiet=True)
    _output.status("compact")
    assert "compact" in capsys.readouterr().out


def test_warn_not_suppressed_by_quiet(capsys: pytest.CaptureFixture[str]) -> None:
    """Warnings always appear on stderr under quiet."""
    _output.configure(quiet=True)
    _output.warn("still-here")
    assert "still-here" in capsys.readouterr().err


def test_no_color_flag_strips_ansi(monkeypatch: pytest.MonkeyPatch) -> None:
    """--no-color must prevent ANSI even on a TTY."""
    fake_err = io.StringIO()
    fake_err.isatty = lambda: True  # type: ignore[method-assign]
    monkeypatch.setattr(sys, "stderr", fake_err)
    monkeypatch.delenv("NO_COLOR", raising=False)
    _output.configure(no_color=True)
    _output.warn("plain")
    assert "\x1b" not in fake_err.getvalue()
    assert "\033" not in fake_err.getvalue()
    assert "plain" in fake_err.getvalue()


def test_no_color_env_strips_ansi(monkeypatch: pytest.MonkeyPatch) -> None:
    """NO_COLOR env var disables ANSI on a TTY."""
    fake_err = io.StringIO()
    fake_err.isatty = lambda: True  # type: ignore[method-assign]
    monkeypatch.setattr(sys, "stderr", fake_err)
    monkeypatch.setenv("NO_COLOR", "1")
    _output.configure(no_color=False)
    _output.error("plain")
    assert "\x1b" not in fake_err.getvalue()
    assert "plain" in fake_err.getvalue()


def test_non_tty_strips_ansi(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-TTY stderr disables color without --no-color."""
    fake_err = io.StringIO()
    fake_err.isatty = lambda: False  # type: ignore[method-assign]
    monkeypatch.setattr(sys, "stderr", fake_err)
    monkeypatch.delenv("NO_COLOR", raising=False)
    _output.configure(no_color=False)
    _output.warn("plain")
    assert "\x1b" not in fake_err.getvalue()


def test_tty_allows_ansi(monkeypatch: pytest.MonkeyPatch) -> None:
    """TTY + color enabled may include ANSI from click.style."""
    fake_err = io.StringIO()
    fake_err.isatty = lambda: True  # type: ignore[method-assign]
    monkeypatch.setattr(sys, "stderr", fake_err)
    monkeypatch.delenv("NO_COLOR", raising=False)
    _output.configure(no_color=False)
    _output.warn("colored")
    out = fake_err.getvalue()
    assert "colored" in out
    assert "\x1b" in out


def test_color_enabled_false_without_isatty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Streams without isatty() disable color."""

    class NoIsatty(io.StringIO):
        pass

    fake = NoIsatty()
    monkeypatch.setattr(sys, "stdout", fake)
    monkeypatch.delenv("NO_COLOR", raising=False)
    _output.configure(no_color=False)
    assert _output._color_enabled(fake) is False


def test_output_module_has_no_heavy_imports() -> None:
    """_output must not pull network/DB/cache/tier modules."""
    import pxaudit._output as mod

    # Assert the module source does not import heavy dependencies.
    source = Path(mod.__file__).read_text(encoding="utf-8")
    for name in ("cache", "db", "pride_client", "tier_engine", "config"):
        assert f"pxaudit.{name}" not in source
        assert f"import {name}" not in source
