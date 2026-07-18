"""Terminal routing, verbosity, color, and dependency-boundary contracts."""

from __future__ import annotations

import ast
import io
import sys
from itertools import product
from pathlib import Path

import click
import pytest

from pxaudit import _output


class _TTYStream(io.StringIO):
    """In-memory text stream with a controlled terminal identity."""

    def __init__(self, *, is_tty: bool) -> None:
        super().__init__()
        self._is_tty = is_tty

    def isatty(self) -> bool:
        """Return the configured terminal identity."""
        return self._is_tty


@pytest.mark.parametrize(("quiet", "verbose"), tuple(product((False, True), repeat=2)))
def test_primary_messages_use_exact_streams_in_every_output_mode(
    quiet: bool, verbose: bool, capsys: pytest.CaptureFixture[str]
) -> None:
    """Status remains on stdout while warnings and errors remain on stderr."""
    _output.configure(quiet=quiet, verbose=verbose)

    _output.status("status")
    _output.warn("warning")
    _output.error("error")

    captured = capsys.readouterr()
    assert captured.out == "status\n"
    assert captured.err == "warning\nerror\n"


@pytest.mark.parametrize(
    ("quiet", "verbose", "expected"),
    [
        (False, False, ""),
        (False, True, "detail\n"),
        (True, False, ""),
        (True, True, ""),
    ],
)
def test_detail_obeys_complete_quiet_verbose_truth_table(
    quiet: bool,
    verbose: bool,
    expected: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Detail emits only when verbose is enabled and quiet is disabled."""
    _output.configure(quiet=quiet, verbose=verbose)

    _output.detail("detail")

    captured = capsys.readouterr()
    assert captured.out == expected
    assert captured.err == ""


@pytest.mark.parametrize(
    "message", ["", "first\nsecond", "\nleading", "trailing\n", "Unicode: αβγ"]
)
def test_status_preserves_edge_message_content(
    message: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Empty, multiline, and Unicode status messages retain their exact content."""
    _output.status(message)

    captured = capsys.readouterr()
    assert captured.out == f"{message}\n"
    assert captured.err == ""


@pytest.mark.parametrize(
    ("no_color", "no_color_environment", "is_tty"),
    tuple(product((False, True), (None, "", "1", "0"), (False, True))),
)
def test_color_precedence_truth_table(
    no_color: bool,
    no_color_environment: str | None,
    is_tty: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ANSI appears only on a TTY without either color suppression mechanism."""
    stream = _TTYStream(is_tty=is_tty)
    monkeypatch.setattr(sys, "stderr", stream)
    if no_color_environment is None:
        monkeypatch.delenv("NO_COLOR", raising=False)
    else:
        monkeypatch.setenv("NO_COLOR", no_color_environment)
    _output.configure(no_color=no_color)

    _output.warn("message")

    rendered = stream.getvalue()
    expected_color = is_tty and not no_color and not no_color_environment
    assert ("\x1b[" in rendered) is expected_color
    assert click.unstyle(rendered) == "message\n"


def test_emitters_use_exact_documented_tty_styles(monkeypatch: pytest.MonkeyPatch) -> None:
    """Warning, error, and detail use yellow, red, and cyan while status stays plain."""
    stdout = _TTYStream(is_tty=True)
    stderr = _TTYStream(is_tty=True)
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)
    monkeypatch.delenv("NO_COLOR", raising=False)
    _output.configure(verbose=True)

    _output.status("status")
    _output.detail("detail")
    _output.warn("warning")
    _output.error("error")

    assert stdout.getvalue() == "status\n\x1b[36mdetail\x1b[0m\n"
    assert stderr.getvalue() == "\x1b[33mwarning\x1b[0m\n\x1b[31merror\x1b[0m\n"


def test_public_api_is_explicit_and_stable() -> None:
    """The module exports only the five supported terminal operations."""
    assert set(_output.__all__) == {"configure", "detail", "error", "status", "warn"}
    assert len(_output.__all__) == 5


def test_output_module_imports_no_domain_or_storage_modules() -> None:
    """The terminal primitive remains independent of non-presentation modules."""
    source_path = Path(_output.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    imported_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if (node.level and node.module is None) or node.module == "pxaudit":
                imported_modules.update(alias.name for alias in node.names)
            elif node.module is not None:
                imported_modules.add(node.module)
    imported_roots = {
        module.removeprefix("pxaudit.").split(".", 1)[0] for module in imported_modules
    }
    forbidden = {
        "accession",
        "cache",
        "cli",
        "config",
        "db",
        "file_classifier",
        "pride_client",
        "report",
        "tier_engine",
    }

    assert imported_roots.isdisjoint(forbidden)
