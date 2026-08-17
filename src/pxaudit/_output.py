"""Structured terminal output for the pxaudit CLI.

Public API
----------
configure(quiet, verbose, no_color, color)
    Set process-wide output mode from the Click group.
flag_glyph(outcome)
    Render one evidence outcome as a glyph, optionally styled.
style_outcome(outcome)
    Render one failed or unknown outcome label, optionally styled.
style_tier(name)
    Render one qualitative or quantitative tier name, optionally styled.
status(message)
    Primary progress/result line on stdout (always emits; callers choose content).
warn(message)
    Warning on stderr (never suppressed by quiet).
detail(message)
    Extra diagnostics on stdout (verbose only; suppressed when quiet).
error(message)
    Error on stderr (never suppressed by quiet).

Color uses ``click.style`` when enabled. Color is disabled when ``--no-color``
is set, when ``NO_COLOR`` is a non-empty environment variable, or when the
target stream is not a TTY.
"""

from __future__ import annotations

import os
import sys
import typing

import click

__all__ = [
    "configure",
    "detail",
    "error",
    "flag_glyph",
    "status",
    "style_outcome",
    "style_tier",
    "warn",
]

_quiet: bool = False
_verbose: bool = False
_no_color: bool = False
_color: bool | None = None

_FLAG_STYLES: dict[str, tuple[str, bool]] = {
    "passed": ("green", False),
    "failed": ("red", False),
    "unknown": ("yellow", False),
}
_FLAG_GLYPHS = {"passed": "\u2714", "failed": "\u2718", "unknown": "?"}
_TIER_STYLES: dict[str, tuple[str, bool]] = {
    "Diamond": ("bright_cyan", False),
    "Platinum": ("bright_blue", False),
    "Gold": ("yellow", False),
    "Silver": ("bright_white", False),
    "Bronze": ("yellow", True),
    "Raw": ("bright_black", False),
    "None": ("bright_black", True),
    "Unverifiable": ("red", False),
    "Unknown": ("bright_black", True),
    "Quant-Complete": ("bright_green", False),
    "Quant-Ready": ("bright_cyan", False),
    "Partial": ("yellow", False),
    "No Quant": ("bright_black", False),
}


def configure(
    *,
    quiet: bool = False,
    verbose: bool = False,
    no_color: bool = False,
    color: bool | None = None,
) -> None:
    """Set output mode for subsequent ``status`` / ``warn`` / ``detail`` / ``error`` calls.

    Parameters
    ----------
    quiet:
        Suppress ``detail`` only. Callers still use ``status`` for compact one-liners.
        Warnings and errors still emit.
    verbose:
        Enable ``detail`` lines. Mutually exclusive with *quiet* at the CLI layer.
    no_color:
        Force plain text even on a TTY.
    color:
        Optional override for TTY detection. ``None`` uses the stream's terminal identity.
    """
    global _quiet, _verbose, _no_color, _color
    _quiet = quiet
    _verbose = verbose
    _no_color = no_color
    _color = color


def _color_enabled(stream: typing.TextIO) -> bool:
    """Return True when ANSI styling is allowed for *stream*."""
    if _quiet or _no_color:
        return False
    if os.environ.get("NO_COLOR", ""):
        return False
    if _color is not None:
        return _color
    isatty = getattr(stream, "isatty", None)
    return isatty is not None and bool(isatty())


def _style(text: str, style: tuple[str, bool] | None) -> str:
    """Apply a configured style to *text* when stdout permits ANSI output."""
    if style is None or not _color_enabled(sys.stdout):
        return text
    fg, dim = style
    return click.style(text, fg=fg, dim=dim)


def flag_glyph(outcome: object) -> str:
    """Render a passed, failed, or unknown evidence outcome as a glyph."""
    raw = getattr(outcome, "value", outcome)
    if raw in ("passed", True, 1):
        state = "passed"
    elif raw in ("failed", False, 0):
        state = "failed"
    else:
        state = "unknown"
    return _style(_FLAG_GLYPHS[state], _FLAG_STYLES[state])


def style_outcome(outcome: str) -> str:
    """Render a failed or unknown outcome label with the shared flag style."""
    return _style(outcome, _FLAG_STYLES.get(outcome.casefold()))


def style_tier(name: str) -> str:
    """Render a qualitative or quantitative tier name with its restrained color."""
    return _style(name, _TIER_STYLES.get(name.strip()))


def _emit(stream: typing.TextIO, message: str, *, fg: str | None = None) -> None:
    """Write *message* to *stream*, optionally colored."""
    if fg is not None and _color_enabled(stream):
        message = click.style(message, fg=fg)
    click.echo(message, file=stream)


def status(message: str) -> None:
    """Print a primary status line to stdout.

    Callers choose compact vs. verbose content. Quiet mode still uses ``status``
    for the single required summary line; it must not emit checklist bodies.
    """
    _emit(sys.stdout, message)


def warn(message: str) -> None:
    """Print a warning to stderr. Never suppressed by quiet mode."""
    _emit(sys.stderr, message, fg="yellow")


def detail(message: str) -> None:
    """Print a verbose detail line to stdout. No-op unless verbose mode is on."""
    if _quiet or not _verbose:
        return
    _emit(sys.stdout, message, fg="cyan")


def error(message: str) -> None:
    """Print an error to stderr. Never suppressed by quiet mode."""
    _emit(sys.stderr, message, fg="red")
