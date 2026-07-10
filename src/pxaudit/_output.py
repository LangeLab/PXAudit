"""Structured terminal output for the pxaudit CLI.

Public API
----------
configure(quiet, verbose, no_color)
    Set process-wide output mode from the Click group.
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
    "status",
    "warn",
]

_quiet: bool = False
_verbose: bool = False
_no_color: bool = False


def configure(*, quiet: bool = False, verbose: bool = False, no_color: bool = False) -> None:
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
    """
    global _quiet, _verbose, _no_color
    _quiet = quiet
    _verbose = verbose
    _no_color = no_color


def _color_enabled(stream: typing.TextIO) -> bool:
    """Return True when ANSI styling is allowed for *stream*."""
    if _no_color:
        return False
    if os.environ.get("NO_COLOR", ""):
        return False
    isatty = getattr(stream, "isatty", None)
    return isatty is not None and bool(isatty())


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
