"""Tests for accession validation and canonicalization."""

from __future__ import annotations

import pytest

from pxaudit.accession import InvalidAccessionError, normalize_accession


@pytest.mark.parametrize(
    ("raw", "canonical"),
    [
        ("PXD000001", "PXD000001"),
        ("pxd000001", "PXD000001"),
        ("  PxD1234567  ", "PXD1234567"),
        ("\tPXD000001\r\n", "PXD000001"),
        ("msv000079514", "MSV000079514"),
        ("jpst-123.abc", "JPST-123.ABC"),
        ("ABC", "ABC"),
        ("A_B", "A_B"),
    ],
)
def test_normalize_accession_accepts_canonical_grammar(raw: str, canonical: str) -> None:
    """Accepted identifiers are trimmed and canonicalized to uppercase."""
    assert normalize_accession(raw) == canonical


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "  ",
        "PX",
        "PXD",
        "PXD12345",
        "PXDABCDEF",
        "PXD000001x",
        "PXD-000001",
        "AB",
        "A" * 65,
        "-MSV000001",
        "MSV000001-",
        "MSV..000001",
        "MSV/000001",
        "MSV\\000001",
        "MSV?000001",
        "MSV#000001",
        "MSV 000001",
        "MSV\t000001",
        "MÜV000001",
        "aßb",
        "\x1cPXD000001",
        "PXD000001\x1c",
    ],
)
def test_normalize_accession_rejects_unsafe_or_ambiguous_values(raw: str) -> None:
    """Malformed, non-ASCII, separator-containing, and PXD-like values are rejected."""
    with pytest.raises(InvalidAccessionError):
        normalize_accession(raw)


def test_normalize_accession_rejects_non_text_runtime_value() -> None:
    """A non-text runtime value receives the domain validation error."""
    with pytest.raises(InvalidAccessionError, match="must be text"):
        normalize_accession(123)  # type: ignore[arg-type]
