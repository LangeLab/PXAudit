"""Tests for accession validation and canonicalization."""

from __future__ import annotations

import string

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
        ("a.-_z", "A.-_Z"),
        ("A" + "_" * 62 + "Z", "A" + "_" * 62 + "Z"),
        ("PXD" + "0" * 61, "PXD" + "0" * 61),
    ],
)
def test_normalize_accession_accepts_canonical_grammar(raw: str, canonical: str) -> None:
    """Accepted identifiers are trimmed and canonicalized to uppercase."""
    assert normalize_accession(raw) == canonical
    assert normalize_accession(canonical) == canonical


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
        "PXD" + "0" * 62,
        "AB",
        "A" * 65,
        "-MSV000001",
        "MSV000001-",
        "MSV..000001",
        "MÜV000001",
        "aßb",
        "\x1cPXD000001",
        "PXD000001\x1c",
    ],
)
def test_normalize_accession_rejects_unsafe_or_ambiguous_values(raw: str) -> None:
    """Malformed, non-ASCII, unsafe-character, and PXD-like values are rejected."""
    with pytest.raises(InvalidAccessionError):
        normalize_accession(raw)


def test_ascii_character_positions_follow_exact_grammars() -> None:
    """An ASCII sweep enforces the PXD suffix and partner position grammars."""
    endpoint_characters = frozenset(string.ascii_letters + string.digits)
    middle_characters = endpoint_characters | frozenset("._-")

    for codepoint in range(128):
        character = chr(codepoint)
        cases = (
            (f"A{character}B", character in middle_characters),
            (f"{character}AB", character in endpoint_characters),
            (f"AB{character}", character in endpoint_characters),
            (f"PXD00000{character}", character in string.digits),
        )
        for raw, is_valid in cases:
            if is_valid:
                assert normalize_accession(raw) == raw.upper()
            else:
                with pytest.raises(InvalidAccessionError):
                    normalize_accession(raw)


def test_normalize_accession_rejects_non_text_runtime_values() -> None:
    """Non-text runtime values receive the domain validation error."""
    values: tuple[object, ...] = (None, 123, b"PXD000001", [], {})
    for value in values:
        with pytest.raises(InvalidAccessionError, match="must be text"):
            normalize_accession(value)  # type: ignore[arg-type]
