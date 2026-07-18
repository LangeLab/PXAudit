"""Accession validation and canonicalization for PXAudit entry points."""

from __future__ import annotations

import re

from pxaudit import _PRIDE_PREFIX

_PRIDE_ACCESSION = re.compile(r"PXD[0-9]{6,}")
_PARTNER_ACCESSION = re.compile(r"[A-Z0-9][A-Z0-9._-]{1,62}[A-Z0-9]")
_SURROUNDING_WHITESPACE = " \t\r\n"
_MAX_ACCESSION_LENGTH = 64


class InvalidAccessionError(ValueError):
    """Raised when an accession cannot be represented safely and unambiguously."""


def normalize_accession(value: str) -> str:
    """Validate and return the canonical uppercase accession.

    Surrounding whitespace is removed. PRIDE accessions require ``PXD`` followed by at
    least six decimal digits. All identifiers are limited to 64 characters. Other
    repository identifiers may contain 3 to 64 ASCII letters, digits, dots, underscores,
    and hyphens, but must begin and end with an alphanumeric character and may not
    contain ``..``.

    Parameters
    ----------
    value:
        User-supplied accession.

    Returns
    -------
    str
        Canonical uppercase accession.

    Raises
    ------
    InvalidAccessionError
        If the accession does not satisfy the accepted grammar.
    """
    if not isinstance(value, str):
        raise InvalidAccessionError("accession must be text")

    trimmed = value.strip(_SURROUNDING_WHITESPACE)
    if len(trimmed) > _MAX_ACCESSION_LENGTH:
        raise InvalidAccessionError("accessions must be at most 64 characters")
    if not trimmed.isascii():
        raise InvalidAccessionError("accessions must contain only safe ASCII characters")

    canonical = trimmed.upper()
    if canonical.startswith(_PRIDE_PREFIX):
        if _PRIDE_ACCESSION.fullmatch(canonical):
            return canonical
        raise InvalidAccessionError("PRIDE accessions require PXD followed by at least six digits")

    if ".." in canonical or _PARTNER_ACCESSION.fullmatch(canonical) is None:
        raise InvalidAccessionError(
            "accessions must be 3-64 safe ASCII characters with alphanumeric endpoints"
        )
    return canonical


__all__ = ["InvalidAccessionError", "normalize_accession"]
