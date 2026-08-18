"""Expose PXAudit version, public API helpers, and shared accession constants."""

from __future__ import annotations

import importlib.metadata

try:
    __version__ = importlib.metadata.version("pxaudit")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

_PRIDE_PREFIX = "PXD"

from pxaudit.api import (  # noqa: E402
    AuditResult,
    FileClass,
    FlagOutcome,
    audit_accessions,
    check_accession,
    compute_audit,
)

__all__ = [
    "__version__",
    "_PRIDE_PREFIX",
    "AuditResult",
    "FileClass",
    "FlagOutcome",
    "audit_accessions",
    "check_accession",
    "compute_audit",
]
