"""Expose the PXAudit package version and shared PRIDE accession prefix."""

from __future__ import annotations

import importlib.metadata

try:
    __version__ = importlib.metadata.version("pxaudit")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

_PRIDE_PREFIX = "PXD"

__all__ = [
    "__version__",
    "_PRIDE_PREFIX",
]
