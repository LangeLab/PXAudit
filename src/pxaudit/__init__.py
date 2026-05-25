"""Proteomics Exchange metadata audit tool.

Public exports:
  __version__    : package version read from installed metadata.
  _PRIDE_PREFIX  : string prefix that identifies PRIDE accessions ("PXD").
"""

from __future__ import annotations

try:
    from importlib.metadata import version as _metadata_version

    __version__ = _metadata_version("pxaudit")
except Exception:  # pragma: no cover
    __version__ = "0.0.0"

_PRIDE_PREFIX = "PXD"

__all__ = [
    "__version__",
    "_PRIDE_PREFIX",
]
