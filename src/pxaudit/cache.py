"""Local JSON file cache for PRIDE API responses.

Cache files live at ``{cache_dir}/{accession}_{endpoint}.json``.
The default cache directory is ``~/.pxaudit_cache/``.
The ``cache_dir`` parameter is configurable to allow custom
cache locations and to isolate filesystem access in tests.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)

_DEFAULT_CACHE_DIR = Path.home() / ".pxaudit_cache"
_DEFAULT_TTL: float = 7 * 24 * 60 * 60  # 7 days in seconds
_CACHE_VERSION: int = 1

__all__ = [
    "read_cache",
    "read_cache_stale",
    "write_cache",
]


def _unwrap_cache(raw: Any, path: Path) -> dict | list | None:
    """Extract the payload from a cache file, handling versioned and legacy formats.

    Returns ``None`` and deletes *path* on version mismatch or corrupt structure.
    """
    if isinstance(raw, dict) and "cache_version" in raw:
        if raw["cache_version"] == _CACHE_VERSION:
            data = raw.get("data")
            if isinstance(data, (dict, list)):
                return data
        _log.warning(
            "Cache file %s has unknown version %r; deleting and re-fetching",
            path,
            raw.get("cache_version"),
        )
        path.unlink(missing_ok=True)
        return None
    # Legacy format (pre-v0.3.0): plain dict or list without version wrapper.
    if isinstance(raw, (dict, list)):
        return raw
    return None


def read_cache(
    accession: str,
    endpoint: str,
    *,
    cache_dir: Path = _DEFAULT_CACHE_DIR,
    max_age: float | None = _DEFAULT_TTL,
) -> dict | list | None:
    """Return the cached JSON payload for *accession* + *endpoint*, or ``None``.

    Returns ``None`` on a cache miss (directory or file absent), on a
    corrupted file, on an unknown cache version, or when the cached file
    exceeds *max_age*.  Corruption recovery: the bad file is deleted so
    the next call triggers a fresh network fetch rather than repeatedly
    failing.

    When *max_age* is ``None``, no TTL check is performed and the cache entry
    is served indefinitely (subject to corruption recovery).

    Parameters
    ----------
    accession:
        PRIDE accession string, e.g. ``"PXD000001"``.
    endpoint:
        Short endpoint label used in the filename, e.g. ``"project"`` or
        ``"files"``.
    cache_dir:
        Root cache directory.  Defaults to ``~/.pxaudit_cache/``.
    max_age:
        Maximum age of the cached file in seconds before it is considered
        stale.  When a file exceeds this age it is deleted and ``None`` is
        returned, triggering a re-fetch.  Defaults to 7 days (604800 s).
        Pass ``None`` to disable TTL checking.
    """
    path = cache_dir / f"{accession}_{endpoint}.json"
    if not path.exists():
        return None

    if max_age is not None:
        try:
            age = time.time() - path.stat().st_mtime
            if age >= max_age:
                _log.info(
                    "Cache file %s is %.1f s old (TTL=%.0f s); stale, will re-fetch",
                    path,
                    age,
                    max_age,
                )
                return None  # Stale: keep file for fallback, caller will re-fetch.
        except OSError:  # pragma: no cover
            return None

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        _log.warning("Corrupted cache file %s, deleting and returning None", path)
        path.unlink(missing_ok=True)
        return None

    return _unwrap_cache(raw, path)


def read_cache_stale(
    accession: str,
    endpoint: str,
    *,
    cache_dir: Path = _DEFAULT_CACHE_DIR,
) -> tuple[dict | list, float] | tuple[None, None]:
    """Read a cached file regardless of age, returning ``(data, age_seconds)``.

    Returns ``(None, None)`` if the file is absent or corrupted.
    Unlike :func:`read_cache`, this does **not** enforce TTL; it is intended
    as a fallback when the live API is unreachable.

    Parameters
    ----------
    accession:
        PRIDE accession string, e.g. ``"PXD000001"``.
    endpoint:
        Short endpoint label used in the filename, e.g. ``"project"`` or
        ``"files"``.
    cache_dir:
        Root cache directory.  Defaults to ``~/.pxaudit_cache/``.
    """
    path = cache_dir / f"{accession}_{endpoint}.json"
    if not path.exists():
        return None, None

    try:
        age = time.time() - path.stat().st_mtime
    except OSError:  # pragma: no cover
        return None, None

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        _log.warning("Corrupted cache file %s, deleting and returning None", path)
        path.unlink(missing_ok=True)
        return None, None

    data = _unwrap_cache(raw, path)
    if data is None:
        return None, None
    return data, age


def write_cache(
    accession: str,
    endpoint: str,
    data: dict | list,
    *,
    cache_dir: Path = _DEFAULT_CACHE_DIR,
) -> None:
    """Serialise *data* to ``{cache_dir}/{accession}_{endpoint}.json``.

    The payload is wrapped in a version header ``{"cache_version": 1, "data": ...}``
    so that future format changes can be detected.  The write is atomic on POSIX
    systems: data is first written to a ``.tmp`` file in the same directory, then
    atomically renamed to the final path via ``os.replace()``.

    The cache directory is created (including all parents) on first write.
    Any ``OSError`` from the filesystem (e.g. permission denied) propagates
    to the caller unchanged.

    Parameters
    ----------
    accession:
        PRIDE accession string, e.g. ``"PXD000001"``.
    endpoint:
        Short endpoint label used in the filename, e.g. ``"project"`` or
        ``"files"``.
    data:
        Parsed JSON payload to cache (dict for project, list for files).
    cache_dir:
        Root cache directory.  Defaults to ``~/.pxaudit_cache/``.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{accession}_{endpoint}.json"
    tmp_path = path.with_suffix(".tmp")
    payload = {"cache_version": _CACHE_VERSION, "data": data}
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_path, path)
