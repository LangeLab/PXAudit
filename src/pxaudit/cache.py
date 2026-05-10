"""Local JSON file cache for PRIDE API responses.

Cache files live at ``{cache_dir}/{accession}_{endpoint}.json``.
The default cache directory is ``~/.pxaudit_cache/``.
Tests always pass ``cache_dir`` explicitly so they never touch the
real filesystem outside ``tmp_path``.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

_log = logging.getLogger(__name__)

_DEFAULT_CACHE_DIR = Path.home() / ".pxaudit_cache"
_DEFAULT_TTL: float = 7 * 24 * 60 * 60  # 7 days in seconds


def read_cache(
    accession: str,
    endpoint: str,
    *,
    cache_dir: Path = _DEFAULT_CACHE_DIR,
    max_age: float | None = _DEFAULT_TTL,
) -> dict | list | None:
    """Return the cached JSON payload for *accession* + *endpoint*, or ``None``.

    Returns ``None`` on a cache miss (directory or file absent), on a
    corrupted file, or when the cached file exceeds *max_age*.
    Corruption recovery: the bad file is deleted so the next call triggers
    a fresh network fetch rather than repeatedly failing.

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
            if age > max_age:
                _log.info(
                    "Cache file %s is %.1f s old (TTL=%.0f s) — stale, re-fetching",
                    path,
                    age,
                    max_age,
                )
                path.unlink(missing_ok=True)
                return None
        except OSError:
            return None

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        _log.warning("Corrupted cache file %s — deleting and returning None", path)
        path.unlink(missing_ok=True)
        return None


def write_cache(
    accession: str,
    endpoint: str,
    data: dict | list,
    *,
    cache_dir: Path = _DEFAULT_CACHE_DIR,
) -> None:
    """Serialise *data* to ``{cache_dir}/{accession}_{endpoint}.json``.

    The write is atomic on POSIX systems: data is first written to a
    ``.tmp`` file in the same directory, then atomically renamed to the
    final path via ``os.replace()``.  If the process is interrupted
    mid-write, only the ``.tmp`` file is lost — the final file remains
    untouched (or absent on first write).

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
    tmp_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_path, path)
