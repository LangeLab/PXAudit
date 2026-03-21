"""Local JSON file cache for PRIDE API responses.

Cache files live at ``{cache_dir}/{accession}_{endpoint}.json``.
The default cache directory is ``.pxaudit_cache/`` relative to the working
directory.  Tests always pass ``cache_dir`` explicitly so they never touch the
real filesystem outside ``tmp_path``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

_log = logging.getLogger(__name__)

_DEFAULT_CACHE_DIR = Path(".pxaudit_cache")


def read_cache(
    accession: str,
    endpoint: str,
    *,
    cache_dir: Path = _DEFAULT_CACHE_DIR,
) -> dict | list | None:
    """Return the cached JSON payload for *accession* + *endpoint*, or ``None``.

    Returns ``None`` on a cache miss (directory or file absent) and on a
    corrupted file.  Corruption recovery: the bad file is deleted so the next
    call triggers a fresh network fetch rather than repeatedly failing.

    Parameters
    ----------
    accession:
        PRIDE accession string, e.g. ``"PXD000001"``.
    endpoint:
        Short endpoint label used in the filename, e.g. ``"project"`` or
        ``"files"``.
    cache_dir:
        Root cache directory.  Defaults to ``.pxaudit_cache/`` in the current
        working directory.
    """
    path = cache_dir / f"{accession}_{endpoint}.json"
    if not path.exists():
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
        Root cache directory.  Defaults to ``.pxaudit_cache/`` in the current
        working directory.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{accession}_{endpoint}.json"
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
