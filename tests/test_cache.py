"""Tests for pxaudit.cache.

Strategy
--------
Every test passes ``cache_dir=tmp_path``  (or a subdir of it) so no test
ever touches the real ``.pxaudit_cache/`` directory on disk.

Branch map
----------
read_cache
  ├── path.exists() == False                    [tests 1, 2]
  ├── json.loads succeeds                       [tests 3, 4]
  └── json.JSONDecodeError                      [tests 7, 8, 9]

write_cache
  ├── cache_dir missing → mkdir creates it      [test 5]
  ├── cache_dir exists  → mkdir is a no-op      [tests 3, 4, 11]
  ├── write succeeds                            [tests 3, 4, 6, 11]
  └── write raises OSError (permission denied)  [test 10]
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from pxaudit.cache import read_cache, write_cache

# ---------------------------------------------------------------------------
# 1 & 2 — cache miss
# ---------------------------------------------------------------------------


def test_cache_miss_nonexistent_dir_returns_none(tmp_path: Path) -> None:
    """read_cache must return None when the cache directory does not exist."""
    missing_dir = tmp_path / "no_such_dir"
    result = read_cache("PXD000001", "project", cache_dir=missing_dir)
    assert result is None


def test_cache_miss_file_absent_returns_none(tmp_path: Path) -> None:
    """read_cache must return None when the directory exists but the file does not."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    result = read_cache("PXD000001", "project", cache_dir=cache_dir)
    assert result is None


# ---------------------------------------------------------------------------
# 3 & 4 — successful roundtrip
# ---------------------------------------------------------------------------


def test_write_then_read_dict_returns_identical(tmp_path: Path) -> None:
    """write_cache → read_cache roundtrip must be lossless for a dict payload."""
    payload: dict = {
        "accession": "PXD000001",
        "title": "Hé llo Wörld",  # unicode must survive, not be ASCII-escaped
        "submissionDate": "2012-03-13",
    }
    write_cache("PXD000001", "project", payload, cache_dir=tmp_path)
    result = read_cache("PXD000001", "project", cache_dir=tmp_path)
    assert result == payload
    assert isinstance(result, dict)


def test_write_then_read_list_returns_identical(tmp_path: Path) -> None:
    """write_cache → read_cache roundtrip must be lossless for a list payload."""
    payload: list = [
        {"fileName": "résumé.raw", "fileCategory": {"name": "RAW"}, "fileSize": 1024},
        {"fileName": "result.mzid", "fileCategory": {"name": "RESULT"}, "fileSize": 2048},
    ]
    write_cache("PXD000001", "files", payload, cache_dir=tmp_path)
    result = read_cache("PXD000001", "files", cache_dir=tmp_path)
    assert result == payload
    assert isinstance(result, list)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# 5 — directory creation
# ---------------------------------------------------------------------------


def test_write_creates_missing_directory(tmp_path: Path) -> None:
    """write_cache must create the cache directory (and parents) if absent."""
    deep_dir = tmp_path / "a" / "b" / "cache"
    assert not deep_dir.exists()
    write_cache("PXD000001", "project", {"x": 1}, cache_dir=deep_dir)
    assert deep_dir.exists()
    assert (deep_dir / "PXD000001_project.json").exists()


# ---------------------------------------------------------------------------
# 6 — correct filename
# ---------------------------------------------------------------------------


def test_cache_file_named_correctly(tmp_path: Path) -> None:
    """Cache file must be named exactly ``{accession}_{endpoint}.json``."""
    write_cache("PXD000001", "files", [{"a": 1}], cache_dir=tmp_path)
    expected = tmp_path / "PXD000001_files.json"
    assert expected.exists(), f"Expected {expected} but not found"
    # No extra files must be created
    json_files = list(tmp_path.glob("*.json"))
    assert len(json_files) == 1


# ---------------------------------------------------------------------------
# 7, 8, 9 — corruption recovery
# ---------------------------------------------------------------------------


def _write_corrupt_file(cache_dir: Path, accession: str, endpoint: str) -> Path:
    """Helper: write syntactically invalid JSON to the cache path."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{accession}_{endpoint}.json"
    path.write_text("{this is: not valid json!!}", encoding="utf-8")
    return path


def test_corrupted_json_returns_none(tmp_path: Path) -> None:
    """read_cache must return None (not raise) when the cached file is corrupt."""
    _write_corrupt_file(tmp_path, "PXD000001", "project")
    result = read_cache("PXD000001", "project", cache_dir=tmp_path)
    assert result is None


def test_corrupted_json_deletes_file(tmp_path: Path) -> None:
    """read_cache must delete the corrupted file so the next call re-fetches."""
    corrupt_path = _write_corrupt_file(tmp_path, "PXD000001", "project")
    assert corrupt_path.exists()
    read_cache("PXD000001", "project", cache_dir=tmp_path)
    assert not corrupt_path.exists(), "Corrupted cache file must be deleted"


def test_corrupted_json_logs_warning(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """read_cache must emit a WARNING through the pxaudit.cache logger."""
    _write_corrupt_file(tmp_path, "PXD000001", "project")
    with caplog.at_level(logging.WARNING, logger="pxaudit.cache"):
        read_cache("PXD000001", "project", cache_dir=tmp_path)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.WARNING
    assert "PXD000001_project.json" in caplog.records[0].message


# ---------------------------------------------------------------------------
# 10 — permission error
# ---------------------------------------------------------------------------


def test_write_permission_error_raises(tmp_path: Path) -> None:
    """write_cache must propagate OSError when the cache dir is not writable."""
    cache_dir = tmp_path / "locked"
    cache_dir.mkdir()
    cache_dir.chmod(0o555)  # r-xr-xr-x — no write bit
    try:
        with pytest.raises(OSError):
            write_cache("PXD000001", "project", {"x": 1}, cache_dir=cache_dir)
    finally:
        cache_dir.chmod(0o755)  # restore so tmp_path cleanup does not fail


# ---------------------------------------------------------------------------
# 11 — overwrite
# ---------------------------------------------------------------------------


def test_overwrite_updates_cached_data(tmp_path: Path) -> None:
    """A second write_cache call must replace the first — no stale data."""
    write_cache("PXD000001", "project", {"title": "Old Title"}, cache_dir=tmp_path)
    write_cache("PXD000001", "project", {"title": "New Title"}, cache_dir=tmp_path)
    result = read_cache("PXD000001", "project", cache_dir=tmp_path)
    assert result == {"title": "New Title"}
    # Only one file must exist
    assert len(list(tmp_path.glob("*.json"))) == 1
