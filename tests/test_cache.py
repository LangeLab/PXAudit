"""Cache ownership, containment, compatibility, and atomic-write tests.

All test artifacts live under ``tmp_path``. Dangerous-root tests resolve broad locations only
far enough to prove refusal; the suite never inventories or mutates the real user cache.
"""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Protocol
from unittest.mock import MagicMock, patch

import pytest

import pxaudit.cache as cache_module
from pxaudit.cache import (
    _DEFAULT_TTL,
    CacheKeyError,
    CacheSafetyError,
    CacheWriteError,
    clear_cache,
    inspect_cache,
    read_cache,
    read_cache_stale,
    validate_cache_root,
    write_cache,
)


class _Barrier(Protocol):
    """Minimal process-barrier interface used by the concurrency test."""

    def wait(self) -> int:
        """Wait for the peer process."""

        ...


def _write_cache_in_process(
    cache_dir: str,
    value: int,
    barrier: _Barrier,
) -> None:
    """Write one cache value after both test processes are ready."""
    barrier.wait()
    write_cache("PXD000001", "project", {"value": value}, cache_dir=cache_dir)


# ---------------------------------------------------------------------------
# 1 & 2 : cache miss
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
# 3 & 4 : successful roundtrip
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
# 5 : directory creation
# ---------------------------------------------------------------------------


def test_write_creates_missing_directory(tmp_path: Path) -> None:
    """write_cache must create the cache directory (and parents) if absent."""
    deep_dir = tmp_path / "a" / "b" / "cache"
    assert not deep_dir.exists()
    write_cache("PXD000001", "project", {"x": 1}, cache_dir=deep_dir)
    assert deep_dir.exists()
    assert (deep_dir / "PXD000001_project.json").exists()


# ---------------------------------------------------------------------------
# 6 : correct filename
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
# 7, 8, 9 : corruption recovery
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


def test_corrupted_json_preserves_unowned_file(tmp_path: Path) -> None:
    """A corrupt file remains untouched because cache ownership is unproven."""
    corrupt_path = _write_corrupt_file(tmp_path, "PXD000001", "project")
    assert corrupt_path.exists()
    read_cache("PXD000001", "project", cache_dir=tmp_path)
    assert corrupt_path.exists()


def test_corrupted_json_logs_warning(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """read_cache must emit a WARNING through the pxaudit.cache logger."""
    _write_corrupt_file(tmp_path, "PXD000001", "project")
    with caplog.at_level(logging.WARNING, logger="pxaudit.cache"):
        read_cache("PXD000001", "project", cache_dir=tmp_path)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.WARNING
    assert "PXD000001_project.json" in caplog.records[0].message


# ---------------------------------------------------------------------------
# 10 : permission error
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="chmod does not prevent writes on Windows")
def test_write_permission_error_raises(tmp_path: Path) -> None:
    """write_cache must propagate OSError when the cache dir is not writable."""
    cache_dir = tmp_path / "locked"
    cache_dir.mkdir()
    cache_dir.chmod(0o555)  # r-xr-xr-x : no write bit
    try:
        with pytest.raises(CacheWriteError):
            write_cache("PXD000001", "project", {"x": 1}, cache_dir=cache_dir)
    finally:
        cache_dir.chmod(0o755)  # restore so tmp_path cleanup does not fail


# ---------------------------------------------------------------------------
# 11 : overwrite
# ---------------------------------------------------------------------------


def test_overwrite_updates_cached_data(tmp_path: Path) -> None:
    """A second write_cache call must replace the first : no stale data."""
    write_cache("PXD000001", "project", {"title": "Old Title"}, cache_dir=tmp_path)
    write_cache("PXD000001", "project", {"title": "New Title"}, cache_dir=tmp_path)
    result = read_cache("PXD000001", "project", cache_dir=tmp_path)
    assert result == {"title": "New Title"}
    # Only one file must exist
    assert len(list(tmp_path.glob("*.json"))) == 1


# ---------------------------------------------------------------------------
# 12 & 13 : TTL: fresh cache is served
# ---------------------------------------------------------------------------


def test_ttl_fresh_cache_served(tmp_path: Path) -> None:
    """A recently-written cache file must be served when within the TTL window."""
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=3600)
    assert result == {"key": "value"}


def test_ttl_fresh_cache_disabled_with_none(tmp_path: Path) -> None:
    """Passing max_age=None must disable TTL and serve even old cache."""
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    old = time.time() - 999999
    os.utime(path, (old, old))
    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=None)
    assert result == {"key": "value"}


# ---------------------------------------------------------------------------
# 14 & 15 : TTL: stale cache returns None, kept for fallback
# ---------------------------------------------------------------------------


def test_ttl_stale_cache_returns_none_keeps_file(tmp_path: Path) -> None:
    """A cache file older than max_age must return None but keep the file for stale fallback."""
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    old = time.time() - 7200
    os.utime(path, (old, old))
    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=3600)
    assert result is None
    assert path.exists(), "stale file must be kept for fallback"


def test_ttl_default_constant_is_seven_days() -> None:
    """_DEFAULT_TTL must equal 7 * 24 * 60 * 60 seconds."""
    assert _DEFAULT_TTL == 7 * 24 * 60 * 60


# ---------------------------------------------------------------------------
# TTL boundary tests : st_mtime at / before / after threshold
# ---------------------------------------------------------------------------


@patch("time.time")
def test_ttl_boundary_exactly_at_max_age_is_stale(mock_time: MagicMock, tmp_path: Path) -> None:
    """age == max_age is stale (code uses >=, not >).

    ``time.time`` is patched to return a fixed timestamp so the age
    calculation in ``read_cache`` is deterministic.
    """
    fixed_mtime = 1_000_000.0
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    os.utime(path, (fixed_mtime, fixed_mtime))
    mock_time.return_value = fixed_mtime + 3600  # age == max_age exactly
    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=3600)
    assert result is None


@patch("time.time")
def test_ttl_boundary_one_second_before_is_fresh(mock_time: MagicMock, tmp_path: Path) -> None:
    """age == max_age - 1 is still within TTL and served."""
    fixed_mtime = 1_000_000.0
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    os.utime(path, (fixed_mtime, fixed_mtime))
    mock_time.return_value = fixed_mtime + 3599  # age == max_age - 1
    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=3600)
    assert result == {"key": "value"}


@patch("time.time")
def test_ttl_boundary_one_second_after_is_stale(mock_time: MagicMock, tmp_path: Path) -> None:
    """age == max_age + 1 is stale and remains available for fallback."""
    fixed_mtime = 1_000_000.0
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    os.utime(path, (fixed_mtime, fixed_mtime))
    mock_time.return_value = fixed_mtime + 3601  # age == max_age + 1
    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=3600)
    assert result is None
    assert path.exists(), "stale file must be kept for fallback"


# ---------------------------------------------------------------------------
# Refresh bypass : max_age=0 forces stale on any cache
# ---------------------------------------------------------------------------


def test_ttl_zero_max_age_bypasses_fresh_cache(tmp_path: Path) -> None:
    """max_age=0 treats any cache as stale (equivalent to --refresh at CLI)."""
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    assert path.exists()
    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=0)
    assert result is None
    assert path.exists(), "stale file must be kept for fallback"
    # Fresh write after bypass must succeed
    write_cache("PXD000001", "project", {"new": "data"}, cache_dir=tmp_path)
    result = read_cache("PXD000001", "project", cache_dir=tmp_path)
    assert result == {"new": "data"}


# ---------------------------------------------------------------------------
# 16, 17, 18 : atomic write via tmp + os.replace
# ---------------------------------------------------------------------------


def test_atomic_write_leaves_no_tmp_file(tmp_path: Path) -> None:
    """A successful atomic write leaves no temporary entry behind."""
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    json_path = tmp_path / "PXD000001_project.json"
    assert json_path.exists()
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_write_interrupted_tmp_does_not_harm_final(tmp_path: Path) -> None:
    """If os.replace is never called (simulated crash), the .json file is untouched."""
    payload = {"version": 1}
    write_cache("PXD000001", "project", payload, cache_dir=tmp_path)
    json_path = tmp_path / "PXD000001_project.json"
    original_mtime = json_path.stat().st_mtime

    tmp_path_candidate = tmp_path / ".PXD000001_project.json.interrupted.tmp"
    tmp_path_candidate.write_text('{"corrupt": true}', encoding="utf-8")
    assert json_path.exists()
    assert json_path.stat().st_mtime == original_mtime
    result = read_cache("PXD000001", "project", cache_dir=tmp_path)
    assert result == payload


@pytest.mark.skipif(sys.platform == "win32", reason="chmod does not prevent writes on Windows")
def test_atomic_write_oserror_on_tmp_propagates(tmp_path: Path) -> None:
    """If writing the .tmp file fails, the error must propagate (no .json created)."""
    cache_dir = tmp_path / "locked"
    cache_dir.mkdir()
    cache_dir.chmod(0o444)  # read-only
    with pytest.raises((CacheSafetyError, CacheWriteError)):
        write_cache("PXD000001", "project", {"x": 1}, cache_dir=cache_dir)
    cache_dir.chmod(0o755)


# ---------------------------------------------------------------------------
# Cache version header tests
# ---------------------------------------------------------------------------


def test_cache_version_header_present_on_write(tmp_path: Path) -> None:
    """Written cache file must contain a version header."""
    data = {"title": "test"}
    write_cache("PXD000001", "project", data, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    raw = json.loads(path.read_text())
    assert raw["cache_version"] == 2
    assert raw["cache_owner"] == "pxaudit"
    assert raw["accession"] == "PXD000001"
    assert raw["endpoint"] == "project"
    assert raw["data"] == data


def test_cache_version_round_trip(tmp_path: Path) -> None:
    """Write then read must return identical data through the version wrapper."""
    data = {"title": "roundtrip"}
    write_cache("PXD000001", "project", data, cache_dir=tmp_path)
    result = read_cache("PXD000001", "project", cache_dir=tmp_path)
    assert result == data


def test_cache_legacy_format_still_readable(tmp_path: Path) -> None:
    """Plain JSON (no version header) from older versions must still be readable."""
    data = {"title": "legacy"}
    path = tmp_path / "PXD000001_project.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=None)
    assert result == data


def test_cache_version_one_still_readable(tmp_path: Path) -> None:
    """Version-1 envelopes remain readable during the compatibility window."""
    data = {"title": "version one"}
    path = tmp_path / "PXD000001_project.json"
    path.write_text(json.dumps({"cache_version": 1, "data": data}), encoding="utf-8")

    assert read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=None) == data


def test_cache_unknown_version_returns_none(tmp_path: Path) -> None:
    """Unknown cache versions are misses and remain untouched."""
    payload = {"cache_version": 999, "data": {"x": 1}}
    path = tmp_path / "PXD000001_project.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=None)
    assert result is None
    assert path.exists()


def test_cache_unknown_version_is_preserved(tmp_path: Path) -> None:
    """Unknown cache versions are not deleted without ownership proof."""
    payload = {"cache_version": 999, "data": {"x": 1}}
    path = tmp_path / "PXD000001_project.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=None)
    assert path.exists()


def test_cache_bad_data_structure_returns_none(tmp_path: Path) -> None:
    """Cache file with a version header but missing data key returns None."""
    payload = {"cache_version": 1, "meta": "no data key"}
    path = tmp_path / "PXD000001_project.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=None)
    assert result is None


def test_cache_scalar_value_returns_none(tmp_path: Path) -> None:
    """Cache file containing a bare string or number (not dict/list) returns None."""
    path = tmp_path / "PXD000001_project.json"
    path.write_text('"just a string"', encoding="utf-8")
    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=None)
    assert result is None


# ---------------------------------------------------------------------------
# read_cache_stale tests
# ---------------------------------------------------------------------------


def test_read_cache_stale_returns_data_and_age(tmp_path: Path) -> None:
    """read_cache_stale must return stale data and its age in seconds."""
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    old = time.time() - 7200
    os.utime(path, (old, old))

    data, age = read_cache_stale("PXD000001", "project", cache_dir=tmp_path)
    assert data == {"key": "value"}
    assert age is not None
    assert age > 7000  # roughly 7200 s, allow slight clock drift


def test_read_cache_stale_missing_file_returns_none_none(tmp_path: Path) -> None:
    """read_cache_stale on absent file must return (None, None)."""
    data, age = read_cache_stale("PXD000001", "project", cache_dir=tmp_path)
    assert data is None
    assert age is None


def test_read_cache_stale_corrupt_file_returns_none_none(tmp_path: Path) -> None:
    """Stale reads ignore corrupt JSON without deleting it."""
    path = tmp_path / "PXD000001_project.json"
    path.write_text("{bad json", encoding="utf-8")
    data, age = read_cache_stale("PXD000001", "project", cache_dir=tmp_path)
    assert data is None
    assert age is None
    assert path.exists()


def test_read_cache_stale_unknown_version_returns_none_none(tmp_path: Path) -> None:
    """Stale reads preserve unknown cache versions."""
    payload = {"cache_version": 999, "data": {"x": 1}}
    path = tmp_path / "PXD000001_project.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    data, age = read_cache_stale("PXD000001", "project", cache_dir=tmp_path)
    assert data is None
    assert age is None
    assert path.exists()


@pytest.mark.parametrize(
    "accession",
    ["../outside", "PXD/000001", "PXD\\000001", "PXD\n000001", "PXD..000001", " PXD1"],
)
def test_cache_accession_rejects_unsafe_components(tmp_path: Path, accession: str) -> None:
    """Unsafe accession components cannot reach any cache filesystem operation."""
    with pytest.raises(CacheKeyError):
        read_cache(accession, "project", cache_dir=tmp_path)
    with pytest.raises(CacheKeyError):
        read_cache_stale(accession, "project", cache_dir=tmp_path)
    with pytest.raises(CacheKeyError):
        write_cache(accession, "project", {}, cache_dir=tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_cache_endpoint_rejects_unknown_value(tmp_path: Path) -> None:
    """Only project and files endpoints can form cache paths."""
    with pytest.raises(CacheKeyError):
        write_cache("PXD000001", "../project", {}, cache_dir=tmp_path)


def test_traversal_key_cannot_touch_parent_directory(tmp_path: Path) -> None:
    """A traversal-shaped accession cannot create or read a parent-directory target."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    outside = tmp_path / "escaped_project.json"

    with pytest.raises(CacheKeyError):
        write_cache("../escaped", "project", {}, cache_dir=cache_dir)
    with pytest.raises(CacheKeyError):
        read_cache("../escaped", "project", cache_dir=cache_dir)
    assert not outside.exists()


@pytest.mark.parametrize("endpoint,data", [("project", []), ("files", {})])
def test_write_rejects_endpoint_payload_mismatch(
    tmp_path: Path, endpoint: str, data: dict | list
) -> None:
    """Project and files entries retain their required outer JSON shapes."""
    with pytest.raises(CacheWriteError):
        write_cache("PXD000001", endpoint, data, cache_dir=tmp_path)


def test_version_two_identity_mismatch_is_cache_miss(tmp_path: Path) -> None:
    """A version-2 envelope must agree with its filename identity."""
    write_cache("PXD000001", "project", {"title": "owned"}, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    raw = json.loads(path.read_text())
    raw["accession"] = "PXD000002"
    path.write_text(json.dumps(raw), encoding="utf-8")

    assert read_cache("PXD000001", "project", cache_dir=tmp_path) is None
    assert path.exists()


def test_version_one_payload_shape_mismatch_is_cache_miss(tmp_path: Path) -> None:
    """A legacy envelope with the wrong endpoint shape is rejected."""
    path = tmp_path / "PXD000001_project.json"
    path.write_text(json.dumps({"cache_version": 1, "data": []}), encoding="utf-8")

    assert read_cache("PXD000001", "project", cache_dir=tmp_path) is None


def test_unicode_decode_failure_is_cache_miss_and_preserved(tmp_path: Path) -> None:
    """Invalid UTF-8 cache bytes are ignored without deletion."""
    path = tmp_path / "PXD000001_project.json"
    path.write_bytes(b"\xff\xfe")

    assert read_cache("PXD000001", "project", cache_dir=tmp_path) is None
    assert path.read_bytes() == b"\xff\xfe"


def test_read_oserror_is_cache_miss(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Filesystem read failures degrade to cache misses."""
    write_cache("PXD000001", "project", {}, cache_dir=tmp_path)
    monkeypatch.setattr("pxaudit.cache.os.open", MagicMock(side_effect=OSError("denied")))

    assert read_cache("PXD000001", "project", cache_dir=tmp_path) is None


def test_read_close_oserror_is_cache_miss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A descriptor-close failure remains a cache miss and is logged."""
    write_cache("PXD000001", "project", {}, cache_dir=tmp_path)
    monkeypatch.setattr("pxaudit.cache.os.open", MagicMock(return_value=-1))
    monkeypatch.setattr("pxaudit.cache.os.close", MagicMock(side_effect=OSError("denied")))

    with caplog.at_level(logging.WARNING, logger="pxaudit.cache"):
        assert read_cache("PXD000001", "project", cache_dir=tmp_path) is None
    assert "Could not close unreadable cache entry" in caplog.text


def test_read_non_regular_cache_path_is_miss(tmp_path: Path) -> None:
    """A directory named like a cache entry is never opened as cache data."""
    (tmp_path / "PXD000001_project.json").mkdir()

    assert read_cache("PXD000001", "project", cache_dir=tmp_path) is None


@pytest.mark.skipif(
    not hasattr(os, "mkfifo"),
    reason="Issue #18: named pipes are unavailable on this platform",
)
def test_inspect_cache_does_not_block_on_named_pipe(tmp_path: Path) -> None:
    """A cache-shaped named pipe is ignored without waiting for a writer."""
    pipe = tmp_path / "PXD000001_project.json"
    os.mkfifo(pipe)

    inventory = inspect_cache(tmp_path)

    assert inventory.entries == ()
    assert inventory.ignored == 1


def test_stale_age_never_reports_negative(tmp_path: Path) -> None:
    """A future filesystem timestamp produces a zero cache age."""
    write_cache("PXD000001", "project", {}, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    future = time.time() + 60
    os.utime(path, (future, future))

    data, age = read_cache_stale("PXD000001", "project", cache_dir=tmp_path)
    assert data == {}
    assert age == 0.0


def test_read_from_dangerous_root_is_cache_miss(tmp_path: Path) -> None:
    """Ordinary reads do not access a configured filesystem root."""
    assert read_cache("PXD000001", "project", cache_dir=Path(tmp_path.anchor)) is None


@pytest.mark.parametrize("cache_dir", ["", Path(tempfile.gettempdir())])
def test_validate_cache_root_rejects_blank_and_temp_root(
    cache_dir: str | Path,
) -> None:
    """Blank and shared temporary roots cannot be cache directories."""
    with pytest.raises(CacheSafetyError):
        validate_cache_root(cache_dir)


def test_validate_cache_root_rejects_home_and_ancestor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The home directory and its parent cannot be treated as dedicated caches."""
    home = tmp_path / "home" / "user"
    home.mkdir(parents=True)
    monkeypatch.setattr("pxaudit.cache.Path.home", lambda: home)

    with pytest.raises(CacheSafetyError):
        validate_cache_root(home)
    with pytest.raises(CacheSafetyError):
        validate_cache_root(home.parent)
    assert validate_cache_root(home / ".pxaudit_cache") == home / ".pxaudit_cache"


def test_validate_cache_root_rejects_current_and_ancestor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The working directory and its parent cannot be cache roots."""
    current = tmp_path / "project"
    current.mkdir()
    monkeypatch.chdir(current)

    with pytest.raises(CacheSafetyError):
        validate_cache_root(current)
    with pytest.raises(CacheSafetyError):
        validate_cache_root(current.parent)
    assert validate_cache_root(current / ".cache" / "pxaudit") == current / ".cache" / "pxaudit"


def test_validate_cache_root_wraps_resolution_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Path-resolution failures become typed cache safety errors."""
    monkeypatch.setattr(Path, "resolve", MagicMock(side_effect=OSError("broken")))

    with pytest.raises(CacheSafetyError):
        validate_cache_root(tmp_path / "cache")


def test_inspect_cache_recognizes_only_owned_version_two_entries(tmp_path: Path) -> None:
    """Inventory excludes unrelated, corrupt, legacy, temporary, and directory entries."""
    cache_dir = tmp_path / "cache"
    write_cache("PXD000001", "project", {"title": "owned"}, cache_dir=cache_dir)
    (cache_dir / "PXD000002_project.json").write_text(
        json.dumps({"cache_version": 1, "data": {"title": "legacy"}}), encoding="utf-8"
    )
    (cache_dir / "PXD000003_files.json").write_text("{broken", encoding="utf-8")
    (cache_dir / "notes.txt").write_text("keep", encoding="utf-8")
    (cache_dir / ".PXD000001_project.json.orphan.tmp").write_text("temp", encoding="utf-8")
    (cache_dir / "subdir").mkdir()

    inventory = inspect_cache(cache_dir)
    assert [entry.path.name for entry in inventory.entries] == ["PXD000001_project.json"]
    assert inventory.ignored == 5


def test_inspect_cache_missing_directory_is_empty(tmp_path: Path) -> None:
    """A missing safe cache directory has an empty inventory."""
    inventory = inspect_cache(tmp_path / "missing" / "cache")
    assert inventory.entries == ()
    assert inventory.ignored == 0


def test_inspect_cache_rejects_non_directory(tmp_path: Path) -> None:
    """A regular file cannot serve as a cache root."""
    cache_path = tmp_path / "cache"
    cache_path.write_text("not a directory", encoding="utf-8")

    with pytest.raises(CacheSafetyError):
        inspect_cache(cache_path)


def test_inspect_cache_wraps_listing_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Directory listing failures become typed safety errors."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    monkeypatch.setattr(Path, "iterdir", MagicMock(side_effect=OSError("denied")))

    with pytest.raises(CacheSafetyError):
        inspect_cache(cache_dir)


def test_clear_cache_removes_owned_and_preserves_ignored(tmp_path: Path) -> None:
    """Cleanup removes owned entries while preserving unrelated files."""
    cache_dir = tmp_path / "cache"
    write_cache("PXD000001", "project", {}, cache_dir=cache_dir)
    unrelated = cache_dir / "notes.txt"
    unrelated.write_text("keep", encoding="utf-8")

    removed, ignored, failed = clear_cache(cache_dir)
    assert (removed, ignored, failed) == (1, 1, 0)
    assert unrelated.read_text() == "keep"


def test_clear_cache_counts_unlink_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An owned-entry unlink error is reported without losing the entry."""
    cache_dir = tmp_path / "cache"
    write_cache("PXD000001", "project", {}, cache_dir=cache_dir)
    target = cache_dir / "PXD000001_project.json"
    real_unlink = Path.unlink

    def fail_target(self: Path, missing_ok: bool = False) -> None:
        if self == target:
            raise OSError("denied")
        real_unlink(self, missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_target)
    assert clear_cache(cache_dir) == (0, 0, 1)
    assert target.exists()


def test_concurrent_process_writes_leave_one_valid_entry_and_no_temporaries(
    tmp_path: Path,
) -> None:
    """Concurrent processes use independent temporary files and one valid final envelope."""
    cache_dir = tmp_path / "cache"
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    processes = [
        context.Process(target=_write_cache_in_process, args=(str(cache_dir), value, barrier))
        for value in (1, 2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=30)
        if process.is_alive():
            process.terminate()
            process.join()
            pytest.fail("concurrent cache writer did not terminate")
        assert process.exitcode == 0

    assert read_cache("PXD000001", "project", cache_dir=cache_dir) in (
        {"value": 1},
        {"value": 2},
    )
    assert [path.name for path in cache_dir.iterdir()] == ["PXD000001_project.json"]


def test_replace_failure_preserves_final_and_cleans_temporary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed atomic replacement preserves the prior entry and removes its temporary."""
    write_cache("PXD000001", "project", {"value": "old"}, cache_dir=tmp_path)
    monkeypatch.setattr("pxaudit.cache.os.replace", MagicMock(side_effect=OSError("busy")))

    with pytest.raises(CacheWriteError):
        write_cache("PXD000001", "project", {"value": "new"}, cache_dir=tmp_path)
    assert read_cache("PXD000001", "project", cache_dir=tmp_path) == {"value": "old"}
    assert not list(tmp_path.glob("*.tmp"))


def test_interrupted_replace_preserves_final_and_cleans_temporary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An interrupted replacement preserves the prior entry and removes its temporary."""
    write_cache("PXD000001", "project", {"value": "old"}, cache_dir=tmp_path)
    monkeypatch.setattr("pxaudit.cache.os.replace", MagicMock(side_effect=KeyboardInterrupt))

    with pytest.raises(KeyboardInterrupt):
        write_cache("PXD000001", "project", {"value": "new"}, cache_dir=tmp_path)
    assert read_cache("PXD000001", "project", cache_dir=tmp_path) == {"value": "old"}
    assert not list(tmp_path.glob("*.tmp"))


def test_serialization_failure_cleans_temporary(tmp_path: Path) -> None:
    """A non-JSON payload fails without leaving a temporary or final entry."""
    invalid: dict = {"values": {1, 2}}

    with pytest.raises(CacheWriteError):
        write_cache("PXD000001", "project", invalid, cache_dir=tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_write_preserves_corrupt_existing_destination(tmp_path: Path) -> None:
    """A corrupt destination is not overwritten without ownership proof."""
    path = tmp_path / "PXD000001_project.json"
    path.write_text("{broken", encoding="utf-8")

    with pytest.raises(CacheSafetyError):
        write_cache("PXD000001", "project", {}, cache_dir=tmp_path)
    assert path.read_text() == "{broken"


def test_write_migrates_structurally_valid_legacy_destination(tmp_path: Path) -> None:
    """A targeted write may replace a compatible unwrapped legacy entry."""
    path = tmp_path / "PXD000001_project.json"
    path.write_text(json.dumps({"title": "old"}), encoding="utf-8")

    write_cache("PXD000001", "project", {"title": "new"}, cache_dir=tmp_path)
    raw = json.loads(path.read_text())
    assert raw["cache_version"] == 2
    assert raw["data"] == {"title": "new"}


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Issue #19: Windows symlink creation is restricted",
)
def test_cache_io_refuses_cache_entry_symlink(tmp_path: Path) -> None:
    """Direct cache reads and writes do not follow an entry symlink."""
    external = tmp_path / "external.json"
    external.write_text("keep", encoding="utf-8")
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / "PXD000001_project.json").symlink_to(external)

    assert read_cache("PXD000001", "project", cache_dir=cache_dir) is None
    with pytest.raises(CacheSafetyError):
        write_cache("PXD000001", "project", {}, cache_dir=cache_dir)
    assert external.read_text() == "keep"


def test_cache_io_refuses_mocked_symlink_on_all_platforms(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The cache-entry symlink guard is enforced where real link creation is restricted."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    target = cache_dir / "PXD000001_project.json"
    target.write_text("keep", encoding="utf-8")
    real_is_symlink = Path.is_symlink

    def identify_target(self: Path) -> bool:
        return self == target or real_is_symlink(self)

    monkeypatch.setattr(Path, "is_symlink", identify_target)
    assert read_cache("PXD000001", "project", cache_dir=cache_dir) is None
    assert inspect_cache(cache_dir).ignored == 1
    with pytest.raises(CacheSafetyError):
        write_cache("PXD000001", "project", {}, cache_dir=cache_dir)


def test_inventory_rejects_unsafe_cache_shaped_filename(tmp_path: Path) -> None:
    """A cache suffix cannot make an unsafe accession filename owned."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / ".._project.json").write_text("{}", encoding="utf-8")

    assert inspect_cache(cache_dir).ignored == 1


def test_inventory_rejects_non_mapping_envelope(tmp_path: Path) -> None:
    """A list at a project cache path is not an owned envelope."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / "PXD000001_project.json").write_text("[]", encoding="utf-8")

    assert inspect_cache(cache_dir).ignored == 1


def test_inventory_counts_symlink_check_oserror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An entry that cannot be checked for symlink status is ignored."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    target = cache_dir / "notes.txt"
    target.write_text("keep", encoding="utf-8")
    real_is_symlink = Path.is_symlink

    def fail_target(self: Path) -> bool:
        if self == target:
            raise OSError("denied")
        return real_is_symlink(self)

    monkeypatch.setattr(Path, "is_symlink", fail_target)
    assert inspect_cache(cache_dir).ignored == 1


def test_clear_cache_ignores_entry_changed_after_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cleanup does not delete an entry that fails immediate revalidation."""
    cache_dir = tmp_path / "cache"
    write_cache("PXD000001", "project", {}, cache_dir=cache_dir)
    real_owned_entry = cache_module._owned_cache_entry
    calls = 0

    def change_after_inventory(path: Path) -> object:
        nonlocal calls
        calls += 1
        return real_owned_entry(path) if calls == 1 else None

    monkeypatch.setattr(cache_module, "_owned_cache_entry", change_after_inventory)
    assert clear_cache(cache_dir) == (0, 1, 0)
    assert (cache_dir / "PXD000001_project.json").exists()


def test_replaceability_rejects_symlink_and_stat_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Replacement validation rejects symlink and inaccessible destinations."""
    target = tmp_path / "target.json"
    link = tmp_path / "link.json"
    target.write_text("{}", encoding="utf-8")
    try:
        link.symlink_to(target)
    except OSError:
        link = target
        monkeypatch.setattr(Path, "is_symlink", lambda self: self == target)
    assert cache_module._existing_entry_is_replaceable(link, "PXD1", "project") is False

    monkeypatch.setattr(Path, "exists", MagicMock(side_effect=OSError("denied")))
    assert cache_module._existing_entry_is_replaceable(target, "PXD1", "project") is False


def test_write_wraps_cache_directory_creation_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cache-directory creation errors become typed write failures."""
    monkeypatch.setattr(Path, "mkdir", MagicMock(side_effect=OSError("denied")))

    with pytest.raises(CacheWriteError):
        write_cache("PXD000001", "project", {}, cache_dir=tmp_path / "cache")


def test_failed_write_logs_temporary_cleanup_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed cleanup is visible while the atomic write still reports failure."""
    real_unlink = Path.unlink

    def fail_temporary(self: Path, missing_ok: bool = False) -> None:
        if self.suffix == ".tmp":
            raise OSError("denied")
        real_unlink(self, missing_ok)

    monkeypatch.setattr("pxaudit.cache.os.replace", MagicMock(side_effect=OSError("busy")))
    monkeypatch.setattr(Path, "unlink", fail_temporary)
    with (
        caplog.at_level(logging.WARNING, logger="pxaudit.cache"),
        pytest.raises(CacheWriteError),
    ):
        write_cache("PXD000001", "project", {}, cache_dir=tmp_path)
    assert "Could not remove cache temporary file" in caplog.text

    monkeypatch.setattr(Path, "unlink", real_unlink)
    for path in tmp_path.glob("*.tmp"):
        path.unlink()
