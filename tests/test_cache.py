"""Cache ownership, containment, compatibility, and atomic-write tests.

All test artifacts live under ``tmp_path``. Dangerous-root tests resolve broad locations only
far enough to prove refusal; the suite never inventories or mutates the real user cache.
"""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import string
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
    CachedResponse,
    CacheKeyError,
    CacheSafetyError,
    CacheWriteError,
    clear_cache,
    inspect_cache,
    read_cache,
    read_cache_response,
    read_cache_stale,
    read_cache_stale_response,
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


def test_cache_miss_nonexistent_dir_returns_none(tmp_path: Path) -> None:
    """A missing cache directory produces a cache miss."""
    missing_dir = tmp_path / "no_such_dir"
    result = read_cache("PXD000001", "project", cache_dir=missing_dir)
    assert result is None


def test_cache_miss_file_absent_is_silent(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """An absent entry is a silent cache miss rather than a corruption warning."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    with caplog.at_level(logging.WARNING, logger="pxaudit.cache"):
        result = read_cache("PXD000001", "project", cache_dir=cache_dir)

    assert result is None
    assert caplog.records == []


@pytest.mark.parametrize(
    ("endpoint", "payload"),
    [
        (
            "project",
            {
                "accession": "PXD000001",
                "title": "Hé llo Wörld",
                "submissionDate": "2012-03-13",
            },
        ),
        (
            "files",
            [
                {"fileName": "résumé.raw", "fileCategory": {"name": "RAW"}, "fileSize": 1024},
                {"fileName": "result.mzid", "fileCategory": {"name": "RESULT"}, "fileSize": 2048},
            ],
        ),
    ],
)
def test_cache_round_trip_preserves_endpoint_payload(
    tmp_path: Path, endpoint: str, payload: dict | list
) -> None:
    """Each endpoint round trip preserves its JSON payload and outer shape."""
    write_cache("PXD000001", endpoint, payload, cache_dir=tmp_path)

    result = read_cache("PXD000001", endpoint, cache_dir=tmp_path)

    assert result == payload
    assert type(result) is type(payload)


def test_write_creates_missing_directory(tmp_path: Path) -> None:
    """A write creates an absent cache directory and its parents."""
    deep_dir = tmp_path / "a" / "b" / "cache"
    assert not deep_dir.exists()
    write_cache("PXD000001", "project", {"x": 1}, cache_dir=deep_dir)
    assert deep_dir.exists()
    assert (deep_dir / "PXD000001_project.json").exists()


def test_cache_file_named_correctly(tmp_path: Path) -> None:
    """A cache write uses the canonical accession and endpoint filename."""
    write_cache("PXD000001", "files", [{"a": 1}], cache_dir=tmp_path)
    expected = tmp_path / "PXD000001_files.json"
    assert expected.exists(), f"Expected {expected} but not found"
    json_files = list(tmp_path.glob("*.json"))
    assert len(json_files) == 1


def _write_corrupt_file(cache_dir: Path, accession: str, endpoint: str) -> Path:
    """Write syntactically invalid JSON to a cache path."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{accession}_{endpoint}.json"
    path.write_text("{this is: not valid json!!}", encoding="utf-8")
    return path


def test_corrupted_json_is_preserved_logged_cache_miss(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Corrupt JSON is preserved and reported as a cache miss."""
    corrupt_path = _write_corrupt_file(tmp_path, "PXD000001", "project")

    with caplog.at_level(logging.WARNING, logger="pxaudit.cache"):
        result = read_cache("PXD000001", "project", cache_dir=tmp_path)

    assert result is None
    assert corrupt_path.read_text(encoding="utf-8") == "{this is: not valid json!!}"
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.WARNING
    assert "PXD000001_project.json" in caplog.records[0].message


@pytest.mark.skipif(sys.platform == "win32", reason="chmod does not prevent writes on Windows")
def test_write_permission_error_raises(tmp_path: Path) -> None:
    """An unwritable cache directory produces a typed write failure."""
    cache_dir = tmp_path / "locked"
    cache_dir.mkdir()
    cache_dir.chmod(0o555)
    try:
        with pytest.raises(CacheWriteError):
            write_cache("PXD000001", "project", {"x": 1}, cache_dir=cache_dir)
    finally:
        cache_dir.chmod(0o755)


def test_overwrite_updates_cached_data(tmp_path: Path) -> None:
    """A second write replaces the prior payload without an extra entry."""
    write_cache("PXD000001", "project", {"title": "Old Title"}, cache_dir=tmp_path)
    write_cache("PXD000001", "project", {"title": "New Title"}, cache_dir=tmp_path)
    result = read_cache("PXD000001", "project", cache_dir=tmp_path)
    assert result == {"title": "New Title"}
    assert len(list(tmp_path.glob("*.json"))) == 1


def test_ttl_fresh_cache_disabled_with_none(tmp_path: Path) -> None:
    """A ``None`` maximum age serves an entry regardless of file age."""
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    old = time.time() - 999999
    os.utime(path, (old, old))
    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=None)
    assert result == {"key": "value"}


def test_ttl_default_constant_is_seven_days() -> None:
    """The default cache lifetime remains seven days in seconds."""
    assert _DEFAULT_TTL == 7 * 24 * 60 * 60


@pytest.mark.parametrize(
    ("age", "expected"),
    [(3_599, {"key": "value"}), (3_600, None), (3_601, None)],
)
@patch("time.time")
def test_ttl_boundary_applies_exact_expiry_contract(
    mock_time: MagicMock,
    tmp_path: Path,
    age: int,
    expected: dict | None,
) -> None:
    """Entries are fresh before the TTL and stale at or beyond it."""
    fixed_mtime = 1_000_000.0
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    os.utime(path, (fixed_mtime, fixed_mtime))
    mock_time.return_value = fixed_mtime + age

    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=3600)

    assert result == expected
    assert path.exists()


def test_ttl_zero_max_age_bypasses_fresh_cache(tmp_path: Path) -> None:
    """max_age=0 treats any cache as stale (equivalent to --refresh at CLI)."""
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    assert path.exists()
    result = read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=0)
    assert result is None
    assert path.exists(), "stale file must be kept for fallback"


def test_atomic_write_leaves_no_tmp_file(tmp_path: Path) -> None:
    """A successful atomic write leaves no temporary entry behind."""
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    json_path = tmp_path / "PXD000001_project.json"
    assert json_path.exists()
    assert not list(tmp_path.glob("*.tmp"))


def test_cache_version_header_present_on_write(tmp_path: Path) -> None:
    """A written envelope contains complete version-2 identity and provenance."""
    data = {"title": "test"}
    write_cache("PXD000001", "project", data, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    raw = json.loads(path.read_text())
    assert raw["cache_version"] == 2
    assert raw["cache_owner"] == "pxaudit"
    assert raw["accession"] == "PXD000001"
    assert raw["endpoint"] == "project"
    assert raw["retrieved_at"]
    assert raw["snapshot_id"]
    assert raw["data"] == data


def test_cache_response_preserves_embedded_provenance(tmp_path: Path) -> None:
    """Provenance-aware reads return the stored retrieval time and snapshot identifier."""
    retrieved_at = "2026-07-17T12:34:56-07:00"
    write_cache(
        "PXD000001",
        "project",
        {"title": "test"},
        cache_dir=tmp_path,
        retrieved_at=retrieved_at,
        snapshot_id="audit-snapshot",
    )

    response = read_cache_response("PXD000001", "project", cache_dir=tmp_path)

    assert isinstance(response, CachedResponse)
    assert response.data == {"title": "test"}
    assert response.retrieved_at == "2026-07-17T19:34:56+00:00"
    assert response.snapshot_id == "audit-snapshot"


@pytest.mark.parametrize(
    "raw",
    [
        {
            "cache_version": 1,
            "retrieved_at": "2026-01-01T00:00:00+00:00",
            "snapshot_id": "untrusted",
            "data": {"title": "legacy"},
        },
        {
            "title": "legacy",
            "retrieved_at": "2026-01-01T00:00:00+00:00",
            "snapshot_id": "untrusted",
        },
        {
            "cache_version": 2,
            "cache_owner": "pxaudit",
            "accession": "PXD000001",
            "endpoint": "project",
            "retrieved_at": None,
            "snapshot_id": "untrusted",
            "data": {"title": "legacy"},
        },
        {
            "cache_version": 2,
            "cache_owner": "pxaudit",
            "accession": "PXD000001",
            "endpoint": "project",
            "retrieved_at": "not-a-timestamp",
            "snapshot_id": "untrusted",
            "data": {"title": "legacy"},
        },
        {
            "cache_version": 2,
            "cache_owner": "pxaudit",
            "accession": "PXD000001",
            "endpoint": "project",
            "retrieved_at": "2026-01-01T00:00:00+00:00",
            "snapshot_id": "   ",
            "data": {"title": "legacy"},
        },
        {
            "cache_version": 2,
            "cache_owner": "pxaudit",
            "accession": "PXD000001",
            "endpoint": "project",
            "retrieved_at": "0001-01-01T00:00:00+23:59",
            "snapshot_id": "untrusted",
            "data": {"title": "legacy"},
        },
    ],
    ids=[
        "version-one-with-envelope-like-provenance",
        "unwrapped-with-response-fields",
        "missing-version-two-timestamp",
        "invalid-version-two-timestamp",
        "blank-version-two-snapshot",
        "overflowing-version-two-timestamp",
    ],
)
def test_compatible_entries_without_valid_provenance_use_file_time(
    tmp_path: Path, raw: dict
) -> None:
    """Compatible older provenance falls back to mtime without claiming a snapshot."""
    path = tmp_path / "PXD000001_project.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    modified_at = 1_700_000_000.0
    os.utime(path, (modified_at, modified_at))

    response = read_cache_response("PXD000001", "project", cache_dir=tmp_path, max_age=None)

    assert response is not None
    assert response.retrieved_at == "2023-11-14T22:13:20+00:00"
    assert response.snapshot_id is None


def test_unwrapped_legacy_files_use_file_time_provenance(tmp_path: Path) -> None:
    """An unwrapped legacy files list receives mtime provenance without a snapshot claim."""
    path = tmp_path / "PXD000001_files.json"
    path.write_text(json.dumps([{"fileName": "legacy.raw"}]), encoding="utf-8")
    modified_at = 1_700_000_000.0
    os.utime(path, (modified_at, modified_at))

    response = read_cache_response("PXD000001", "files", cache_dir=tmp_path, max_age=None)

    assert response is not None
    assert response.data == [{"fileName": "legacy.raw"}]
    assert response.retrieved_at == "2023-11-14T22:13:20+00:00"
    assert response.snapshot_id is None


@pytest.mark.parametrize(
    ("retrieved_at", "snapshot_id"),
    [
        ("2026-01-01T00:00:00", "snapshot"),
        ("0001-01-01T00:00:00+23:59", "snapshot"),
        (None, ""),
        (None, "   "),
    ],
)
def test_write_rejects_invalid_provenance(
    tmp_path: Path, retrieved_at: str | None, snapshot_id: str
) -> None:
    """Writes reject timezone-free timestamps and blank snapshot identifiers."""
    with pytest.raises(CacheWriteError):
        write_cache(
            "PXD000001",
            "project",
            {},
            cache_dir=tmp_path,
            retrieved_at=retrieved_at,
            snapshot_id=snapshot_id,
        )
    assert list(tmp_path.iterdir()) == []


def test_cache_legacy_format_still_readable(tmp_path: Path) -> None:
    """An unwrapped legacy project payload remains readable."""
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


@pytest.mark.parametrize("version", [999, True, 2.0], ids=["unknown", "boolean", "float"])
def test_unsupported_cache_version_is_unowned_and_irreplaceable(
    tmp_path: Path, version: int | bool | float
) -> None:
    """Unknown and non-integer versions cannot claim compatibility or ownership."""
    payload = {
        "cache_version": version,
        "cache_owner": "pxaudit",
        "accession": "PXD000001",
        "endpoint": "project",
        "retrieved_at": "2026-01-01T00:00:00+00:00",
        "snapshot_id": "untrusted",
        "data": {"title": "untrusted"},
    }
    path = tmp_path / "PXD000001_project.json"
    serialized = json.dumps(payload)
    path.write_text(serialized, encoding="utf-8")

    assert read_cache("PXD000001", "project", cache_dir=tmp_path, max_age=None) is None
    assert inspect_cache(tmp_path).ignored == 1
    with pytest.raises(CacheSafetyError):
        write_cache("PXD000001", "project", {}, cache_dir=tmp_path)
    assert path.read_text(encoding="utf-8") == serialized


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


def test_excessively_nested_json_is_preserved_cache_miss(tmp_path: Path) -> None:
    """JSON beyond the decoder recursion limit is ignored without deletion."""
    path = tmp_path / "PXD000001_project.json"
    nesting = sys.getrecursionlimit() + 100
    path.write_text("[" * nesting + "0" + "]" * nesting, encoding="utf-8")

    assert read_cache("PXD000001", "project", cache_dir=tmp_path) is None
    assert path.exists()


@patch("time.time")
def test_read_cache_stale_returns_data_and_age(mock_time: MagicMock, tmp_path: Path) -> None:
    """A stale read returns the payload and its age in seconds."""
    write_cache("PXD000001", "project", {"key": "value"}, cache_dir=tmp_path)
    path = tmp_path / "PXD000001_project.json"
    modified_at = 1_700_000_000.0
    os.utime(path, (modified_at, modified_at))
    mock_time.return_value = modified_at + 7_200

    data, age = read_cache_stale("PXD000001", "project", cache_dir=tmp_path)
    assert data == {"key": "value"}
    assert age == 7_200


@patch("time.time")
def test_read_cache_stale_response_includes_provenance_and_age(
    mock_time: MagicMock, tmp_path: Path
) -> None:
    """A stale provenance read returns stored identity together with cache age."""
    write_cache(
        "PXD000001",
        "project",
        {"key": "value"},
        cache_dir=tmp_path,
        retrieved_at="2025-01-01T00:00:00+00:00",
        snapshot_id="old-snapshot",
    )
    path = tmp_path / "PXD000001_project.json"
    modified_at = 1_700_000_000.0
    os.utime(path, (modified_at, modified_at))
    mock_time.return_value = modified_at + 7_200

    response = read_cache_stale_response("PXD000001", "project", cache_dir=tmp_path)

    assert response is not None
    assert response.retrieved_at == "2025-01-01T00:00:00+00:00"
    assert response.snapshot_id == "old-snapshot"
    assert response.age == 7_200


def test_read_cache_stale_missing_file_returns_none_none(tmp_path: Path) -> None:
    """A stale read of an absent entry returns the miss tuple."""
    data, age = read_cache_stale("PXD000001", "project", cache_dir=tmp_path)
    assert data is None
    assert age is None


@pytest.mark.parametrize(
    "accession",
    [
        "../outside",
        "PXD/000001",
        "PXD\\000001",
        "PXD\n000001",
        "PXD..000001",
        " PXD1",
        "A" * 65,
    ],
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


def test_cache_accession_accepts_maximum_length(tmp_path: Path) -> None:
    """A 64-character portable accession remains a valid cache key."""
    accession = "A" * 64

    write_cache(accession, "project", {"boundary": 64}, cache_dir=tmp_path)

    assert read_cache(accession, "project", cache_dir=tmp_path) == {"boundary": 64}
    assert (tmp_path / f"{accession}_project.json").is_file()


def test_cache_accession_ascii_positions_follow_portable_filename_grammar(
    tmp_path: Path,
) -> None:
    """An ASCII sweep accepts only portable characters in each filename position."""
    endpoints = frozenset(string.ascii_letters + string.digits)
    middle = endpoints | frozenset("._-")

    for codepoint in range(128):
        character = chr(codepoint)
        for accession, is_valid in (
            (f"A{character}B", character in middle),
            (f"{character}AB", character in endpoints),
            (f"AB{character}", character in endpoints),
        ):
            if is_valid:
                assert read_cache(accession, "project", cache_dir=tmp_path) is None
            else:
                with pytest.raises(CacheKeyError):
                    read_cache(accession, "project", cache_dir=tmp_path)


def test_cache_endpoint_rejects_unknown_value(tmp_path: Path) -> None:
    """Only project and files endpoints can form cache paths."""
    with pytest.raises(CacheKeyError):
        write_cache("PXD000001", "../project", {}, cache_dir=tmp_path)


@pytest.mark.parametrize("endpoint,data", [("project", []), ("files", {})])
def test_write_rejects_endpoint_payload_mismatch(
    tmp_path: Path, endpoint: str, data: dict | list
) -> None:
    """Project and files entries retain their required outer JSON shapes."""
    with pytest.raises(CacheWriteError):
        write_cache("PXD000001", endpoint, data, cache_dir=tmp_path)


def test_version_two_identity_mismatch_is_cache_miss(tmp_path: Path) -> None:
    """A version-2 identity mismatch is preserved as a cache miss."""
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


@pytest.mark.parametrize("symlink_result", [True, OSError("denied")], ids=["link", "stat-error"])
def test_cache_io_refuses_unsafe_entry_on_all_platforms(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    symlink_result: bool | OSError,
) -> None:
    """Direct and inventory access reject links and failed link checks."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    target = cache_dir / "PXD000001_project.json"
    target.write_text("keep", encoding="utf-8")
    real_is_symlink = Path.is_symlink

    def identify_target(self: Path) -> bool:
        if self != target:
            return real_is_symlink(self)
        if isinstance(symlink_result, OSError):
            raise symlink_result
        return symlink_result

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
