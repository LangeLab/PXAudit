"""Own, validate, and persist local PRIDE API cache entries.

PXAudit writes versioned JSON files under ``cache_dir``. Cache keys are constrained to
portable filename components, and maintenance commands recognize only version-2 envelopes
that explicitly identify PXAudit, the accession, and the endpoint.
"""

from __future__ import annotations

import json
import logging
import os
import re
import stat
import tempfile
import time
import typing
from dataclasses import dataclass
from pathlib import Path

_log = logging.getLogger(__name__)

_DEFAULT_CACHE_DIR = Path.home() / ".pxaudit_cache"
_DEFAULT_TTL: float = 7 * 24 * 60 * 60
_CACHE_VERSION: int = 2
_CACHE_OWNER = "pxaudit"
_CACHE_ENDPOINTS = frozenset({"project", "files"})
_CACHE_COMPONENT_RE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?\Z")


class CacheError(Exception):
    """Base exception for cache validation and write failures."""


class CacheKeyError(CacheError, ValueError):
    """Raised when a cache key cannot form a safe portable filename."""


class CacheSafetyError(CacheError):
    """Raised when a cache root or destination is unsafe to modify."""


class CacheWriteError(CacheError):
    """Raised when a cache entry cannot be written atomically."""


@dataclass(frozen=True)
class CacheEntry:
    """Metadata for one validated PXAudit-owned cache entry."""

    path: Path
    size: int
    modified_at: float


@dataclass(frozen=True)
class CacheInventory:
    """Validated cache entries and the count of ignored directory entries."""

    entries: tuple[CacheEntry, ...]
    ignored: int


__all__ = [
    "CacheEntry",
    "CacheError",
    "CacheInventory",
    "CacheKeyError",
    "CacheSafetyError",
    "CacheWriteError",
    "clear_cache",
    "inspect_cache",
    "read_cache",
    "read_cache_stale",
    "validate_cache_root",
    "write_cache",
]


def _validate_cache_key(accession: str, endpoint: str) -> None:
    """Reject cache-key components that are unsafe or ambiguous as filenames."""
    if endpoint not in _CACHE_ENDPOINTS:
        raise CacheKeyError(f"unsupported cache endpoint: {endpoint!r}")
    if not _CACHE_COMPONENT_RE.fullmatch(accession) or ".." in accession:
        raise CacheKeyError(f"unsafe cache accession: {accession!r}")


def validate_cache_root(cache_dir: str | os.PathLike[str]) -> Path:
    """Resolve and validate a dedicated cache directory.

    Parameters
    ----------
    cache_dir:
        Configured cache directory. Blank values and broad shared roots are refused.

    Returns
    -------
    pathlib.Path
        The resolved safe cache root.

    Raises
    ------
    CacheSafetyError
        If the value is blank, cannot be resolved, or names a dangerous shared location.
    """
    raw_path = os.fspath(cache_dir)
    if not raw_path.strip():
        raise CacheSafetyError("cache directory is empty")

    try:
        resolved = Path(raw_path).expanduser().resolve(strict=False)
        home = Path.home().resolve(strict=False)
        current = Path.cwd().resolve(strict=False)
        temp_root = Path(tempfile.gettempdir()).resolve(strict=False)
        filesystem_root = Path(resolved.anchor)
    except (OSError, RuntimeError, ValueError) as exc:
        raise CacheSafetyError("cache directory cannot be resolved safely") from exc

    dangerous = {filesystem_root, home, current, temp_root}
    if resolved in dangerous or resolved in home.parents or resolved in current.parents:
        raise CacheSafetyError("cache directory must be a dedicated subdirectory")
    return resolved


def _cache_path(
    accession: str,
    endpoint: str,
    cache_dir: str | os.PathLike[str],
) -> Path:
    """Return a contained cache path for a validated key."""
    _validate_cache_key(accession, endpoint)
    root = validate_cache_root(cache_dir)
    path = root / f"{accession}_{endpoint}.json"
    try:
        if path.is_symlink():
            raise CacheSafetyError("cache entry is a symlink")
    except OSError as exc:
        raise CacheSafetyError("cache entry cannot be resolved safely") from exc
    return path


def _payload_matches_endpoint(data: typing.Any, endpoint: str) -> bool:
    """Return whether a decoded payload has the endpoint's required outer shape."""
    return isinstance(data, dict) if endpoint == "project" else isinstance(data, list)


def _unwrap_cache(raw: typing.Any, accession: str, endpoint: str) -> dict | list | None:
    """Extract a compatible payload without treating invalid data as owned."""
    if isinstance(raw, dict) and "cache_version" in raw:
        version = raw.get("cache_version")
        data = raw.get("data")
        if version == _CACHE_VERSION:
            identity_matches = (
                raw.get("cache_owner") == _CACHE_OWNER
                and raw.get("accession") == accession
                and raw.get("endpoint") == endpoint
            )
            if identity_matches and _payload_matches_endpoint(data, endpoint):
                return typing.cast(dict | list, data)
            return None
        if version == 1 and _payload_matches_endpoint(data, endpoint):
            return typing.cast(dict | list, data)
        return None
    if _payload_matches_endpoint(raw, endpoint):
        return typing.cast(dict | list, raw)
    return None


def _read_json_file(
    path: Path,
    *,
    max_age: float | None,
    log_failures: bool,
) -> tuple[typing.Any, os.stat_result] | None:
    """Read one regular JSON file without following a final symlink."""
    descriptor: int | None = None
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
        file_stat = os.fstat(descriptor)
        if not stat.S_ISREG(file_stat.st_mode):
            return None
        if max_age is not None and time.time() - file_stat.st_mtime >= max_age:
            return None
        with os.fdopen(descriptor, encoding="utf-8") as handle:
            descriptor = None
            return json.load(handle), file_stat
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        if log_failures:
            _log.warning("Ignoring unreadable cache entry %s: %s", path.name, type(exc).__name__)
        return None
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                if log_failures:
                    _log.warning("Could not close unreadable cache entry %s", path.name)


def _read_entry(
    accession: str,
    endpoint: str,
    *,
    cache_dir: str | os.PathLike[str],
    max_age: float | None,
) -> tuple[dict | list, float] | tuple[None, None]:
    """Read and validate one cache entry, returning its payload and age."""
    try:
        path = _cache_path(accession, endpoint, cache_dir)
    except CacheSafetyError as exc:
        _log.warning("Ignoring unsafe cache location: %s", exc)
        return None, None

    loaded = _read_json_file(path, max_age=max_age, log_failures=True)
    if loaded is None:
        return None, None
    raw, file_stat = loaded
    data = _unwrap_cache(raw, accession, endpoint)
    if data is None:
        _log.warning("Ignoring invalid cache entry %s", path.name)
        return None, None
    return data, max(0.0, time.time() - file_stat.st_mtime)


def read_cache(
    accession: str,
    endpoint: str,
    *,
    cache_dir: str | os.PathLike[str] = _DEFAULT_CACHE_DIR,
    max_age: float | None = _DEFAULT_TTL,
) -> dict | list | None:
    """Return a fresh compatible cache payload, or ``None`` on a cache miss.

    Filesystem, decoding, structure, ownership, and expiry failures are cache misses. Invalid
    files remain untouched because their ownership cannot be proven. Unsafe key components
    raise :class:`CacheKeyError` before filesystem access.

    Parameters
    ----------
    accession:
        Accession component of the cache identity.
    endpoint:
        Either ``"project"`` for a mapping or ``"files"`` for a list.
    cache_dir:
        Dedicated cache directory.
    max_age:
        Maximum entry age in seconds. ``None`` disables expiry.

    Returns
    -------
    dict | list | None
        The decoded endpoint payload, or ``None`` when no fresh valid entry is available.

    Raises
    ------
    CacheKeyError
        If the accession or endpoint cannot form a safe cache key.
    """
    data, _age = _read_entry(
        accession,
        endpoint,
        cache_dir=cache_dir,
        max_age=max_age,
    )
    return data


def read_cache_stale(
    accession: str,
    endpoint: str,
    *,
    cache_dir: str | os.PathLike[str] = _DEFAULT_CACHE_DIR,
) -> tuple[dict | list, float] | tuple[None, None]:
    """Return a compatible cache payload and age without applying the TTL.

    Parameters
    ----------
    accession:
        Accession component of the cache identity.
    endpoint:
        Either ``"project"`` for a mapping or ``"files"`` for a list.
    cache_dir:
        Dedicated cache directory.

    Returns
    -------
    tuple[dict | list, float] | tuple[None, None]
        The decoded payload and nonnegative age in seconds, or ``(None, None)`` on a miss.

    Raises
    ------
    CacheKeyError
        If the accession or endpoint cannot form a safe cache key.
    """
    return _read_entry(accession, endpoint, cache_dir=cache_dir, max_age=None)


def _parse_entry_name(name: str) -> tuple[str, str] | None:
    """Parse a canonical cache filename into its accession and endpoint."""
    for endpoint in _CACHE_ENDPOINTS:
        suffix = f"_{endpoint}.json"
        if not name.endswith(suffix):
            continue
        accession = name[: -len(suffix)]
        try:
            _validate_cache_key(accession, endpoint)
        except CacheKeyError:
            return None
        return accession, endpoint
    return None


def _owned_cache_entry(path: Path) -> CacheEntry | None:
    """Return metadata only when a file proves PXAudit ownership."""
    try:
        if path.is_symlink():
            return None
    except OSError:
        return None
    identity = _parse_entry_name(path.name)
    if identity is None:
        return None
    accession, endpoint = identity
    loaded = _read_json_file(path, max_age=None, log_failures=False)
    if loaded is None:
        return None
    raw, file_stat = loaded
    if not isinstance(raw, dict):
        return None
    data = raw.get("data")
    if not (
        raw.get("cache_version") == _CACHE_VERSION
        and raw.get("cache_owner") == _CACHE_OWNER
        and raw.get("accession") == accession
        and raw.get("endpoint") == endpoint
        and _payload_matches_endpoint(data, endpoint)
    ):
        return None
    return CacheEntry(path=path, size=file_stat.st_size, modified_at=file_stat.st_mtime)


def inspect_cache(cache_dir: str | os.PathLike[str]) -> CacheInventory:
    """Return the validated owned entries and ignored-entry count for a cache root.

    The function never follows directory-entry symlinks. Missing safe directories produce an
    empty inventory; listing failures raise :class:`CacheSafetyError`.

    Parameters
    ----------
    cache_dir:
        Dedicated cache directory to inspect.

    Returns
    -------
    CacheInventory
        Owned version-2 entries and the number of ignored directory entries.

    Raises
    ------
    CacheSafetyError
        If the cache root is dangerous, is not a directory, or cannot be listed.
    """
    root = validate_cache_root(cache_dir)
    try:
        if not root.exists():
            return CacheInventory(entries=(), ignored=0)
        if not root.is_dir():
            raise CacheSafetyError("cache path is not a directory")
        paths = tuple(root.iterdir())
    except OSError as exc:
        raise CacheSafetyError("cache directory cannot be inspected") from exc

    entries: list[CacheEntry] = []
    ignored = 0
    for path in paths:
        entry = _owned_cache_entry(path)
        if entry is None:
            ignored += 1
        else:
            entries.append(entry)
    return CacheInventory(entries=tuple(entries), ignored=ignored)


def clear_cache(cache_dir: str | os.PathLike[str]) -> tuple[int, int, int]:
    """Delete validated PXAudit entries and return ``(removed, ignored, failed)``.

    Every entry is revalidated immediately before deletion. Unowned, changed, unreadable, and
    symlink entries are ignored. An unlink failure is counted without deleting other entries.

    Parameters
    ----------
    cache_dir:
        Dedicated cache directory to clean.

    Returns
    -------
    tuple[int, int, int]
        Counts of removed, ignored, and failed entries.

    Raises
    ------
    CacheSafetyError
        If the cache root is dangerous or cannot be inspected.
    """
    inventory = inspect_cache(cache_dir)
    removed = 0
    failed = 0
    ignored = inventory.ignored
    for entry in inventory.entries:
        current = _owned_cache_entry(entry.path)
        if current is None:
            ignored += 1
            continue
        try:
            entry.path.unlink()
        except OSError:
            failed += 1
        else:
            removed += 1
    return removed, ignored, failed


def _existing_entry_is_replaceable(path: Path, accession: str, endpoint: str) -> bool:
    """Allow replacement only for owned or structurally valid legacy entries."""
    try:
        if path.is_symlink():
            return False
        if not path.exists():
            return True
    except OSError:
        return False
    loaded = _read_json_file(path, max_age=None, log_failures=False)
    if loaded is None:
        return False
    raw, _file_stat = loaded
    return _unwrap_cache(raw, accession, endpoint) is not None


def write_cache(
    accession: str,
    endpoint: str,
    data: dict | list,
    *,
    cache_dir: str | os.PathLike[str] = _DEFAULT_CACHE_DIR,
) -> None:
    """Atomically write one owned cache entry with a unique temporary file.

    Existing version-2 entries and structurally valid version-1 or unwrapped legacy entries may
    be replaced. Unknown, corrupt, mismatched, and symlink destinations remain untouched.

    Parameters
    ----------
    accession:
        Accession component of the cache identity.
    endpoint:
        Either ``"project"`` for a mapping or ``"files"`` for a list.
    data:
        Decoded endpoint payload with the shape required by ``endpoint``.
    cache_dir:
        Dedicated cache directory.

    Raises
    ------
    CacheKeyError
        If ``accession`` or ``endpoint`` cannot form a safe cache key.
    CacheSafetyError
        If the cache root or existing destination is unsafe to modify.
    CacheWriteError
        If serialization, temporary-file creation, replacement, or cleanup fails.
    """
    _validate_cache_key(accession, endpoint)
    if not _payload_matches_endpoint(data, endpoint):
        raise CacheWriteError("cache payload does not match its endpoint")

    path = _cache_path(accession, endpoint, cache_dir)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise CacheWriteError("cache directory could not be created") from exc
    if not _existing_entry_is_replaceable(path, accession, endpoint):
        raise CacheSafetyError("existing cache destination is not owned or migratable")

    payload = {
        "cache_version": _CACHE_VERSION,
        "cache_owner": _CACHE_OWNER,
        "accession": accession,
        "endpoint": endpoint,
        "data": data,
    }
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            json.dump(payload, handle, ensure_ascii=False)
        os.replace(temporary_path, path)
    except (OSError, TypeError, ValueError) as exc:
        raise CacheWriteError("cache entry could not be written atomically") from exc
    finally:
        if temporary_path is not None and temporary_path.exists():
            try:
                temporary_path.unlink()
            except OSError:
                _log.warning("Could not remove cache temporary file %s", temporary_path.name)
