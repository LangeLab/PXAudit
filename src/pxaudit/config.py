"""User configuration for pxaudit.

Loads ``~/.pxaudit.toml`` (or ``PXAUDIT_CONFIG`` / an explicit path), validates
types, warns on unknown keys, and merges with CLI overrides.

Precedence
----------
CLI flag > config file > built-in default.

Type policy
-----------
If a known key has the wrong type, that key falls back to its built-in default
and a warning is recorded. Other valid keys from the same file are kept.
Corrupt TOML falls back to all defaults and records a warning with the file path.
"""

from __future__ import annotations

import os
import tomllib
import typing
from dataclasses import dataclass
from pathlib import Path

__all__ = [
    "CONFIG_KEYS",
    "DEFAULTS",
    "EffectiveConfig",
    "default_config_path",
    "format_config_show",
    "load_file_config",
    "merge_config",
]

CONFIG_KEYS: tuple[str, ...] = (
    "cache_dir",
    "cache_ttl_seconds",
    "db_path",
    "request_delay",
    "bulk_delay",
    "export_format",
)

Source = typing.Literal["default", "config", "flag"]

DEFAULTS: dict[str, object] = {
    "cache_dir": str(Path.home() / ".pxaudit_cache"),
    "cache_ttl_seconds": float(7 * 24 * 60 * 60),
    "db_path": "pxaudit_results.db",
    "request_delay": 0.5,
    "bulk_delay": 1.0,
    "export_format": None,
}

_KEY_TYPES: dict[str, type | tuple[type, ...]] = {
    "cache_dir": str,
    "cache_ttl_seconds": (int, float),
    "db_path": str,
    "request_delay": (int, float),
    "bulk_delay": (int, float),
    "export_format": (str, type(None)),
}

_EXPORT_FORMATS = frozenset({"tsv", "csv", "json"})


@dataclass(frozen=True)
class EffectiveConfig:
    """Resolved settings with per-key provenance.

    Attributes
    ----------
    cache_dir:
        Directory for JSON API cache files.
    cache_ttl_seconds:
        Fresh-cache TTL passed to ``read_cache``.
    db_path:
        Default SQLite path when ``--db`` is omitted.
    request_delay:
        Politeness sleep inside ``fetch_project`` / ``fetch_files``.
    bulk_delay:
        Inter-accession pause in ``bulk-audit``.
    export_format:
        Optional default export format for ``bulk-audit`` (``tsv``/``csv``/``json``).
    sources:
        Map of setting name to ``default``, ``config``, or ``flag``.
    warnings:
        Human-readable warnings from load/merge (unknown keys, type errors, parse errors).
    """

    cache_dir: str
    cache_ttl_seconds: float
    db_path: str
    request_delay: float
    bulk_delay: float
    export_format: str | None
    sources: dict[str, Source]
    warnings: tuple[str, ...]


def default_config_path() -> Path:
    """Return the user config path (``PXAUDIT_CONFIG`` or ``~/.pxaudit.toml``)."""
    override = os.environ.get("PXAUDIT_CONFIG")
    if override:
        return Path(override)
    return Path.home() / ".pxaudit.toml"


def _type_ok(key: str, value: object) -> bool:
    if key == "export_format":
        if value is None:
            return True
        return isinstance(value, str) and value.casefold() in _EXPORT_FORMATS
    if key in {"cache_ttl_seconds", "request_delay", "bulk_delay"}:
        # bool is a subclass of int; reject it explicitly.
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False
        return float(value) >= 0
    expected = _KEY_TYPES[key]
    return isinstance(value, expected)


def _normalize(key: str, value: object) -> object:
    if key in {"cache_ttl_seconds", "request_delay", "bulk_delay"}:
        return float(value)  # type: ignore[arg-type]
    if key == "export_format" and isinstance(value, str):
        return value.casefold()
    if key == "cache_dir":
        if isinstance(value, str) and not value.strip():
            return value
        return str(Path(value).expanduser())  # type: ignore[arg-type]
    return value


def load_file_config(
    path: Path | None = None,
) -> tuple[dict[str, object], tuple[str, ...]]:
    """Load known keys from a TOML file.

    Parameters
    ----------
    path:
        Config file path. Defaults to :func:`default_config_path`.

    Returns
    -------
    tuple[dict[str, object], tuple[str, ...]]
        Validated key/value map (may be empty) and warning messages.
    """
    cfg_path = path if path is not None else default_config_path()
    warnings: list[str] = []

    if not cfg_path.exists():
        return {}, tuple(warnings)

    try:
        raw_text = cfg_path.read_text(encoding="utf-8")
        data = tomllib.loads(raw_text)
    except tomllib.TOMLDecodeError as exc:
        warnings.append(f"Warning: could not parse config {cfg_path}: {exc}")
        return {}, tuple(warnings)
    except OSError as exc:
        warnings.append(f"Warning: could not read config {cfg_path}: {exc}")
        return {}, tuple(warnings)

    if not isinstance(data, dict):
        warnings.append(f"Warning: config {cfg_path} root must be a table; ignoring file.")
        return {}, tuple(warnings)

    result: dict[str, object] = {}
    for key, value in data.items():
        if isinstance(value, dict):
            warnings.append(
                f"Warning: nested table {key!r} in {cfg_path} is not supported; "
                "use flat keys. Ignoring."
            )
            continue
        if key not in CONFIG_KEYS:
            warnings.append(f"Warning: unknown config key {key!r} in {cfg_path}; ignoring.")
            continue
        if not _type_ok(key, value):
            warnings.append(
                f"Warning: config key {key!r} in {cfg_path} has invalid type/value "
                f"{value!r}; using default."
            )
            continue
        result[key] = _normalize(key, value)
    return result, tuple(warnings)


def merge_config(
    file_values: dict[str, object] | None = None,
    *,
    file_warnings: tuple[str, ...] = (),
    cache_dir: str | None = None,
    db_path: str | None = None,
    request_delay: float | None = None,
    bulk_delay: float | None = None,
    export_format: str | None = None,
    cache_ttl_seconds: float | None = None,
) -> EffectiveConfig:
    """Merge defaults, file values, and explicit CLI overrides.

    Parameters
    ----------
    file_values:
        Validated values from :func:`load_file_config`.
    file_warnings:
        Warnings produced while loading the file.
    cache_dir, db_path, request_delay, bulk_delay, export_format, cache_ttl_seconds:
        CLI overrides. ``None`` means the flag was not provided.

    Returns
    -------
    EffectiveConfig
        Fully resolved settings and provenance.
    """
    file_values = file_values or {}
    values: dict[str, object] = dict(DEFAULTS)
    sources: dict[str, Source] = {k: "default" for k in CONFIG_KEYS}

    for key, value in file_values.items():
        values[key] = value
        sources[key] = "config"

    flag_map: dict[str, object | None] = {
        "cache_dir": cache_dir,
        "db_path": db_path,
        "request_delay": request_delay,
        "bulk_delay": bulk_delay,
        "export_format": export_format,
        "cache_ttl_seconds": cache_ttl_seconds,
    }
    for key, flag_val in flag_map.items():
        if flag_val is None:
            continue
        if key == "export_format":
            if not _type_ok(key, flag_val):
                continue
            values[key] = _normalize(key, flag_val)
        elif key == "cache_dir":
            values[key] = _normalize(key, flag_val)
        elif key in {"cache_ttl_seconds", "request_delay", "bulk_delay"}:
            if isinstance(flag_val, bool) or float(flag_val) < 0:  # type: ignore[arg-type]
                continue
            values[key] = float(flag_val)  # type: ignore[arg-type]
        else:
            # db_path (only remaining keyed override)
            values[key] = str(flag_val)
        sources[key] = "flag"

    export_val = values["export_format"]
    return EffectiveConfig(
        cache_dir=str(values["cache_dir"]),
        cache_ttl_seconds=float(values["cache_ttl_seconds"]),  # type: ignore[arg-type]
        db_path=str(values["db_path"]),
        request_delay=float(values["request_delay"]),  # type: ignore[arg-type]
        bulk_delay=float(values["bulk_delay"]),  # type: ignore[arg-type]
        export_format=None if export_val is None else str(export_val),
        sources=sources,
        warnings=file_warnings,
    )


def format_config_show(cfg: EffectiveConfig) -> str:
    """Render ``config show`` text with source tags."""
    lines = []
    for key in CONFIG_KEYS:
        val = getattr(cfg, key)
        src = cfg.sources[key]
        lines.append(f"{key}={val!r}  ({src})")
    return "\n".join(lines)
