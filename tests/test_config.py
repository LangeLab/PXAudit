"""Configuration loading, merging, rendering, and CLI contract tests."""

from __future__ import annotations

import os
import socket
from pathlib import Path

import pytest
from click.testing import CliRunner

from pxaudit.cli import main
from pxaudit.config import (
    CONFIG_KEYS,
    DEFAULTS,
    default_config_path,
    format_config_show,
    load_file_config,
    merge_config,
)


def test_shared_fixture_isolates_ambient_paths(tmp_path: Path) -> None:
    """Every test runs with temporary home, config, cache, and working paths."""
    home = Path.home()
    config_path = Path(os.environ["PXAUDIT_CONFIG"])
    cache_dir = DEFAULTS["cache_dir"]

    assert home.name == "home"
    assert home.parent.name.endswith("-ambient")
    assert config_path.parent == home.parent
    assert isinstance(cache_dir, str)
    assert Path(cache_dir).parent == home.parent
    assert Path.cwd() == home.parent / "work"


def test_shared_fixture_blocks_offline_socket_access() -> None:
    """Offline tests reject DNS, TCP, and unconnected UDP entry points."""
    with pytest.raises(pytest.fail.Exception, match="offline tests must not open"):
        socket.create_connection(("example.invalid", 443))
    with pytest.raises(pytest.fail.Exception, match="offline tests must not open"):
        socket.getaddrinfo("example.invalid", 443)

    connect_ex = vars(socket.socket)["connect_ex"]
    with pytest.raises(pytest.fail.Exception, match="offline tests must not open"):
        connect_ex(None, ("192.0.2.1", 443))
    sendto = vars(socket.socket)["sendto"]
    with pytest.raises(pytest.fail.Exception, match="offline tests must not open"):
        sendto(None, b"probe", ("192.0.2.1", 443))


@pytest.mark.parametrize("contents", [None, ""], ids=["missing", "empty"])
def test_missing_or_empty_file_returns_empty(tmp_path: Path, contents: str | None) -> None:
    """Missing and empty configuration files contribute no values or warnings."""
    path = tmp_path / "config.toml"
    if contents is not None:
        path.write_text(contents, encoding="utf-8")

    values, warnings = load_file_config(path)

    assert values == {}
    assert warnings == ()


def test_load_valid_keys(tmp_path: Path) -> None:
    """Known keys load and normalize."""
    path = tmp_path / "cfg.toml"
    path.write_text(
        'cache_dir = "pxa_cache"\n'
        "cache_ttl_seconds = 60\n"
        'db_path = "custom.db"\n'
        "request_delay = 0.25\n"
        "bulk_delay = 2\n"
        'export_format = "TSV"\n'
        "color = true\n",
        encoding="utf-8",
    )

    values, warnings = load_file_config(path)

    assert warnings == ()
    assert values == {
        "cache_dir": "pxa_cache",
        "cache_ttl_seconds": 60.0,
        "db_path": "custom.db",
        "request_delay": 0.25,
        "bulk_delay": 2.0,
        "export_format": "tsv",
        "color": True,
    }


def test_unknown_key_warns(tmp_path: Path) -> None:
    """Unknown keys warn and are ignored."""
    path = tmp_path / "cfg.toml"
    path.write_text('db_path = "x.db"\nfancy = true\n', encoding="utf-8")

    values, warnings = load_file_config(path)

    assert values == {"db_path": "x.db"}
    assert len(warnings) == 1
    assert "'fancy'" in warnings[0]


def test_wrong_type_falls_back_key(tmp_path: Path) -> None:
    """Wrong type for one key keeps other valid keys."""
    path = tmp_path / "cfg.toml"
    path.write_text('db_path = "ok.db"\nrequest_delay = "fast"\n', encoding="utf-8")

    values, warnings = load_file_config(path)

    assert values == {"db_path": "ok.db"}
    assert len(warnings) == 1
    assert "'request_delay'" in warnings[0]


def test_corrupt_toml_warns_with_path(tmp_path: Path) -> None:
    """Corrupt TOML contributes no values and reports its path and line."""
    path = tmp_path / "bad.toml"
    path.write_text("db_path = [unterminated\n", encoding="utf-8")

    values, warnings = load_file_config(path)

    assert values == {}
    assert len(warnings) == 1
    assert str(path) in warnings[0]
    assert "line 1" in warnings[0]


def test_oversized_integer_literal_warns_and_returns_empty(tmp_path: Path) -> None:
    """An integer beyond parser limits warns instead of raising ``ValueError``."""
    path = tmp_path / "oversized.toml"
    path.write_text(f"request_delay = {'9' * 5_000}\n", encoding="utf-8")

    values, warnings = load_file_config(path)

    assert values == {}
    assert len(warnings) == 1
    assert str(path) in warnings[0]
    assert "could not parse" in warnings[0]


def test_invalid_utf8_warns_and_returns_empty(tmp_path: Path) -> None:
    """An undecodable configuration file warns and contributes no values."""
    path = tmp_path / "invalid.toml"
    path.write_bytes(b'db_path = "\xff.db"\n')

    values, warnings = load_file_config(path)

    assert values == {}
    assert len(warnings) == 1
    assert str(path) in warnings[0]
    assert "could not read" in warnings[0]


def test_merge_precedence_flag_over_config_over_default() -> None:
    """CLI flag > config file > default."""
    file_values = {"db_path": "from_file.db", "bulk_delay": 3.0}
    cfg = merge_config(file_values, db_path="from_flag.db")
    assert cfg.db_path == "from_flag.db"
    assert cfg.sources["db_path"] == "flag"
    assert cfg.bulk_delay == 3.0
    assert cfg.sources["bulk_delay"] == "config"
    assert cfg.request_delay == DEFAULTS["request_delay"]
    assert cfg.sources["request_delay"] == "default"


def test_delays_remain_separate() -> None:
    """request_delay and bulk_delay are never collapsed."""
    cfg = merge_config({"request_delay": 0.1, "bulk_delay": 9.0})
    assert cfg.request_delay == 0.1
    assert cfg.bulk_delay == 9.0


def test_format_config_show_includes_sources() -> None:
    """Configuration rendering preserves key order and source tags."""
    cfg = merge_config({"db_path": "a.db"}, db_path="b.db")
    lines = format_config_show(cfg).splitlines()

    assert [line.partition("=")[0] for line in lines] == list(CONFIG_KEYS)
    assert lines[2] == "db_path='b.db'  (flag)"
    assert sum(line.endswith("(default)") for line in lines) == len(CONFIG_KEYS) - 1


def test_default_config_path_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """PXAUDIT_CONFIG overrides default path."""
    target = tmp_path / "alt.toml"
    monkeypatch.setenv("PXAUDIT_CONFIG", str(target))
    assert default_config_path() == target


@pytest.mark.parametrize("override", [None, ""], ids=["missing", "empty"])
def test_default_config_path_home(monkeypatch: pytest.MonkeyPatch, override: str | None) -> None:
    """A missing or empty override selects the configuration path under home."""
    if override is None:
        monkeypatch.delenv("PXAUDIT_CONFIG", raising=False)
    else:
        monkeypatch.setenv("PXAUDIT_CONFIG", override)

    assert default_config_path() == Path.home() / ".pxaudit.toml"


def test_config_show_cli_no_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The show command reports every default when no file exists."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    runner = CliRunner()
    result = runner.invoke(main, ["config", "show"])
    assert result.exit_code == 0
    for key in CONFIG_KEYS:
        assert key in result.output
    assert "(default)" in result.output


def test_config_show_cli_with_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The show command tags file-sourced values as configuration values."""
    cfg_path = tmp_path / "u.toml"
    cfg_path.write_text("bulk_delay = 4.5\n", encoding="utf-8")
    monkeypatch.setenv("PXAUDIT_CONFIG", str(cfg_path))
    runner = CliRunner()
    result = runner.invoke(main, ["config", "show"])
    assert result.exit_code == 0
    assert "bulk_delay=4.5" in result.output
    assert "(config)" in result.output


def test_config_show_flag_cache_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The show command tags a cache-directory override as a flag value."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    runner = CliRunner()
    result = runner.invoke(main, ["--cache-dir", str(tmp_path / "c"), "config", "show"])
    assert result.exit_code == 0
    assert "(flag)" in result.output
    assert "cache_dir=" in result.output


def test_oserror_on_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A configuration read error warns and contributes no values."""
    path = tmp_path / "x.toml"
    path.write_text('db_path = "a.db"\n', encoding="utf-8")

    def boom(self: Path, *args: object, **kwargs: object) -> str:
        raise OSError("permission denied")

    monkeypatch.setattr(Path, "read_text", boom)
    values, warnings = load_file_config(path)

    assert values == {}
    assert len(warnings) == 1
    assert "could not read" in warnings[0]


def test_cache_dir_expanduser(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """cache_dir expands ~ (HOME on POSIX, USERPROFILE on Windows)."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    path = tmp_path / "cfg.toml"
    path.write_text('cache_dir = "~/mycache"\n', encoding="utf-8")
    values, warnings = load_file_config(path)

    assert values["cache_dir"] == str(tmp_path / "mycache")
    assert warnings == ()


@pytest.mark.parametrize("blank", ["", "   "], ids=["empty", "whitespace"])
def test_blank_cache_dir_remains_blank_for_safety_validation(tmp_path: Path, blank: str) -> None:
    """A blank cache path is not normalized into the current directory."""
    path = tmp_path / "cfg.toml"
    path.write_text(f'cache_dir = "{blank}"\n', encoding="utf-8")

    values, warnings = load_file_config(path)

    assert values["cache_dir"] == blank
    assert warnings == ()


def test_root_not_table(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Non-dict TOML root is rejected."""
    path = tmp_path / "x.toml"
    path.write_text("ok = 1\n", encoding="utf-8")
    monkeypatch.setattr("pxaudit.config.tomllib.loads", lambda _s: [1, 2, 3])
    values, warnings = load_file_config(path)

    assert values == {}
    assert len(warnings) == 1
    assert "root must be a table" in warnings[0]


def test_merge_invalid_export_format_flag_ignored() -> None:
    """An invalid format override preserves the configured value and source."""
    cfg = merge_config({"export_format": "tsv"}, export_format="xml")
    assert cfg.export_format == "tsv"
    assert cfg.sources["export_format"] == "config"


def test_merge_invalid_color_flag_ignored() -> None:
    """An invalid defensive color override preserves the configured value."""
    cfg = merge_config({"color": False}, color=object())  # type: ignore[arg-type]
    assert cfg.color is False
    assert cfg.sources["color"] == "config"


def test_merge_valid_flags_preserve_values_and_provenance() -> None:
    """Valid overrides are normalized independently and marked as flags."""
    cfg = merge_config(
        cache_dir="flag-cache",
        cache_ttl_seconds=9.0,
        db_path="flag.db",
        request_delay=0.01,
        bulk_delay=2.0,
        export_format="JSON",
        color=False,
    )

    assert (
        cfg.cache_dir,
        cfg.cache_ttl_seconds,
        cfg.db_path,
        cfg.request_delay,
        cfg.bulk_delay,
        cfg.export_format,
        cfg.color,
    ) == ("flag-cache", 9.0, "flag.db", 0.01, 2.0, "json", False)
    assert set(cfg.sources.values()) == {"flag"}


def test_color_config_precedence() -> None:
    """Color follows config unless an explicit merge override supplies a value."""
    from_file = merge_config({"color": False})
    from_flag = merge_config({"color": False}, color=True)

    assert from_file.color is False
    assert from_file.sources["color"] == "config"
    assert from_flag.color is True
    assert from_flag.sources["color"] == "flag"


@pytest.mark.parametrize(
    ("key", "raw_value"),
    [
        pytest.param("cache_dir", "1", id="cache-dir-type"),
        pytest.param("cache_ttl_seconds", '"60"', id="ttl-type"),
        pytest.param("db_path", "true", id="db-path-type"),
        pytest.param("request_delay", "true", id="request-bool"),
        pytest.param("bulk_delay", "-1", id="bulk-negative"),
        pytest.param("export_format", '"xml"', id="format-choice"),
        pytest.param("color", '"yes"', id="color-type"),
        pytest.param("cache_ttl_seconds", "nan", id="ttl-nan"),
        pytest.param("request_delay", "inf", id="request-infinity"),
        pytest.param("bulk_delay", "-inf", id="bulk-negative-infinity"),
        pytest.param("request_delay", "9" * 400, id="request-overflow"),
    ],
)
def test_invalid_file_values_warn_and_are_ignored(tmp_path: Path, key: str, raw_value: str) -> None:
    """Wrong-type, out-of-range, and non-finite values are ignored per key."""
    path = tmp_path / "invalid.toml"
    path.write_text(f"{key} = {raw_value}\n", encoding="utf-8")

    values, warnings = load_file_config(path)

    assert values == {}
    assert len(warnings) == 1
    assert repr(key) in warnings[0]


def test_nested_table_warns_clearly(tmp_path: Path) -> None:
    """Nested TOML tables are rejected with a flat-keys message."""
    path = tmp_path / "cfg.toml"
    path.write_text('[cache]\ndir = "/tmp/x"\n', encoding="utf-8")

    values, warnings = load_file_config(path)

    assert values == {}
    assert len(warnings) == 1
    assert "nested table" in warnings[0]


@pytest.mark.parametrize(
    "invalid",
    [
        pytest.param(-1.0, id="negative"),
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="infinity"),
        pytest.param(float("-inf"), id="negative-infinity"),
        pytest.param(10**400, id="overflow"),
    ],
)
def test_merge_invalid_delay_flag_ignored(invalid: float) -> None:
    """Invalid numeric overrides preserve the configured value and source."""
    cfg = merge_config({"bulk_delay": 3.0}, bulk_delay=invalid)
    assert cfg.bulk_delay == 3.0
    assert cfg.sources["bulk_delay"] == "config"
