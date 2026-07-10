"""Tests for pxaudit.config load/merge and config show."""

from __future__ import annotations

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


def test_missing_file_returns_empty(tmp_path: Path) -> None:
    """Absent config file yields empty values and no warnings."""
    values, warnings = load_file_config(tmp_path / "missing.toml")
    assert values == {}
    assert warnings == ()


def test_load_valid_keys(tmp_path: Path) -> None:
    """Known keys load and normalize."""
    path = tmp_path / "cfg.toml"
    path.write_text(
        'cache_dir = "/tmp/pxa"\n'
        "cache_ttl_seconds = 60\n"
        'db_path = "custom.db"\n'
        "request_delay = 0.25\n"
        "bulk_delay = 2\n"
        'export_format = "TSV"\n'
    )
    values, warnings = load_file_config(path)
    assert warnings == ()
    assert values["cache_dir"] == "/tmp/pxa"
    assert values["cache_ttl_seconds"] == 60.0
    assert values["db_path"] == "custom.db"
    assert values["request_delay"] == 0.25
    assert values["bulk_delay"] == 2.0
    assert values["export_format"] == "tsv"


def test_unknown_key_warns(tmp_path: Path) -> None:
    """Unknown keys warn and are ignored."""
    path = tmp_path / "cfg.toml"
    path.write_text('db_path = "x.db"\nfancy = true\n')
    values, warnings = load_file_config(path)
    assert values == {"db_path": "x.db"}
    assert any("fancy" in w for w in warnings)


def test_wrong_type_falls_back_key(tmp_path: Path) -> None:
    """Wrong type for one key keeps other valid keys."""
    path = tmp_path / "cfg.toml"
    path.write_text('db_path = "ok.db"\nrequest_delay = "fast"\n')
    values, warnings = load_file_config(path)
    assert values == {"db_path": "ok.db"}
    assert any("request_delay" in w for w in warnings)


def test_invalid_export_format_rejected(tmp_path: Path) -> None:
    """export_format must be tsv/csv/json."""
    path = tmp_path / "cfg.toml"
    path.write_text('export_format = "xml"\n')
    values, warnings = load_file_config(path)
    assert "export_format" not in values
    assert warnings


def test_corrupt_toml_warns_with_path(tmp_path: Path) -> None:
    """Corrupt TOML falls back with path in warning."""
    path = tmp_path / "bad.toml"
    path.write_text("db_path = [unterminated\n")
    values, warnings = load_file_config(path)
    assert values == {}
    assert any(str(path) in w for w in warnings)


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
    assert "request_delay" in CONFIG_KEYS and "bulk_delay" in CONFIG_KEYS


def test_format_config_show_includes_sources() -> None:
    """config show rendering includes source tags."""
    cfg = merge_config({"db_path": "a.db"}, db_path="b.db")
    text = format_config_show(cfg)
    assert "db_path=" in text
    assert "(flag)" in text
    assert "(default)" in text


def test_default_config_path_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """PXAUDIT_CONFIG overrides default path."""
    target = tmp_path / "alt.toml"
    monkeypatch.setenv("PXAUDIT_CONFIG", str(target))
    assert default_config_path() == target


def test_default_config_path_home(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without PXAUDIT_CONFIG, path is ~/.pxaudit.toml."""
    monkeypatch.delenv("PXAUDIT_CONFIG", raising=False)
    assert default_config_path() == Path.home() / ".pxaudit.toml"


def test_config_show_cli_no_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """config show works with no config file (all defaults)."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    runner = CliRunner()
    result = runner.invoke(main, ["config", "show"])
    assert result.exit_code == 0
    for key in CONFIG_KEYS:
        assert key in result.output
    assert "(default)" in result.output


def test_config_show_cli_with_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """config show tags file-sourced keys as config."""
    cfg_path = tmp_path / "u.toml"
    cfg_path.write_text("bulk_delay = 4.5\n")
    monkeypatch.setenv("PXAUDIT_CONFIG", str(cfg_path))
    runner = CliRunner()
    result = runner.invoke(main, ["config", "show"])
    assert result.exit_code == 0
    assert "bulk_delay=4.5" in result.output
    assert "(config)" in result.output


def test_config_show_flag_cache_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Group --cache-dir appears as flag in config show."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    runner = CliRunner()
    result = runner.invoke(main, ["--cache-dir", str(tmp_path / "c"), "config", "show"])
    assert result.exit_code == 0
    assert "(flag)" in result.output
    assert "cache_dir=" in result.output


def test_oserror_on_read(tmp_path: Path) -> None:
    """Unreadable config path warns and returns empty."""
    path = tmp_path / "x.toml"
    path.write_text('db_path = "a.db"\n')
    path.chmod(0)

    try:
        values, warnings = load_file_config(path)
    finally:
        path.chmod(0o644)

    # Some environments still allow root/owner read; accept either outcome
    # but if OSError path triggers, values must be empty with warning.
    if values == {}:
        assert any("could not read" in w or "could not parse" in w for w in warnings)


def test_cache_dir_expanduser(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """cache_dir expands ~."""
    monkeypatch.setenv("HOME", str(tmp_path))
    path = tmp_path / "cfg.toml"
    path.write_text('cache_dir = "~/mycache"\n')
    values, _ = load_file_config(path)
    assert values["cache_dir"] == str(tmp_path / "mycache")


def test_export_format_none_type_ok() -> None:
    """_type_ok accepts None for export_format."""
    from pxaudit.config import _type_ok

    assert _type_ok("export_format", None) is True


def test_root_not_table(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Non-dict TOML root is rejected."""
    path = tmp_path / "x.toml"
    path.write_text("ok = 1\n")
    monkeypatch.setattr("pxaudit.config.tomllib.loads", lambda _s: [1, 2, 3])
    values, warnings = load_file_config(path)
    assert values == {}
    assert any("root must be a table" in w for w in warnings)


def test_merge_invalid_export_format_flag_ignored() -> None:
    """Invalid export_format flag does not override default."""
    cfg = merge_config(export_format="xml")
    assert cfg.export_format is None
    assert cfg.sources["export_format"] == "default"


def test_merge_request_delay_flag() -> None:
    """request_delay flag is applied."""
    cfg = merge_config(request_delay=0.01)
    assert cfg.request_delay == 0.01
    assert cfg.sources["request_delay"] == "flag"


def test_merge_cache_ttl_flag() -> None:
    """cache_ttl_seconds flag is applied."""
    cfg = merge_config(cache_ttl_seconds=9.0)
    assert cfg.cache_ttl_seconds == 9.0
    assert cfg.sources["cache_ttl_seconds"] == "flag"


def test_normalize_db_path_passthrough() -> None:
    """_normalize leaves db_path unchanged."""
    from pxaudit.config import _normalize

    assert _normalize("db_path", "a.db") == "a.db"


def test_bool_delay_rejected(tmp_path: Path) -> None:
    """Boolean delays are invalid (bool subclasses int)."""
    path = tmp_path / "cfg.toml"
    path.write_text("request_delay = true\nbulk_delay = false\n")
    values, warnings = load_file_config(path)
    assert "request_delay" not in values
    assert "bulk_delay" not in values
    assert len(warnings) >= 2


def test_negative_delay_rejected(tmp_path: Path) -> None:
    """Negative delays fall back to defaults with a warning."""
    path = tmp_path / "cfg.toml"
    path.write_text("bulk_delay = -1\nrequest_delay = 0.5\n")
    values, warnings = load_file_config(path)
    assert "bulk_delay" not in values
    assert values["request_delay"] == 0.5
    assert any("bulk_delay" in w for w in warnings)


def test_nested_table_warns_clearly(tmp_path: Path) -> None:
    """Nested TOML tables are rejected with a flat-keys message."""
    path = tmp_path / "cfg.toml"
    path.write_text('[cache]\ndir = "/tmp/x"\n')
    values, warnings = load_file_config(path)
    assert values == {}
    assert any("nested table" in w for w in warnings)


def test_merge_negative_delay_flag_ignored() -> None:
    """Negative delay flags do not override prior values."""
    cfg = merge_config({"bulk_delay": 3.0}, bulk_delay=-1.0)
    assert cfg.bulk_delay == 3.0
    assert cfg.sources["bulk_delay"] == "config"
