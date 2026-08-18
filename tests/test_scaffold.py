"""Installed package metadata, module discovery, and entry-point contracts."""

from __future__ import annotations

import importlib
import importlib.metadata as importlib_metadata
import pkgutil
import runpy
import tomllib
from pathlib import Path
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

import pxaudit
from pxaudit.cli import main

_PROJECT = tomllib.loads(
    (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
)["project"]


def test_distribution_metadata_matches_project_version() -> None:
    """Repository, installed distribution, and public package expose one version."""
    expected = _PROJECT["version"]

    assert pxaudit.__version__ == importlib_metadata.version("pxaudit") == expected


def test_console_script_resolves_to_click_group() -> None:
    """The configured ``pxaudit`` script resolves and reports the project version."""
    configured_entry_point = _PROJECT["scripts"]["pxaudit"]
    matches = [
        entry_point
        for entry_point in importlib_metadata.entry_points(group="console_scripts")
        if entry_point.name == "pxaudit"
    ]

    assert configured_entry_point == "pxaudit.cli:main"
    assert [entry_point.value for entry_point in matches] == [configured_entry_point]
    assert matches[0].load() is main
    result = CliRunner().invoke(main, ["--version"], prog_name="pxaudit")
    assert result.exit_code == 0
    assert result.output == f"pxaudit, version {_PROJECT['version']}\n"


def test_project_urls_cover_public_release_links() -> None:
    """Project metadata exposes stable homepage, repository, issue, and changelog links."""
    assert _PROJECT["urls"] == {
        "Homepage": "https://github.com/LangeLab/PXAudit",
        "Repository": "https://github.com/LangeLab/PXAudit",
        "Issues": "https://github.com/LangeLab/PXAudit/issues",
        "Changelog": "https://github.com/LangeLab/PXAudit/blob/main/CHANGELOG.md",
    }


def test_discovered_package_modules_are_importable() -> None:
    """Every module shipped beneath :mod:`pxaudit` imports successfully."""
    module_names = [
        module.name
        for module in pkgutil.walk_packages(pxaudit.__path__, prefix=f"{pxaudit.__name__}.")
    ]

    assert module_names
    for module_name in module_names:
        importlib.import_module(module_name)


def test_missing_distribution_uses_source_tree_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Package initialization falls back when distribution metadata is absent."""
    monkeypatch.setattr(
        importlib_metadata,
        "version",
        Mock(side_effect=importlib_metadata.PackageNotFoundError("pxaudit")),
    )

    namespace = runpy.run_path(pxaudit.__file__)

    assert namespace["__version__"] == "0.0.0"


def test_broken_distribution_metadata_is_not_hidden(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unexpected metadata failures remain visible instead of impersonating a source checkout."""
    monkeypatch.setattr(
        importlib_metadata,
        "version",
        Mock(side_effect=RuntimeError("metadata database is unreadable")),
    )

    with pytest.raises(RuntimeError, match="metadata database is unreadable"):
        runpy.run_path(pxaudit.__file__)
