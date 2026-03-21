"""Smoke tests for the C01 scaffold: importability and version pin."""

import pxaudit
import pxaudit.cache
import pxaudit.cli
import pxaudit.db
import pxaudit.pride_client
import pxaudit.tier_engine


def test_version() -> None:
    assert pxaudit.__version__ == "0.1.0"


def test_all_modules_importable() -> None:
    """All stub modules must be importable without errors."""
    for mod in (
        pxaudit.cache,
        pxaudit.cli,
        pxaudit.db,
        pxaudit.pride_client,
        pxaudit.tier_engine,
    ):
        assert mod is not None


def test_cli_entrypoint_exists() -> None:
    """The CLI entry point must be a callable (Click group)."""
    assert callable(pxaudit.cli.main)
