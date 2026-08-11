"""Tests for the repository-side GitHub Wiki validator."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

_REPOSITORY_ROOT = Path(__file__).parents[1]
_WIKI_CHECK = _REPOSITORY_ROOT / ".github/scripts/wiki_check.py"


def _run_validator(wiki_dir: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_WIKI_CHECK), str(wiki_dir)],
        check=False,
        capture_output=True,
        text=True,
    )


def _write_pages(wiki_dir: Path, **pages: str) -> None:
    wiki_dir.mkdir()
    for filename, content in {
        "Home.md": "# Home\n",
        "_Sidebar.md": "- [[Home]]\n",
        "_Footer.md": "Footer\n",
        **pages,
    }.items():
        (wiki_dir / filename).write_text(content, encoding="utf-8")


def test_current_wiki_passes() -> None:
    result = _run_validator(_REPOSITORY_ROOT / "wiki")

    assert result.returncode == 0, result.stderr
    assert "validated 8 wiki pages" in result.stdout


def test_resolves_px_audit_space_and_hyphen_page_names(tmp_path: Path) -> None:
    _write_pages(
        tmp_path / "wiki",
        **{
            "Tier-System.md": "See [[Home|the home page]] and [Home](Home#overview).\n",
            "Code.md": "```markdown\n[[Missing]]\n```\n`[[Missing]]`\n",
        },
    )

    result = _run_validator(tmp_path / "wiki")

    assert result.returncode == 0, result.stderr


def test_rejects_missing_page_and_repository_style_link(tmp_path: Path) -> None:
    _write_pages(tmp_path / "wiki", **{"Home.md": "See [[Missing]] and [Home](Home.md).\n"})

    result = _run_validator(tmp_path / "wiki")

    assert result.returncode == 1
    assert "links to a missing wiki page: Missing" in result.stderr
    assert "repository-style .md wiki link: Home.md" in result.stderr


@pytest.mark.parametrize("bad_entry", ["notes.txt", "assets"])
def test_rejects_non_markdown_top_level_entries(tmp_path: Path, bad_entry: str) -> None:
    wiki_dir = tmp_path / "wiki"
    _write_pages(wiki_dir)
    bad_path = wiki_dir / bad_entry
    if bad_entry == "assets":
        bad_path.mkdir()
    else:
        bad_path.write_text("not a wiki page\n", encoding="utf-8")

    result = _run_validator(wiki_dir)

    assert result.returncode == 1
    assert "regular top-level files" in result.stderr or "Markdown pages only" in result.stderr
