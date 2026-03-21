"""Dedicated edge-case and false-positive test module.

This module consolidates cross-module edge cases that do not belong to a
single unit.  Every test here covers ground that is NOT already tested in
test_tier_engine.py, test_cli.py, test_cache.py, or test_db.py.

Test organisation
-----------------
1.  Accession validation — whitespace, tab-prefix, lowercase/mixed-case PXD
2.  fileCategory near-matches — trailing spaces, SEARCHING, RESULTSET
3.  SDRF token-boundary near-misses — bare token, underscores, letters-both-sides
4.  Full pipeline → DB row verification (most critical: not tested anywhere else)
      a. Gold:    audit + study + study_files rows match expected values
      b. Silver:  audit row has tier=Silver, has_sdrf=0
      c. files_fetch_failed Bronze: tier=Bronze, files_fetch_failed=1
      d. Unverifiable (MSV): tier=Unverifiable, is_unverifiable=1
      e. Upsert: second run overwrites, does not duplicate
5.  Output formatting guard — correct ✔/✘ symbols for non-Gold tiers
"""

from __future__ import annotations

import sqlite3
from dataclasses import fields
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from pxaudit.cli import main
from pxaudit.pride_client import PrideAPIError
from pxaudit.tier_engine import AuditResult, compute_audit

# ---------------------------------------------------------------------------
# Local helpers (not in conftest because they are low-level building blocks
# used only within this module)
# ---------------------------------------------------------------------------

_FULL_PROJECT = {
    "title": "T",
    "organisms": [{"name": "Homo sapiens", "accession": "NEWT:9606"}],
    "instruments": [{"name": "Orbitrap"}],
}


def _file(name: str, category: str = "RAW") -> dict:
    return {
        "fileName": name,
        "fileCategory": {"value": category},
        "fileSizeBytes": 0,
        "publicFileLocations": [],
    }


def _result_file(name: str = "results.mzid") -> dict:
    return _file(name, "RESULT")


# ---------------------------------------------------------------------------
# Fixture: patch only the API layer; let DB operations run for real.
# Used exclusively by the pipeline DB-verification tests.
# ---------------------------------------------------------------------------


@pytest.fixture()
def _api_mocks(
    monkeypatch: pytest.MonkeyPatch,
    pride_project_gold: dict,
    pride_files_gold: list,
) -> dict:
    """Patch cache and API; leave get_or_create_db and insert_* untouched."""
    m = {
        "read_cache": MagicMock(return_value=None),
        "write_cache": MagicMock(),
        "fetch_project": MagicMock(return_value=pride_project_gold),
        "fetch_files": MagicMock(return_value=pride_files_gold),
    }
    for name, mock in m.items():
        monkeypatch.setattr(f"pxaudit.cli.{name}", mock)
    return m


# ---------------------------------------------------------------------------
# 1. Accession validation edges
# ---------------------------------------------------------------------------


def test_whitespace_only_accession_raises_value_error() -> None:
    """'   ' starts with a space, which is not alpha → ValueError."""
    with pytest.raises(ValueError, match="Invalid accession"):
        compute_audit("   ", {}, [])


def test_tab_prefix_accession_raises_value_error() -> None:
    """'\\tPXD000001' starts with a tab, which is not alpha → ValueError."""
    with pytest.raises(ValueError, match="Invalid accession"):
        compute_audit("\tPXD000001", {}, [])


def test_lowercase_pxd_routes_to_full_audit_not_unverifiable() -> None:
    """'pxd000001' starts with 'p' (alpha, passes validation).
    .upper().startswith('PXD') → True → full audit path, NOT Unverifiable."""
    r = compute_audit("pxd000001", {}, [])
    assert r.is_unverifiable is False
    assert r.tier == "None"  # empty project data → missing title/organism/instrument


def test_mixed_case_pxd_routes_to_full_audit_not_unverifiable() -> None:
    """'PxD000001' — case-insensitive prefix check must treat it as PXD."""
    r = compute_audit("PxD000001", {}, [])
    assert r.is_unverifiable is False


def test_lowercase_pxd_cli_routes_correctly(
    monkeypatch: pytest.MonkeyPatch, pride_project_gold: dict, pride_files_gold: list
) -> None:
    """CLI must also route lowercase 'pxd...' to the PXD fetch path."""
    fetch_project = MagicMock(return_value=pride_project_gold)
    monkeypatch.setattr("pxaudit.cli.read_cache", MagicMock(return_value=None))
    monkeypatch.setattr("pxaudit.cli.write_cache", MagicMock())
    monkeypatch.setattr("pxaudit.cli.fetch_project", fetch_project)
    monkeypatch.setattr("pxaudit.cli.fetch_files", MagicMock(return_value=pride_files_gold))
    monkeypatch.setattr("pxaudit.cli.get_or_create_db", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr("pxaudit.cli.insert_study", MagicMock())
    monkeypatch.setattr("pxaudit.cli.insert_study_files", MagicMock())
    monkeypatch.setattr("pxaudit.cli.insert_audit", MagicMock())

    result = CliRunner().invoke(main, ["check", "pxd000001"])
    assert result.exit_code == 0
    fetch_project.assert_called_once()


# ---------------------------------------------------------------------------
# 2. fileCategory near-matches — must NOT be recognised as result/search
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "category",
    [
        "result ",  # trailing space — casefold gives "result ", not "result"
        "SEARCHING",  # near-match on SEARCH; contains "search" but is longer
        "SEARCH ",  # trailing space
        "RESULTSET",  # RESULT as prefix of longer word
        " RESULT",  # leading space
        "re sult",  # internal space
    ],
    ids=[
        "result-trailing-space",
        "SEARCHING",
        "SEARCH-trailing-space",
        "RESULTSET",
        "RESULT-leading-space",
        "result-internal-space",
    ],
)
def test_file_category_near_matches_not_counted_as_result(category: str) -> None:
    """None of these near-matches should satisfy has_result_files."""
    files = [_file("data.raw", category)]
    r = compute_audit("PXD000001", _FULL_PROJECT, files)
    assert r.has_result_files is False


# ---------------------------------------------------------------------------
# 3. SDRF token-boundary near-misses and positives
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, expected",
    [
        ("sdrf", False),  # bare token, no extension — fallback requires tabular ext
        ("_sdrf_.tsv", True),  # underscores are not letters → boundary OK
        ("123sdrf456.tsv", True),  # digits are not letters → boundary OK
        ("xsdrfx.tsv", False),  # letter immediately on both sides
        ("prefixsdrfsuffix", False),  # letters on both sides, no separator
        ("asdrf.tsv", False),  # letter immediately before sdrf
        ("sdrf_file.tsv", True),  # sdrf followed by underscore (not a letter)
    ],
    ids=[
        "bare-token-no-ext",
        "underscore-both-sides",
        "digit-both-sides",
        "letter-both-sides",
        "letter-both-sides-no-sep",
        "letter-prefix",
        "sdrf-then-underscore",
    ],
)
def test_sdrf_token_boundary_cases(filename: str, expected: bool) -> None:
    """Exhaustive token-boundary tests for the SDRF regex.

    Positive cases: sdrf must be matched when not immediately adjacent to a letter.
    Negative cases: sdrf must NOT match when a letter touches it on either side.
    """
    files = [
        _file(filename, "OTHER"),
        _result_file(),  # ensure has_result_files so tier can reach Silver/Gold
    ]
    r = compute_audit("PXD000001", _FULL_PROJECT, files)
    assert r.has_sdrf is expected, (
        f"filename={filename!r}: expected has_sdrf={expected}, got {r.has_sdrf}"
    )


# ---------------------------------------------------------------------------
# 4. Full pipeline → DB row verification
#    These are integration-style tests: only the API layer is mocked; the DB
#    write path (get_or_create_db, insert_study, insert_study_files,
#    insert_audit) runs for real against a tmp_path SQLite file.
# ---------------------------------------------------------------------------

_AUDIT_COLS = (
    "accession",
    "tier",
    "has_title",
    "has_organism",
    "has_organism_id",
    "has_instrument",
    "has_result_files",
    "has_sdrf",
    "has_mztab",
    "files_fetch_failed",
    "is_unverifiable",
    "tier_logic_version",
)

_STUDY_COLS = (
    "accession",
    "title",
    "organism",
    "organism_id",
    "instrument",
    "submission_year",
    "keywords",
    "repository",
    "fetched_at",
)


def _read_audit(db_path: str, accession: str = "PXD000001") -> dict:
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        f"SELECT {', '.join(_AUDIT_COLS)} FROM audit WHERE accession=?", (accession,)
    ).fetchone()
    conn.close()
    assert row is not None, f"No audit row found for {accession}"
    return dict(zip(_AUDIT_COLS, row, strict=True))


def _read_study(db_path: str, accession: str = "PXD000001") -> dict:
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        f"SELECT {', '.join(_STUDY_COLS)} FROM study WHERE accession=?", (accession,)
    ).fetchone()
    conn.close()
    assert row is not None, f"No study row found for {accession}"
    return dict(zip(_STUDY_COLS, row, strict=True))


def _count_rows(db_path: str, table: str, accession: str = "PXD000001") -> int:
    conn = sqlite3.connect(db_path)
    n = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE accession=?", (accession,)).fetchone()[0]
    conn.close()
    return n


def test_pipeline_gold_audit_row_tier_and_flags(_api_mocks: dict, tmp_path: Path) -> None:
    """Gold run: audit table must have tier=Gold and all Boolean flags correct."""
    db = str(tmp_path / "audit.db")
    res = CliRunner().invoke(main, ["check", "PXD000001", "--db", db])
    assert res.exit_code == 0

    row = _read_audit(db)
    assert row["tier"] == "Gold"
    assert row["has_title"] == 1
    assert row["has_organism"] == 1
    assert row["has_organism_id"] == 1
    assert row["has_instrument"] == 1
    assert row["has_result_files"] == 1
    assert row["has_sdrf"] == 1
    assert row["has_mztab"] == 1
    assert row["files_fetch_failed"] == 0
    assert row["is_unverifiable"] == 0


def test_pipeline_gold_study_row_fields(_api_mocks: dict, tmp_path: Path) -> None:
    """Gold run: study table must have correct title, organism, organism_id, instrument, year."""
    db = str(tmp_path / "audit.db")
    CliRunner().invoke(main, ["check", "PXD000001", "--db", db])

    row = _read_study(db)
    assert row["accession"] == "PXD000001"
    assert row["title"] == "Gold tier study"
    assert row["organism"] == "Homo sapiens"
    assert row["organism_id"] == "NEWT:9606"
    assert row["instrument"] == "Orbitrap Fusion"
    assert row["submission_year"] == 2020
    assert row["repository"] == "PRIDE"


def test_pipeline_gold_study_files_row_count(
    _api_mocks: dict, tmp_path: Path, pride_files_gold: list
) -> None:
    """Gold run: study_files must have one row per file returned by the API."""
    db = str(tmp_path / "audit.db")
    CliRunner().invoke(main, ["check", "PXD000001", "--db", db])
    assert _count_rows(db, "study_files") == len(pride_files_gold)


def test_pipeline_silver_audit_row(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    pride_project_gold: dict,
    pride_files_silver: list,
) -> None:
    """Silver run: audit row must have tier=Silver and has_sdrf=0."""
    monkeypatch.setattr("pxaudit.cli.read_cache", MagicMock(return_value=None))
    monkeypatch.setattr("pxaudit.cli.write_cache", MagicMock())
    monkeypatch.setattr("pxaudit.cli.fetch_project", MagicMock(return_value=pride_project_gold))
    monkeypatch.setattr("pxaudit.cli.fetch_files", MagicMock(return_value=pride_files_silver))

    db = str(tmp_path / "audit.db")
    CliRunner().invoke(main, ["check", "PXD000001", "--db", db])

    row = _read_audit(db)
    assert row["tier"] == "Silver"
    assert row["has_sdrf"] == 0
    assert row["has_result_files"] == 1


def test_pipeline_files_fetch_failed_bronze_in_db(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, pride_project_gold: dict
) -> None:
    """Files endpoint failure: audit row must have tier=Bronze, files_fetch_failed=1."""
    monkeypatch.setattr("pxaudit.cli.read_cache", MagicMock(return_value=None))
    monkeypatch.setattr("pxaudit.cli.write_cache", MagicMock())
    monkeypatch.setattr("pxaudit.cli.fetch_project", MagicMock(return_value=pride_project_gold))
    monkeypatch.setattr("pxaudit.cli.fetch_files", MagicMock(side_effect=PrideAPIError("down")))

    db = str(tmp_path / "audit.db")
    res = CliRunner().invoke(main, ["check", "PXD000001", "--db", db])
    assert res.exit_code == 0

    row = _read_audit(db)
    assert row["tier"] == "Bronze"
    assert row["files_fetch_failed"] == 1
    assert row["has_result_files"] == 0
    assert row["has_sdrf"] == 0


def test_pipeline_unverifiable_accession_in_db(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Non-PXD accession: audit row must have tier=Unverifiable, is_unverifiable=1."""
    monkeypatch.setattr("pxaudit.cli.read_cache", MagicMock(return_value=None))
    monkeypatch.setattr("pxaudit.cli.write_cache", MagicMock())
    fetch_project = MagicMock()
    fetch_files = MagicMock()
    monkeypatch.setattr("pxaudit.cli.fetch_project", fetch_project)
    monkeypatch.setattr("pxaudit.cli.fetch_files", fetch_files)

    db = str(tmp_path / "audit.db")
    res = CliRunner().invoke(main, ["check", "MSV000001", "--db", db])
    assert res.exit_code == 0

    # No API calls must have been made for a non-PXD accession.
    fetch_project.assert_not_called()
    fetch_files.assert_not_called()

    row = _read_audit(db, accession="MSV000001")
    assert row["tier"] == "Unverifiable"
    assert row["is_unverifiable"] == 1


def test_pipeline_upsert_second_run_does_not_duplicate_rows(
    _api_mocks: dict, tmp_path: Path
) -> None:
    """Running check twice on the same accession must produce exactly 1 row in
    each of study and audit (INSERT OR REPLACE semantics)."""
    db = str(tmp_path / "audit.db")
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--db", db])
    runner.invoke(main, ["check", "PXD000001", "--db", db])

    assert _count_rows(db, "study") == 1
    assert _count_rows(db, "audit") == 1


def test_pipeline_upsert_study_files_replaced_on_second_run(
    _api_mocks: dict, tmp_path: Path, pride_files_gold: list
) -> None:
    """Second run must delete+replace study_files, not append.
    The final count must equal the API file count, not double it."""
    db = str(tmp_path / "audit.db")
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--db", db])
    runner.invoke(main, ["check", "PXD000001", "--db", db])

    assert _count_rows(db, "study_files") == len(pride_files_gold)


# ---------------------------------------------------------------------------
# 5. Output formatting — ✔/✘ symbols for non-Gold tiers
# ---------------------------------------------------------------------------


def test_silver_output_shows_cross_for_sdrf(
    monkeypatch: pytest.MonkeyPatch,
    pride_project_gold: dict,
    pride_files_silver: list,
) -> None:
    """Silver tier output must show ✘ for SDRF line and ✔ for result files."""
    monkeypatch.setattr("pxaudit.cli.read_cache", MagicMock(return_value=None))
    monkeypatch.setattr("pxaudit.cli.write_cache", MagicMock())
    monkeypatch.setattr("pxaudit.cli.fetch_project", MagicMock(return_value=pride_project_gold))
    monkeypatch.setattr("pxaudit.cli.fetch_files", MagicMock(return_value=pride_files_silver))
    monkeypatch.setattr("pxaudit.cli.get_or_create_db", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr("pxaudit.cli.insert_study", MagicMock())
    monkeypatch.setattr("pxaudit.cli.insert_study_files", MagicMock())
    monkeypatch.setattr("pxaudit.cli.insert_audit", MagicMock())

    result = CliRunner().invoke(main, ["check", "PXD000001"])
    assert "Silver" in result.output
    assert "\u2718" in result.output  # ✘ must appear (SDRF missing)
    assert "\u2714" in result.output  # ✔ must also appear (result files present)


def test_unverifiable_output_shows_tier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unverifiable output must show the tier string and not crash."""
    monkeypatch.setattr("pxaudit.cli.read_cache", MagicMock(return_value=None))
    monkeypatch.setattr("pxaudit.cli.write_cache", MagicMock())
    monkeypatch.setattr("pxaudit.cli.get_or_create_db", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr("pxaudit.cli.insert_study", MagicMock())
    monkeypatch.setattr("pxaudit.cli.insert_study_files", MagicMock())
    monkeypatch.setattr("pxaudit.cli.insert_audit", MagicMock())

    result = CliRunner().invoke(main, ["check", "MSV000001"])
    assert result.exit_code == 0
    assert "Unverifiable" in result.output
    assert "MSV000001" in result.output


# ---------------------------------------------------------------------------
# 6. AuditResult dataclass structural guard
#    Ensures the fields defined in AuditResult exactly match what the DB
#    layer expects (_AUDIT_COLS), including order.
# ---------------------------------------------------------------------------


def test_audit_result_field_names_match_audit_cols_exactly() -> None:
    """AuditResult field names must match _AUDIT_COLS from db.py in the same order.

    This test catches any rename/addition/removal in either the dataclass or
    the DB layer that would cause insert_audit to silently write wrong values.
    """
    from pxaudit.db import _AUDIT_COLS as db_cols

    dataclass_fields = tuple(f.name for f in fields(AuditResult))
    assert dataclass_fields == db_cols, (
        f"AuditResult fields {dataclass_fields} do not match db._AUDIT_COLS {db_cols}"
    )
