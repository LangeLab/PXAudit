"""Cross-module edge workflows using mocked APIs and real temporary storage."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Literal
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from pxaudit.cli import main
from pxaudit.pride_client import PrideAPIError


@pytest.fixture()
def _api_mocks(
    monkeypatch: pytest.MonkeyPatch,
    pride_project_complete_metadata: dict[str, object],
    pride_files_psi_sdrf: list[dict[str, object]],
) -> dict[str, MagicMock]:
    """Mock remote and cache boundaries while retaining real SQLite writes."""
    mocks = {
        "read_cache_response": MagicMock(return_value=None),
        "read_cache_stale_response": MagicMock(return_value=None),
        "write_cache": MagicMock(),
        "fetch_project": MagicMock(return_value=pride_project_complete_metadata),
        "fetch_files": MagicMock(return_value=pride_files_psi_sdrf),
    }
    for name, mock in mocks.items():
        monkeypatch.setattr(f"pxaudit.cli.{name}", mock)
    return mocks


def _read_row(
    database: str | Path,
    table: Literal["audit", "study"],
    accession: str = "PXD000001",
) -> dict[str, object]:
    """Read one persisted audit or study row by accession."""
    with closing(sqlite3.connect(database)) as connection:
        connection.row_factory = sqlite3.Row
        query = {
            "audit": "SELECT * FROM audit WHERE accession = ?",
            "study": "SELECT * FROM study WHERE accession = ?",
        }[table]
        row = connection.execute(query, (accession,)).fetchone()
    assert row is not None, f"no {table} row found for {accession}"
    return dict(row)


def _read_file_names(database: str | Path, accession: str = "PXD000001") -> list[str]:
    """Read persisted filenames in deterministic order."""
    with closing(sqlite3.connect(database)) as connection:
        rows = connection.execute(
            "SELECT file_name FROM study_files WHERE accession = ? ORDER BY file_name",
            (accession,),
        ).fetchall()
    return [row[0] for row in rows]


def _database_snapshot(database: str | Path) -> tuple[list[tuple[object, ...]], ...]:
    """Return every persisted row in deterministic table order."""
    with closing(sqlite3.connect(database)) as connection:
        return (
            connection.execute("SELECT * FROM study ORDER BY accession").fetchall(),
            connection.execute("SELECT * FROM study_files ORDER BY file_name").fetchall(),
            connection.execute("SELECT * FROM audit ORDER BY accession").fetchall(),
        )


@pytest.mark.component
def test_pipeline_success_persists_aligned_record(
    _api_mocks: dict[str, MagicMock],
    tmp_path: Path,
    pride_files_psi_sdrf: list[dict[str, object]],
) -> None:
    """A successful audit persists aligned study, file, and audit components."""
    database = tmp_path / "audit.db"

    result = CliRunner().invoke(main, ["check", "PXD000001", "--db", str(database)])

    assert result.exit_code == 0
    assert _read_row(database, "audit") == {
        "accession": "PXD000001",
        "tier": "Gold",
        "has_title": "passed",
        "has_organism": "passed",
        "has_organism_id": "passed",
        "has_instrument": "passed",
        "has_result_files": "passed",
        "has_psi_results": "passed",
        "has_open_spectra": "failed",
        "has_organism_part": "unknown",
        "has_publication": "unknown",
        "has_tabular_quant": "failed",
        "has_quant_metadata": "unknown",
        "has_sdrf": "passed",
        "has_mztab": "passed",
        "files_fetch_failed": 0,
        "is_unverifiable": 0,
        "ambiguity_count": 3,
        "tier_logic_version": "v3.0",
        "quant_tier": "Partial",
    }
    study = _read_row(database, "study")
    expected_study = {
        "accession": "PXD000001",
        "title": "Complete metadata study",
        "organism": "Homo sapiens",
        "organism_id": "NEWT:9606",
        "instrument": "Orbitrap Fusion",
        "submission_year": 2020,
        "submission_type": None,
        "keywords": "proteomics, phospho",
        "repository": "PRIDE",
    }
    assert {name: study[name] for name in expected_study} == expected_study
    assert isinstance(study["fetched_at"], str)
    assert _read_file_names(database) == sorted(
        str(file_data["fileName"]) for file_data in pride_files_psi_sdrf
    )


@pytest.mark.component
def test_pipeline_case_variants_share_one_canonical_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    pride_project_complete_metadata: dict[str, object],
    pride_files_psi_sdrf: list[dict[str, object]],
) -> None:
    """Case variants share one API, cache, study, file, and audit identity."""
    fetch_project = MagicMock(return_value=pride_project_complete_metadata)
    fetch_files = MagicMock(return_value=pride_files_psi_sdrf)
    monkeypatch.setattr("pxaudit.cli.fetch_project", fetch_project)
    monkeypatch.setattr("pxaudit.cli.fetch_files", fetch_files)
    database = tmp_path / "audit.db"
    cache = tmp_path / "cache"
    runner = CliRunner()

    first = runner.invoke(
        main,
        ["--cache-dir", str(cache), "check", "pxd000001", "--db", str(database)],
    )
    second = runner.invoke(
        main,
        ["--cache-dir", str(cache), "check", "PxD000001", "--db", str(database)],
    )

    assert first.exit_code == second.exit_code == 0
    fetch_project.assert_called_once_with("PXD000001", delay=0.5)
    fetch_files.assert_called_once_with("PXD000001", delay=0.5)
    assert sorted(path.name for path in cache.iterdir()) == [
        "PXD000001_files.json",
        "PXD000001_project.json",
    ]
    assert _read_row(database, "study")["accession"] == "PXD000001"
    assert _read_row(database, "audit")["accession"] == "PXD000001"
    with closing(sqlite3.connect(database)) as connection:
        identities = connection.execute("SELECT DISTINCT accession FROM study_files").fetchall()
    assert identities == [("PXD000001",)]


@pytest.mark.component
def test_pipeline_category_only_result_does_not_persist_psi_evidence(
    _api_mocks: dict[str, MagicMock], tmp_path: Path
) -> None:
    """A generic PRIDE RESULT persists processed evidence without a PSI claim."""
    _api_mocks["fetch_files"].return_value = [
        {
            "fileName": "results.csv",
            "fileCategory": {"value": "RESULT"},
            "fileSizeBytes": 10,
            "publicFileLocations": [],
        }
    ]
    database = tmp_path / "audit.db"

    result = CliRunner().invoke(main, ["check", "PXD000001", "--db", str(database)])

    assert result.exit_code == 0
    audit = _read_row(database, "audit")
    assert audit["tier"] == "Bronze"
    assert audit["has_result_files"] == "passed"
    assert audit["has_psi_results"] == "failed"
    assert audit["tier_logic_version"] == "v3.0"
    assert _read_file_names(database) == ["results.csv"]


@pytest.mark.component
def test_pipeline_new_accession_files_failure_creates_no_database(
    _api_mocks: dict[str, MagicMock],
    tmp_path: Path,
) -> None:
    """An incomplete new audit creates no database or misleading tier output."""
    _api_mocks["fetch_files"].side_effect = PrideAPIError("down")
    database = tmp_path / "audit.db"

    result = CliRunner().invoke(main, ["check", "PXD000001", "--db", str(database)])

    assert result.exit_code == 1
    assert "Warning" in result.stderr
    assert "audit is incomplete" in result.stderr
    assert "Tier" not in result.stdout
    assert not database.exists()


@pytest.mark.component
def test_pipeline_verified_empty_files_persists_completed_raw_audit(
    _api_mocks: dict[str, MagicMock], tmp_path: Path
) -> None:
    """A successful empty response remains distinct from unavailable file evidence."""
    _api_mocks["fetch_files"].return_value = []
    database = tmp_path / "audit.db"

    result = CliRunner().invoke(main, ["check", "PXD000001", "--db", str(database)])

    assert result.exit_code == 0
    assert "Raw" in result.stdout
    assert _read_file_names(database) == []
    assert _read_row(database, "study")["accession"] == "PXD000001"
    audit = _read_row(database, "audit")
    assert audit["tier"] == "Raw"
    assert audit["files_fetch_failed"] == 0


@pytest.mark.component
def test_pipeline_files_failure_preserves_database_and_manifest(
    _api_mocks: dict[str, MagicMock], tmp_path: Path
) -> None:
    """An incomplete repeat audit preserves every prior row and manifest byte."""
    database = tmp_path / "audit.db"
    runner = CliRunner()
    first = runner.invoke(main, ["check", "PXD000001", "--db", str(database)])
    assert first.exit_code == 0
    rows_before = _database_snapshot(database)
    manifest_before = runner.invoke(
        main,
        ["manifest", "PXD000001", "--db", str(database)],
    )
    _api_mocks["fetch_files"].side_effect = PrideAPIError("down")

    failed = runner.invoke(
        main,
        ["check", "PXD000001", "--db", str(database), "--no-cache"],
    )
    manifest_after = runner.invoke(
        main,
        ["manifest", "PXD000001", "--db", str(database)],
    )

    assert failed.exit_code == 1
    assert "Warning" in failed.stderr
    assert "Tier" not in failed.stdout
    assert manifest_before.exit_code == manifest_after.exit_code == 0
    assert manifest_after.stdout_bytes == manifest_before.stdout_bytes
    assert _database_snapshot(database) == rows_before


@pytest.mark.component
def test_pipeline_unverifiable_accession_persists_truthful_provenance(
    _api_mocks: dict[str, MagicMock], tmp_path: Path
) -> None:
    """A non-PRIDE audit persists its inferred repository without remote calls."""
    database = tmp_path / "audit.db"

    result = CliRunner().invoke(main, ["check", "MSV000001", "--db", str(database)])

    assert result.exit_code == 0
    for boundary in (
        "read_cache_response",
        "read_cache_stale_response",
        "write_cache",
        "fetch_project",
        "fetch_files",
    ):
        _api_mocks[boundary].assert_not_called()
    audit = _read_row(database, "audit", "MSV000001")
    assert audit["tier"] == "Unverifiable"
    assert audit["is_unverifiable"] == 1
    study = _read_row(database, "study", "MSV000001")
    assert study["fetched_at"] is None
    assert study["repository"] == "MassIVE"


@pytest.mark.component
def test_pipeline_repeat_audit_replaces_each_component(
    _api_mocks: dict[str, MagicMock],
    tmp_path: Path,
    pride_project_complete_metadata: dict[str, object],
) -> None:
    """A repeated audit replaces rather than duplicates every persisted component."""
    database = tmp_path / "audit.db"
    runner = CliRunner()
    first = runner.invoke(main, ["check", "PXD000001", "--db", str(database)])
    assert first.exit_code == 0
    _api_mocks["fetch_project"].return_value = {
        **pride_project_complete_metadata,
        "title": "Replacement study",
    }
    _api_mocks["fetch_files"].return_value = [
        {
            "fileName": "replacement.mzid",
            "fileCategory": {"value": "RESULT"},
            "fileSizeBytes": 1,
            "publicFileLocations": [],
        }
    ]

    second = runner.invoke(main, ["check", "PXD000001", "--db", str(database)])

    assert second.exit_code == 0
    snapshot = _database_snapshot(database)
    assert tuple(len(rows) for rows in snapshot) == (1, 1, 1)
    assert _read_row(database, "study")["title"] == "Replacement study"
    assert _read_file_names(database) == ["replacement.mzid"]
