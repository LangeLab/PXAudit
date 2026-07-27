"""Command-line orchestration, persistence, and output contract tests."""

from __future__ import annotations

import gc
import json
import os
import sqlite3
import sys
import weakref
from collections.abc import Iterable, Iterator
from contextlib import closing
from io import StringIO
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest
from click.testing import CliRunner

from pxaudit.cache import CachedResponse, CacheSafetyError, CacheWriteError, write_cache
from pxaudit.cli import (
    AuditData,
    _default_export_path,
    _export_csv,
    _export_json,
    _export_tsv,
    _extract_files_df,
    _extract_study,
    _read_accessions,
    _result_to_row,
    _write_export,
    main,
)
from pxaudit.pride_client import PrideAPIError
from pxaudit.tier_engine import AuditResult

_GOLD_PROJECT: dict = {
    "title": "TMT spikes study",
    "submissionDate": "2020-01-15",
    "keywords": ["proteomics", "phospho"],
    "organisms": [{"@type": "CvParam", "name": "Homo sapiens", "accession": "NEWT:9606"}],
    "instruments": [{"@type": "CvParam", "name": "Orbitrap Fusion"}],
}

_DIAMOND_PROJECT: dict = {
    "title": "Diamond study",
    "submissionDate": "2021-06-01",
    "submissionType": "COMPLETE",
    "keywords": ["proteomics"],
    "organisms": [{"@type": "CvParam", "name": "Homo sapiens", "accession": "NEWT:9606"}],
    "instruments": [{"@type": "CvParam", "name": "Orbitrap Fusion"}],
    "organismParts": [{"name": "brain"}],
    "references": [{"pubmedID": 12345}],
    "quantificationMethods": [{"name": "iTRAQ"}],
}
_DIAMOND_FILES: list[dict] = [
    {
        "fileName": "results.mzid",
        "fileCategory": {"@type": "CvParam", "value": "RESULT"},
        "fileSizeBytes": 1024,
        "publicFileLocations": [],
    },
    {
        "fileName": "run1.mzML",
        "fileCategory": {"@type": "CvParam", "value": "PEAK"},
        "fileSizeBytes": 2048,
        "publicFileLocations": [],
    },
    {
        "fileName": "sdrf.tsv",
        "fileCategory": {"@type": "CvParam", "value": "EXPERIMENTAL DESIGN"},
        "fileSizeBytes": 512,
        "publicFileLocations": [],
    },
    {
        "fileName": "results.mzTab",
        "fileCategory": {"@type": "CvParam", "value": "RESULT"},
        "fileSizeBytes": 256,
        "publicFileLocations": [
            {"name": "FTP Protocol", "value": "ftp://ftp.ebi.ac.uk/results.mzTab"},
        ],
    },
    {
        "fileName": "proteinGroups.txt",
        "fileCategory": {"@type": "CvParam", "value": "OTHER"},
        "fileSizeBytes": 1024,
        "publicFileLocations": [],
    },
]

_GOLD_FILES: list[dict] = [
    {
        "fileName": "results.mzid",
        "fileCategory": {"@type": "CvParam", "value": "RESULT"},
        "fileSizeBytes": 1024,
        "publicFileLocations": [
            {"name": "FTP Protocol", "value": "ftp://ftp.ebi.ac.uk/results.mzid"},
        ],
    },
    {
        "fileName": "sdrf.tsv",
        "fileCategory": {"@type": "CvParam", "value": "OTHER"},
        "fileSizeBytes": 512,
        "publicFileLocations": [],
    },
    {
        "fileName": "results.mzTab",
        "fileCategory": {"@type": "CvParam", "value": "RESULT"},
        "fileSizeBytes": 256,
        "publicFileLocations": [],
    },
]


def _cached(
    data: dict | list,
    *,
    retrieved_at: str = "2026-01-01T00:00:00+00:00",
    snapshot_id: str | None = "test-snapshot",
    age: float = 0.0,
) -> CachedResponse:
    """Build deterministic cache provenance for CLI orchestration tests."""
    return CachedResponse(data, retrieved_at, snapshot_id, age)


@pytest.fixture()
def mocks(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Patch external I/O for CLI orchestration tests.

    Default behaviour: cache miss, successful API fetch of _GOLD_PROJECT and
    _GOLD_FILES, no-op DB writes.  Individual tests override specific mocks
    as needed via monkeypatch within the test body.
    """
    m: dict = {
        "read_cache_response": MagicMock(return_value=None),
        "read_cache_stale_response": MagicMock(return_value=None),
        "write_cache": MagicMock(),
        "fetch_project": MagicMock(return_value=_GOLD_PROJECT),
        "fetch_files": MagicMock(return_value=_GOLD_FILES),
        "get_or_create_db": MagicMock(return_value=MagicMock()),
        "insert_audit_record": MagicMock(),
    }
    for name, mock in m.items():
        monkeypatch.setattr(f"pxaudit.cli.{name}", mock)
    return m


@pytest.mark.parametrize(
    "bad",
    ["", "PXD12345", "PXDABCDEF", "MSV/000001", "PXD" + "0" * 62],
)
def test_check_invalid_accession_exits_two(bad: str, mocks: dict) -> None:
    """Malformed or unsafe accessions exit 2 before any I/O."""
    runner = CliRunner()
    result = runner.invoke(main, ["check", bad])
    assert result.exit_code == 2
    mocks["fetch_project"].assert_not_called()
    mocks["fetch_files"].assert_not_called()


def test_check_cache_miss_emits_complete_summary_and_persists(mocks: dict) -> None:
    """A successful cache miss reports the audit and persists both responses."""
    result = CliRunner().invoke(main, ["check", "PXD000001"])

    assert result.exit_code == 0
    for expected in ("PXD000001", "Gold", "Quant Tier", "Partial", "\u2714"):
        assert expected in result.output
    assert mocks["write_cache"].call_count == 2
    mocks["insert_audit_record"].assert_called_once()


def test_check_mixed_case_uses_canonical_identity_at_every_boundary(mocks: dict) -> None:
    """Cache, API, persistence, and output receive one uppercase accession."""
    result = CliRunner().invoke(main, ["check", " PxD000001 "])

    assert result.exit_code == 0
    assert "PXD000001" in result.stdout
    assert mocks["read_cache_response"].call_args_list[0].args[:2] == (
        "PXD000001",
        "project",
    )
    mocks["fetch_project"].assert_called_once_with("PXD000001", delay=0.5)
    study = mocks["insert_audit_record"].call_args.args[1]
    assert study["accession"] == "PXD000001"
    assert mocks["insert_audit_record"].call_args.args[2] == "PXD000001"


def test_check_diamond_stdout_no_crossmarks(mocks: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """A Diamond audit prints check symbols without cross symbols."""
    monkeypatch.setattr("pxaudit.cli.fetch_project", MagicMock(return_value=_DIAMOND_PROJECT))
    monkeypatch.setattr("pxaudit.cli.fetch_files", MagicMock(return_value=_DIAMOND_FILES))
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert result.exit_code == 0
    assert "Diamond" in result.output
    assert "\u2714" in result.output
    assert "\u2718" not in result.output


def test_check_project_api_failure_is_clean_error(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A project endpoint failure exits one with a concise error."""
    monkeypatch.setattr(
        "pxaudit.cli.fetch_project", MagicMock(side_effect=PrideAPIError("server error"))
    )
    result = CliRunner().invoke(main, ["check", "PXD000001"])

    assert result.exit_code == 1
    assert "Error" in result.stderr


def test_check_files_api_failure_is_incomplete_without_computing_or_persisting(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unavailable files evidence exits nonzero before tier computation or persistence."""
    compute = MagicMock()
    monkeypatch.setattr(
        "pxaudit.cli.fetch_files", MagicMock(side_effect=PrideAPIError("files down"))
    )
    monkeypatch.setattr("pxaudit.cli.compute_audit", compute)

    result = CliRunner().invoke(main, ["check", "PXD000001"])

    assert result.exit_code == 1
    assert "Warning" in result.stderr
    assert "audit is incomplete" in result.stderr
    assert "no database records were created or replaced" in result.stderr
    assert "Tier" not in result.stdout
    compute.assert_not_called()
    assert mocks["write_cache"].call_count == 1
    mocks["get_or_create_db"].assert_not_called()
    mocks["insert_audit_record"].assert_not_called()


def test_check_project_cache_hit_skips_fetch_project(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cached project response avoids the project API fetch."""
    monkeypatch.setattr(
        "pxaudit.cli.read_cache_response",
        MagicMock(
            side_effect=lambda acc, ep, **kw: _cached(_GOLD_PROJECT) if ep == "project" else None
        ),
    )
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001"])
    mocks["fetch_project"].assert_not_called()


def test_check_files_cache_hit_skips_fetch_files(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cached file response avoids the files API fetch."""
    monkeypatch.setattr(
        "pxaudit.cli.read_cache_response",
        MagicMock(
            side_effect=lambda acc, ep, **kw: _cached(_GOLD_FILES) if ep == "files" else None
        ),
    )
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001"])
    mocks["fetch_files"].assert_not_called()


def test_check_both_cached_no_api_calls(mocks: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """A full cache hit avoids both API fetch functions."""
    monkeypatch.setattr(
        "pxaudit.cli.read_cache_response",
        MagicMock(
            side_effect=lambda acc, ep, **kw: _cached(
                _GOLD_PROJECT if ep == "project" else _GOLD_FILES
            )
        ),
    )
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert result.exit_code == 0
    mocks["fetch_project"].assert_not_called()
    mocks["fetch_files"].assert_not_called()


def test_check_rejects_malformed_cached_payload_without_traceback(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Malformed cached project data becomes a clean audit error."""
    malformed_project = {**_GOLD_PROJECT, "organisms": ["not-a-CV-param"]}
    monkeypatch.setattr(
        "pxaudit.cli.read_cache_response",
        MagicMock(
            side_effect=lambda acc, endpoint, **kw: _cached(
                malformed_project if endpoint == "project" else _GOLD_FILES
            )
        ),
    )

    result = CliRunner().invoke(main, ["check", "PXD000001"])

    assert result.exit_code == 1
    assert "invalid organisms data" in result.stderr
    assert isinstance(result.exception, SystemExit)
    mocks["fetch_project"].assert_not_called()
    mocks["fetch_files"].assert_not_called()


def test_check_full_cache_hit_preserves_project_retrieval_time(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cache hit persists the original project retrieval time as ``fetched_at``."""
    project_retrieved_at = "2025-09-10T11:12:13+00:00"
    monkeypatch.setattr(
        "pxaudit.cli.read_cache_response",
        MagicMock(
            side_effect=lambda acc, ep, **kw: _cached(
                _GOLD_PROJECT if ep == "project" else _GOLD_FILES,
                retrieved_at=project_retrieved_at
                if ep == "project"
                else "2025-09-10T11:12:14+00:00",
            )
        ),
    )

    result = CliRunner().invoke(main, ["check", "PXD000001"])

    assert result.exit_code == 0
    study = mocks["insert_audit_record"].call_args.args[1]
    assert study["fetched_at"] == project_retrieved_at


@pytest.mark.component
def test_full_cache_hit_persists_retrieval_time_with_real_cache_and_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real cache-to-SQLite workflow preserves project retrieval provenance."""
    cache_dir = tmp_path / "cache"
    database = tmp_path / "audit.db"
    config = tmp_path / "missing.toml"
    project_retrieved_at = "2025-08-09T10:11:12+00:00"
    write_cache(
        "PXD000001",
        "project",
        _GOLD_PROJECT,
        cache_dir=cache_dir,
        retrieved_at=project_retrieved_at,
        snapshot_id="component-snapshot",
    )
    write_cache(
        "PXD000001",
        "files",
        _GOLD_FILES,
        cache_dir=cache_dir,
        retrieved_at="2025-08-09T10:11:13+00:00",
        snapshot_id="component-snapshot",
    )
    fetch_project_mock = MagicMock()
    fetch_files_mock = MagicMock()
    monkeypatch.setattr("pxaudit.cli.fetch_project", fetch_project_mock)
    monkeypatch.setattr("pxaudit.cli.fetch_files", fetch_files_mock)
    monkeypatch.setenv("PXAUDIT_CONFIG", str(config))

    result = CliRunner().invoke(
        main,
        [
            "--cache-dir",
            str(cache_dir),
            "check",
            "PXD000001",
            "--db",
            str(database),
        ],
    )

    assert result.exit_code == 0
    fetch_project_mock.assert_not_called()
    fetch_files_mock.assert_not_called()
    with closing(sqlite3.connect(database)) as connection:
        stored = connection.execute(
            "SELECT fetched_at FROM study WHERE accession = ?", ("PXD000001",)
        ).fetchone()
    assert stored == (project_retrieved_at,)
    assert "different or unverified snapshots" not in result.output


@pytest.mark.parametrize("global_flags", [[], ["-q"], ["-v"]])
def test_check_mixed_snapshot_warning_survives_all_output_modes(
    mocks: dict,
    monkeypatch: pytest.MonkeyPatch,
    global_flags: list[str],
) -> None:
    """Default, quiet, and verbose modes all report mixed endpoint provenance."""
    monkeypatch.setattr(
        "pxaudit.cli.read_cache_response",
        MagicMock(
            side_effect=lambda acc, ep, **kw: _cached(
                _GOLD_PROJECT if ep == "project" else _GOLD_FILES,
                retrieved_at="2025-01-01T00:00:00+00:00"
                if ep == "project"
                else "2025-02-01T00:00:00+00:00",
                snapshot_id=f"{ep}-snapshot",
            )
        ),
    )

    result = CliRunner().invoke(main, [*global_flags, "check", "PXD000001"])

    assert result.exit_code == 0
    assert "different or unverified snapshots" in result.stderr
    assert "project retrieved 2025-01-01" in result.stderr
    assert "files retrieved 2025-02-01" in result.stderr


def test_check_matching_snapshot_has_no_mixed_warning(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Responses carrying one snapshot identifier do not produce a provenance warning."""
    monkeypatch.setattr(
        "pxaudit.cli.read_cache_response",
        MagicMock(
            side_effect=lambda acc, ep, **kw: _cached(
                _GOLD_PROJECT if ep == "project" else _GOLD_FILES
            )
        ),
    )

    result = CliRunner().invoke(main, ["check", "PXD000001"])

    assert result.exit_code == 0
    assert "different or unverified snapshots" not in result.output


@pytest.mark.parametrize("snapshot_id", [None, "   "])
def test_check_missing_snapshot_provenance_warns(
    mocks: dict, monkeypatch: pytest.MonkeyPatch, snapshot_id: str | None
) -> None:
    """Missing or blank snapshot identifiers are reported as unverified."""
    monkeypatch.setattr(
        "pxaudit.cli.read_cache_response",
        MagicMock(
            side_effect=lambda acc, ep, **kw: _cached(
                _GOLD_PROJECT if ep == "project" else _GOLD_FILES,
                snapshot_id=snapshot_id,
            )
        ),
    )

    result = CliRunner().invoke(main, ["check", "PXD000001"])

    assert result.exit_code == 0
    assert "different or unverified snapshots" in result.stderr


def test_check_no_cache_bypasses_all_cache_io(mocks: dict) -> None:
    """Disabled cache mode fetches live responses without cache I/O."""
    result = CliRunner().invoke(main, ["check", "PXD000001", "--no-cache"])

    assert result.exit_code == 0
    mocks["read_cache_response"].assert_not_called()
    mocks["fetch_project"].assert_called_once()
    mocks["fetch_files"].assert_called_once()
    mocks["read_cache_stale_response"].assert_not_called()
    mocks["write_cache"].assert_not_called()


@pytest.mark.parametrize(
    ("failed_fetch", "expected_exit"),
    [("fetch_project", 1), ("fetch_files", 1)],
)
def test_check_no_cache_failure_never_uses_cache(
    mocks: dict, failed_fetch: str, expected_exit: int
) -> None:
    """Disabled cache mode makes no cache call after either endpoint fails."""
    mocks[failed_fetch].side_effect = PrideAPIError("down")

    result = CliRunner().invoke(main, ["check", "PXD000001", "--no-cache"])

    assert result.exit_code == expected_exit
    mocks["read_cache_response"].assert_not_called()
    mocks["read_cache_stale_response"].assert_not_called()
    mocks["write_cache"].assert_not_called()


def test_check_refresh_skips_reads_then_fetches_and_writes(mocks: dict) -> None:
    """Refresh skips fresh reads but fetches live responses and writes successes."""
    result = CliRunner().invoke(main, ["check", "PXD000001", "--refresh"])

    assert result.exit_code == 0
    mocks["read_cache_response"].assert_not_called()
    mocks["fetch_project"].assert_called_once()
    mocks["fetch_files"].assert_called_once()
    assert mocks["write_cache"].call_count == 2
    project_write, files_write = mocks["write_cache"].call_args_list
    assert project_write.kwargs["snapshot_id"] == files_write.kwargs["snapshot_id"]
    assert project_write.kwargs["retrieved_at"]
    assert files_write.kwargs["retrieved_at"]


def test_check_refresh_with_no_cache_combined(mocks: dict) -> None:
    """--refresh combined with --no-cache must skip writes (--no-cache wins)."""
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--refresh", "--no-cache"])
    mocks["read_cache_response"].assert_not_called()
    mocks["read_cache_stale_response"].assert_not_called()
    mocks["fetch_project"].assert_called_once()
    mocks["fetch_files"].assert_called_once()
    mocks["write_cache"].assert_not_called()


def test_check_refresh_project_failure_uses_stale_cache(mocks: dict) -> None:
    """Refresh falls back to stale project data only after the live request fails."""
    mocks["fetch_project"].side_effect = PrideAPIError("down")
    mocks["read_cache_stale_response"].return_value = _cached(
        _GOLD_PROJECT,
        retrieved_at="2025-12-01T00:00:00+00:00",
        snapshot_id="old-snapshot",
        age=3600.0,
    )

    result = CliRunner().invoke(main, ["check", "PXD000001", "--refresh"])

    assert result.exit_code == 0
    mocks["read_cache_response"].assert_not_called()
    mocks["read_cache_stale_response"].assert_called_once()
    assert "cache age: 3600s" in result.output
    assert "different or unverified snapshots" in result.output


def test_check_refresh_files_failure_uses_stale_cache(mocks: dict) -> None:
    """Refresh falls back to stale files data only after the live request fails."""
    mocks["fetch_files"].side_effect = PrideAPIError("down")
    mocks["read_cache_stale_response"].return_value = _cached(
        _GOLD_FILES,
        retrieved_at="2025-12-01T00:00:00+00:00",
        snapshot_id="old-snapshot",
        age=7200.0,
    )

    result = CliRunner().invoke(main, ["check", "PXD000001", "--refresh"])

    assert result.exit_code == 0
    mocks["read_cache_response"].assert_not_called()
    mocks["read_cache_stale_response"].assert_called_once()
    assert mocks["read_cache_stale_response"].call_args.args[1] == "files"
    assert "cache age: 7200s" in result.output
    assert "different or unverified snapshots" in result.output


def test_check_refresh_project_failure_without_stale_exits_one(mocks: dict) -> None:
    """Refresh reports project failure when no stale response is available."""
    mocks["fetch_project"].side_effect = PrideAPIError("down")

    result = CliRunner().invoke(main, ["check", "PXD000001", "--refresh"])

    assert result.exit_code == 1
    mocks["read_cache_response"].assert_not_called()
    mocks["read_cache_stale_response"].assert_called_once()
    mocks["write_cache"].assert_not_called()


def test_check_refresh_files_failure_without_stale_does_not_cache_failure(
    mocks: dict,
) -> None:
    """Refresh writes the project response but never caches a failed files response."""
    mocks["fetch_files"].side_effect = PrideAPIError("down")

    result = CliRunner().invoke(main, ["check", "PXD000001", "--refresh"])

    assert result.exit_code == 1
    mocks["read_cache_response"].assert_not_called()
    mocks["read_cache_stale_response"].assert_called_once()
    mocks["write_cache"].assert_called_once()
    assert mocks["write_cache"].call_args.args[1] == "project"


def test_check_db_path_forwarded_to_get_or_create_db(mocks: dict, tmp_path: Path) -> None:
    """--db value must be passed verbatim to get_or_create_db."""
    db_path = str(tmp_path / "audit.db")
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--db", db_path])
    mocks["get_or_create_db"].assert_called_once_with(db_path)


def test_check_conn_closed_after_inserts(mocks: dict) -> None:
    """Connection must be closed regardless of insert outcome."""
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001"])
    mocks["get_or_create_db"].return_value.close.assert_called_once()


def test_check_non_pxd_exits_zero(mocks: dict) -> None:
    """Non-PXD accessions are Unverifiable : exit 0."""
    runner = CliRunner()
    result = runner.invoke(main, ["check", "MSV000001"])
    assert result.exit_code == 0
    assert "Unverifiable" in result.output


def test_check_non_pxd_makes_no_api_calls(mocks: dict) -> None:
    """A non-PXD accession avoids cache reads and API calls."""
    runner = CliRunner()
    runner.invoke(main, ["check", "MSV000001"])
    mocks["read_cache_response"].assert_not_called()
    mocks["fetch_project"].assert_not_called()
    mocks["fetch_files"].assert_not_called()


def test_extract_study_all_fields_populated() -> None:
    """A complete project payload populates every study field."""
    project = {
        "title": "Test study",
        "submissionDate": "2019-06-01",
        "submissionType": "COMPLETE",
        "keywords": ["proteomics"],
        "organisms": [{"name": "Homo sapiens", "accession": "NEWT:9606"}],
        "instruments": [{"name": "Orbitrap Fusion"}],
    }
    row = _extract_study("PXD000001", project, "2026-01-01T00:00:00+00:00")
    assert row["accession"] == "PXD000001"
    assert row["title"] == "Test study"
    assert row["organism"] == "Homo sapiens"
    assert row["organism_id"] == "NEWT:9606"
    assert row["instrument"] == "Orbitrap Fusion"
    assert row["submission_year"] == 2019
    assert row["submission_type"] == "COMPLETE"
    assert row["keywords"] == "proteomics"
    assert row["repository"] == "PRIDE"
    assert row["fetched_at"] == "2026-01-01T00:00:00+00:00"


def test_extract_study_empty_optional_collections_give_none() -> None:
    """Empty optional PRIDE collections map to nullable database fields."""
    row = _extract_study("PXD000001", {"organisms": [], "instruments": [], "keywords": []}, "ts")
    assert row["organism"] is None
    assert row["organism_id"] is None
    assert row["instrument"] is None
    assert row["keywords"] is None


@pytest.mark.parametrize("submission_date", [None, "not-a-date", "20", "abcd-ef-gh"])
def test_extract_study_invalid_or_missing_date_gives_none_year(
    submission_date: str | None,
) -> None:
    """Missing and malformed submission dates remain safely nullable."""
    project = {} if submission_date is None else {"submissionDate": submission_date}
    assert _extract_study("PXD000001", project, "ts")["submission_year"] is None


def test_extract_study_multi_keyword_joined() -> None:
    """Multiple keywords are joined with a comma and space."""
    row = _extract_study("PXD000001", {"keywords": ["a", "b", "c"]}, "ts")
    assert row["keywords"] == "a, b, c"


@pytest.mark.parametrize(
    ("accession", "repository"),
    [
        ("PXD999", "PRIDE"),
        ("MSV000001", "MassIVE"),
        ("JPST000001", "jPOST"),
        ("IPX000001", "iProX"),
        ("MTBLS000001", None),
    ],
)
def test_extract_study_records_truthful_non_pride_repository(
    accession: str, repository: str | None
) -> None:
    """Recognized partner prefixes are inferred without claiming a PRIDE fetch."""
    assert _extract_study(accession, {}, None)["repository"] == repository


def test_extract_study_submission_type_extracted() -> None:
    """A submission type is retained in the extracted study row."""
    row = _extract_study("PXD000001", {"submissionType": "PARTIAL"}, "ts")
    assert row["submission_type"] == "PARTIAL"


def test_extract_study_missing_submission_type_gives_none() -> None:
    """A missing submission type produces a nullable field."""
    row = _extract_study("PXD000001", {}, "ts")
    assert row["submission_type"] is None


def test_extract_files_df_empty_gives_empty_dataframe() -> None:
    """An empty file list produces a zero-row frame with the export schema."""
    df = _extract_files_df("PXD000001", [])
    assert len(df) == 0
    assert list(df.columns) == [
        "accession",
        "file_name",
        "file_category",
        "file_extension",
        "ftp_location",
        "file_size",
        "checksum",
        "checksum_type",
    ]


def test_extract_files_df_maps_complete_payload() -> None:
    """A populated response preserves its shape, identity, and FTP location."""
    df = _extract_files_df("PXD000001", _GOLD_FILES)
    assert len(df) == len(_GOLD_FILES)
    assert set(df.columns) == {
        "accession",
        "file_name",
        "file_category",
        "file_extension",
        "ftp_location",
        "file_size",
        "checksum",
        "checksum_type",
    }
    assert (df["accession"] == "PXD000001").all()
    assert df.loc[0, "ftp_location"] == "ftp://ftp.ebi.ac.uk/results.mzid"


@pytest.mark.parametrize(
    ("payload", "checksum", "checksum_type"),
    [
        ({"fileChecksum": "a" * 32}, "a" * 32, "MD5"),
        ({"checksum": "b" * 40}, "b" * 40, "SHA-1"),
        ({"checksum": "c" * 64}, "c" * 64, "SHA-256"),
        ({"checksum": "not-a-declared-hash"}, "not-a-declared-hash", None),
        ({"checksum": 1234}, None, None),
        ({}, None, None),
    ],
)
def test_extract_files_df_normalizes_checksum_metadata(
    payload: dict, checksum: str | None, checksum_type: str | None
) -> None:
    """Checksum variants receive values and algorithm labels only when defensible."""
    df = _extract_files_df("PXD000001", [{"fileName": "result.mzid", **payload}])
    stored_checksum = df.loc[0, "checksum"]
    stored_type = df.loc[0, "checksum_type"]
    assert stored_checksum == checksum or (checksum is None and pd.isna(stored_checksum))
    assert stored_type == checksum_type or (checksum_type is None and pd.isna(stored_type))


def test_extract_files_df_no_ftp_gives_none() -> None:
    """A file without an FTP location produces a nullable location field."""
    file_no_ftp = {
        "fileName": "raw.raw",
        "fileCategory": {"value": "RAW"},
        "fileSizeBytes": 2048,
        "publicFileLocations": [
            {"name": "Aspera Protocol", "value": "prd@fasp.ebi.ac.uk:/raw.raw"}
        ],
    }
    df = _extract_files_df("PXD000001", [file_no_ftp])
    assert df.loc[0, "ftp_location"] is None


def test_extract_files_df_extension_derived_from_filename() -> None:
    """The extracted extension preserves the filename suffix."""
    file = {
        "fileName": "results.mzTab",
        "fileCategory": {"value": "RESULT"},
        "fileSizeBytes": 100,
        "publicFileLocations": [],
    }
    df = _extract_files_df("PXD000001", [file])
    assert df.loc[0, "file_extension"] == ".mzTab"


def test_extract_files_df_missing_filename_gives_empty_name() -> None:
    """A missing filename produces an empty name and nullable extension."""
    file = {"fileCategory": {"value": "RAW"}, "fileSizeBytes": 0, "publicFileLocations": []}
    df = _extract_files_df("PXD000001", [file])
    assert df.loc[0, "file_name"] == ""
    assert df.loc[0, "file_extension"] is None


def test_check_keyboard_interrupt_during_fetch_exits_130(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ctrl+C during fetch_project must print 'Interrupted.' and exit 130."""
    monkeypatch.setattr(
        "pxaudit.cli.fetch_project",
        MagicMock(side_effect=KeyboardInterrupt),
    )
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert result.exit_code == 130
    assert "Interrupted." in result.output


def test_check_keyboard_interrupt_before_db_clean_close(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ctrl+C before DB insert still calls conn.close via finally."""

    def _interrupt(*args: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr("pxaudit.cli.insert_audit_record", _interrupt)
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert result.exit_code == 130
    assert "Interrupted." in result.output
    mocks["get_or_create_db"].return_value.close.assert_called_once()


def test_check_database_failure_is_clean_runtime_error(mocks: dict) -> None:
    """A persistence failure exits 1 without exposing an uncaught exception."""
    mocks["get_or_create_db"].side_effect = sqlite3.DatabaseError("database unavailable")

    result = CliRunner().invoke(main, ["check", "PXD000001"])

    assert result.exit_code == 1
    assert "audit failed" in result.stderr
    assert isinstance(result.exception, SystemExit)


@pytest.mark.parametrize("format_name", ["tsv", "json"])
def test_default_export_path_uses_requested_extension(format_name: str) -> None:
    """Default export names contain the date and requested extension."""
    path = _default_export_path(format_name)
    assert path.startswith("pxaudit_bulk_")
    assert path.endswith(f".{format_name}")
    assert len(path) == len(f"pxaudit_bulk_20260525.{format_name}")


def test_read_accessions_file(tmp_path: Path) -> None:
    """Read accessions from a file, skipping blanks and comments."""
    f = tmp_path / "accessions.txt"
    f.write_text("PXD000001\n\n# comment\nPXD000002\n  PXD000003  \n", encoding="utf-8")
    result = _read_accessions(str(f))
    assert result == [(1, "PXD000001"), (4, "PXD000002"), (5, "PXD000003")]


def test_read_accessions_all_blank(tmp_path: Path) -> None:
    """Blank and comment-only files produce no accession records."""
    f = tmp_path / "empty.txt"
    f.write_text("# only comment\n\n  \n", encoding="utf-8")
    result = _read_accessions(str(f))
    assert result == []


def test_read_accessions_file_not_found(tmp_path: Path) -> None:
    """A missing input file remains distinguishable from empty input."""
    with pytest.raises(FileNotFoundError):
        _read_accessions(str(tmp_path / "nope.txt"))


def test_read_accessions_stdin_preserves_source_line_numbers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stdin records retain physical line numbers after blank and comment filtering."""
    monkeypatch.setattr(sys, "stdin", StringIO("# note\npxd000001\n"))
    assert _read_accessions("-") == [(2, "pxd000001")]


def test_result_to_row_keys_match_export_cols() -> None:
    """Export rows preserve the public column order and primary fields."""
    from pxaudit.cli import _EXPORT_COLS

    result = AuditResult(accession="PXD000001", tier="Diamond")
    row = _result_to_row(result)
    assert list(row.keys()) == list(_EXPORT_COLS)
    assert row["accession"] == "PXD000001"
    assert row["tier"] == "Diamond"


def test_export_tsv(tmp_path: Path) -> None:
    """TSV export writes its header and every result."""
    results = [
        AuditResult(accession="PXD000001", tier="Gold", quant_tier="Partial"),
        AuditResult(accession="PXD000002", tier="Diamond", quant_tier="Quant-Complete"),
    ]
    path = str(tmp_path / "out.tsv")
    _export_tsv(results, path)
    content = Path(path).read_text()
    assert "PXD000001" in content
    assert "PXD000002" in content
    assert "Gold" in content
    assert "Diamond" in content
    lines = content.splitlines()
    assert lines[0].startswith("accession")


def test_export_csv(tmp_path: Path) -> None:
    """CSV export uses comma-delimited rows."""
    results = [AuditResult(accession="PXD000001", tier="Raw")]
    path = str(tmp_path / "out.csv")
    _export_csv(results, path)
    content = Path(path).read_text()
    assert "PXD000001" in content
    assert "," in content


def test_export_json(tmp_path: Path) -> None:
    """JSON export emits structured result objects."""
    results = [AuditResult(accession="PXD000001", tier="Diamond")]
    path = str(tmp_path / "out.json")
    _export_json(results, path)
    data = json.loads(Path(path).read_text())
    assert len(data) == 1
    assert data[0]["accession"] == "PXD000001"
    assert data[0]["tier"] == "Diamond"


def test_write_export_failure_preserves_existing_target(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A failed export leaves the previous target and no temporary artifact."""
    output = tmp_path / "results.tsv"
    output.write_text("previous\n", encoding="utf-8")

    def fail_export(results: list[AuditResult], path: str) -> None:
        Path(path).write_text("partial\n", encoding="utf-8")
        raise OSError("disk full")

    monkeypatch.setattr("pxaudit.cli._export_tsv", fail_export)

    with pytest.raises(OSError, match="disk full"):
        _write_export([AuditResult(accession="PXD000001", tier="Gold")], str(output), "tsv")

    assert output.read_text(encoding="utf-8") == "previous\n"
    assert list(tmp_path.glob(f".{output.name}.*.tmp")) == []


@pytest.fixture()
def bulk_mocks(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Patch _audit_single and time.sleep to skip real I/O during bulk-audit tests.

    Returns results for two accessions: PXD000001 (Gold, Partial)
    and PXD000002 (Diamond, Quant-Complete).
    """
    results = {
        "PXD000001": AuditResult(accession="PXD000001", tier="Gold", quant_tier="Partial"),
        "PXD000002": AuditResult(
            accession="PXD000002", tier="Diamond", quant_tier="Quant-Complete"
        ),
    }

    def fake_audit(accession: str, db_path: str, **kw: object) -> tuple:
        if accession == "PXD000001":
            r = results["PXD000001"]
        elif accession == "PXD000002":
            r = results["PXD000002"]
        elif accession.upper().startswith("MSV"):
            r = AuditResult(
                accession=accession,
                tier="Unverifiable",
                is_unverifiable=True,
                quant_tier="Unverifiable",
            )
        else:
            raise PrideAPIError(f"unknown {accession}")
        return AuditData(r, {}, MagicMock(), [], "2026-01-01T00:00:00+00:00", [], [], True)

    m: dict = {
        "_audit_single": MagicMock(side_effect=fake_audit),
        "get_or_create_db": MagicMock(return_value=MagicMock()),
    }
    monkeypatch.setattr("pxaudit.cli._audit_single", m["_audit_single"])
    monkeypatch.setattr("pxaudit.cli.get_or_create_db", m["get_or_create_db"])
    monkeypatch.setattr("pxaudit.cli.time.sleep", lambda _: None)
    return m


def _write_bulk_cache(cache_dir: Path, accessions: list[str]) -> None:
    """Write deterministic project and file responses for real bulk component tests."""
    for accession in accessions:
        write_cache(
            accession,
            "project",
            _DIAMOND_PROJECT,
            cache_dir=cache_dir,
            retrieved_at="2026-01-01T00:00:00+00:00",
            snapshot_id=f"snapshot-{accession}",
        )
        write_cache(
            accession,
            "files",
            _DIAMOND_FILES,
            cache_dir=cache_dir,
            retrieved_at="2026-01-01T00:00:00+00:00",
            snapshot_id=f"snapshot-{accession}",
        )


def _bulk_database_snapshot(database: Path) -> tuple[list[tuple], dict[str, list[tuple]]]:
    """Return normalized schema and rows for a bulk semantic comparison."""
    with closing(sqlite3.connect(database)) as connection:
        schema = connection.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE type IN ('table', 'index') ORDER BY type, name"
        ).fetchall()
        tables = {
            table: connection.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
            for table in ("study", "study_files", "audit")
        }
    return schema, tables


def test_bulk_batch_success_matches_per_accession_semantics(tmp_path: Path) -> None:
    """Successful batching preserves schema, rows, and TSV output exactly."""
    accessions = ["PXD000001", "PXD000002", "PXD000003"]
    reference_root = tmp_path / "reference"
    candidate_root = tmp_path / "candidate"
    for root in (reference_root, candidate_root):
        _write_bulk_cache(root / "cache", accessions)
        (root / "accessions.txt").write_text("\n".join(accessions) + "\n")

    runner = CliRunner()
    reference_export = reference_root / "results.tsv"
    candidate_export = candidate_root / "results.tsv"
    reference = runner.invoke(
        main,
        [
            "--cache-dir",
            str(reference_root / "cache"),
            "bulk-audit",
            "--input",
            str(reference_root / "accessions.txt"),
            "--db",
            str(reference_root / "results.db"),
            "--format",
            "tsv",
            "--output",
            str(reference_export),
            "--delay",
            "0",
        ],
    )
    candidate = runner.invoke(
        main,
        [
            "--cache-dir",
            str(candidate_root / "cache"),
            "bulk-audit",
            "--input",
            str(candidate_root / "accessions.txt"),
            "--db",
            str(candidate_root / "results.db"),
            "--format",
            "tsv",
            "--output",
            str(candidate_export),
            "--delay",
            "0",
            "--batch-size",
            "2",
        ],
    )

    assert reference.exit_code == candidate.exit_code == 0
    assert _bulk_database_snapshot(reference_root / "results.db") == _bulk_database_snapshot(
        candidate_root / "results.db"
    )
    assert reference_export.read_bytes() == candidate_export.read_bytes()


def test_bulk_batch_failure_contract_compares_partial_side_effects(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A batch rollback differs from per-accession progress only at its documented boundary."""
    from pxaudit.cli import _audit_single as real_audit

    accessions = ["PXD000001", "PXD000002"]
    roots = {batch_size: tmp_path / f"batch-{batch_size}" for batch_size in (1, 2)}
    for root in roots.values():
        _write_bulk_cache(root / "cache", accessions)
        (root / "accessions.txt").write_text("\n".join(accessions) + "\n")

    def fail_second(accession: str, db_path: str, **kwargs: Any) -> AuditData:
        if accession == "PXD000002":
            raise PrideAPIError("second accession unavailable")
        return real_audit(accession, db_path, **kwargs)

    monkeypatch.setattr("pxaudit.cli._audit_single", fail_second)
    runner = CliRunner()
    results: dict[int, Any] = {}
    exports: dict[int, Path] = {}
    for batch_size, root in roots.items():
        export = root / "results.tsv"
        exports[batch_size] = export
        results[batch_size] = runner.invoke(
            main,
            [
                "--cache-dir",
                str(root / "cache"),
                "bulk-audit",
                "--input",
                str(root / "accessions.txt"),
                "--db",
                str(root / "results.db"),
                "--format",
                "tsv",
                "--output",
                str(export),
                "--batch-size",
                str(batch_size),
            ],
        )

    reference = results[1]
    candidate = results[2]
    assert reference.exit_code == candidate.exit_code == 1
    assert exports[1].exists()
    assert not exports[2].exists()
    reference_schema, reference_tables = _bulk_database_snapshot(roots[1] / "results.db")
    candidate_schema, candidate_tables = _bulk_database_snapshot(roots[2] / "results.db")
    assert reference_schema == candidate_schema
    assert len(reference_tables["audit"]) == 1
    assert candidate_tables["audit"] == []
    assert "rolled_back=1" in candidate.output


def test_bulk_audit_happy_path_tsv(bulk_mocks: dict, tmp_path: Path) -> None:
    """A two-accession TSV audit succeeds and writes both rows."""
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\nPXD000002\n")
    out_path = tmp_path / "out.tsv"
    runner = CliRunner()
    result = runner.invoke(
        main, ["bulk-audit", "--input", str(acc_file), "--format", "tsv", "--output", str(out_path)]
    )
    assert result.exit_code == 0
    assert "Completed : 2" in result.output
    assert "Failed    : 0" in result.output
    assert out_path.exists()
    content = out_path.read_text()
    assert "PXD000001" in content
    assert "PXD000002" in content


def test_bulk_audit_reuses_one_database_connection(bulk_mocks: dict, tmp_path: Path) -> None:
    """Bulk persistence shares setup while each accession remains independently audited."""
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\nPXD000002\n")

    result = CliRunner().invoke(main, ["bulk-audit", "--input", str(acc_file)])

    assert result.exit_code == 0
    bulk_mocks["get_or_create_db"].assert_called_once()
    connection = bulk_mocks["get_or_create_db"].return_value
    connection.close.assert_called_once()
    calls = bulk_mocks["_audit_single"].call_args_list
    assert len(calls) == 2
    assert all(call.kwargs["db_connection"] is connection for call in calls)


def test_bulk_audit_releases_per_accession_audit_payloads(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Bulk orchestration does not retain DataFrames from completed accessions."""
    accessions = ["PXD000001", "PXD000002", "PXD000003"]
    input_path = tmp_path / "accessions.txt"
    input_path.write_text("\n".join(accessions) + "\n")
    payload_refs: list[weakref.ReferenceType[pd.DataFrame]] = []

    def fake_audit(accession: str, db_path: str, **_kwargs: Any) -> AuditData:
        payload = pd.DataFrame({"accession": [accession], "file_name": ["result.mzid"]})
        payload_refs.append(weakref.ref(payload))
        result = AuditResult(accession=accession, tier="Gold")
        return AuditData(result, {}, payload, [], "2026-01-01T00:00:00+00:00", [], [], False)

    monkeypatch.setattr("pxaudit.cli._audit_single", fake_audit)
    monkeypatch.setattr("pxaudit.cli.get_or_create_db", MagicMock(return_value=MagicMock()))
    result = CliRunner().invoke(main, ["bulk-audit", "--input", str(input_path)])
    gc.collect()

    assert result.exit_code == 0
    assert len(payload_refs) == len(accessions)
    assert all(reference() is None for reference in payload_refs)


def test_bulk_batch_persists_real_cached_rows_and_commits_before_network_delay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An explicit batch size persists real rows while applying network delay."""
    from pxaudit.cli import _audit_single as real_audit

    accessions = ["PXD000001", "PXD000002"]
    cache_dir = tmp_path / "cache"
    _write_bulk_cache(cache_dir, accessions)
    input_path = tmp_path / "accessions.txt"
    input_path.write_text("\n".join(accessions) + "\n")
    database = tmp_path / "results.db"
    sleeps: list[float] = []

    def audit_with_network_marker(accession: str, db_path: str, **kwargs: Any) -> AuditData:
        data = real_audit(accession, db_path, **kwargs)
        return data._replace(network_used=accession == "PXD000001")

    monkeypatch.setattr("pxaudit.cli._audit_single", audit_with_network_marker)
    monkeypatch.setattr("pxaudit.cli.time.sleep", lambda seconds: sleeps.append(seconds))
    result = CliRunner().invoke(
        main,
        [
            "--cache-dir",
            str(cache_dir),
            "bulk-audit",
            "--input",
            str(input_path),
            "--db",
            str(database),
            "--batch-size",
            "2",
            "--delay",
            "1.5",
        ],
    )

    assert result.exit_code == 0
    assert sleeps == [1.5]
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM study").fetchone() == (2,)
        assert connection.execute("SELECT COUNT(*) FROM audit").fetchone() == (2,)


def test_bulk_batch_network_delay_preserves_active_batch_rollback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Network politeness delay does not commit an incomplete active batch."""
    from pxaudit.cli import _audit_single as real_audit

    accessions = ["PXD000001", "PXD000002", "PXD000003"]
    cache_dir = tmp_path / "cache"
    _write_bulk_cache(cache_dir, accessions)
    input_path = tmp_path / "accessions.txt"
    input_path.write_text("\n".join(accessions) + "\n")
    database = tmp_path / "results.db"

    def fail_third(accession: str, db_path: str, **kwargs: Any) -> AuditData:
        if accession == "PXD000003":
            raise PrideAPIError("third accession unavailable")
        return real_audit(accession, db_path, **kwargs)._replace(network_used=True)

    monkeypatch.setattr("pxaudit.cli._audit_single", fail_third)
    result = CliRunner().invoke(
        main,
        [
            "--cache-dir",
            str(cache_dir),
            "bulk-audit",
            "--input",
            str(input_path),
            "--db",
            str(database),
            "--batch-size",
            "3",
        ],
    )

    assert result.exit_code == 1
    assert "committed=0 rolled_back=2" in result.output
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM audit").fetchone() == (0,)


def test_bulk_batch_continue_commits_pending_accessions_after_api_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Continue-on-error commits pending successes before skipping a failed accession."""
    from pxaudit.cli import _audit_single as real_audit

    accessions = ["PXD000001", "PXD000002"]
    cache_dir = tmp_path / "cache"
    _write_bulk_cache(cache_dir, accessions)
    input_path = tmp_path / "accessions.txt"
    input_path.write_text("\n".join(accessions) + "\n")
    database = tmp_path / "results.db"

    def fail_second(accession: str, db_path: str, **kwargs: Any) -> AuditData:
        if accession == "PXD000002":
            raise PrideAPIError("second accession unavailable")
        return real_audit(accession, db_path, **kwargs)

    monkeypatch.setattr("pxaudit.cli._audit_single", fail_second)
    result = CliRunner().invoke(
        main,
        [
            "--cache-dir",
            str(cache_dir),
            "bulk-audit",
            "--input",
            str(input_path),
            "--db",
            str(database),
            "--batch-size",
            "2",
            "--continue-on-error",
        ],
    )

    assert result.exit_code == 0
    assert "Completed : 1" in result.output
    assert "Failed    : 1" in result.output
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM audit").fetchone() == (1,)


def test_bulk_batch_stop_rolls_back_pending_accessions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Stopping on an API failure rolls back the active batch and preserves no pending rows."""
    from pxaudit.cli import _audit_single as real_audit

    accessions = ["PXD000001", "PXD000002"]
    cache_dir = tmp_path / "cache"
    _write_bulk_cache(cache_dir, accessions)
    input_path = tmp_path / "accessions.txt"
    input_path.write_text("\n".join(accessions) + "\n")
    database = tmp_path / "results.db"
    export = tmp_path / "partial.tsv"

    def fail_second(accession: str, db_path: str, **kwargs: Any) -> AuditData:
        if accession == "PXD000002":
            raise PrideAPIError("second accession unavailable")
        return real_audit(accession, db_path, **kwargs)

    monkeypatch.setattr("pxaudit.cli._audit_single", fail_second)
    result = CliRunner().invoke(
        main,
        [
            "--cache-dir",
            str(cache_dir),
            "bulk-audit",
            "--input",
            str(input_path),
            "--db",
            str(database),
            "--batch-size",
            "2",
            "--format",
            "tsv",
            "--output",
            str(export),
        ],
    )

    assert result.exit_code == 1
    assert "rolled_back=1" in result.output
    assert not export.exists()
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM audit").fetchone() == (0,)


def test_bulk_batch_stop_reports_zero_rollback_without_pending_rows(
    bulk_mocks: dict, tmp_path: Path
) -> None:
    """A failure before the first write reports an empty active batch cleanly."""
    bulk_mocks["_audit_single"].side_effect = PrideAPIError("first accession unavailable")
    input_path = tmp_path / "accessions.txt"
    input_path.write_text("PXD000001\n")

    result = CliRunner().invoke(
        main,
        ["bulk-audit", "--input", str(input_path), "--batch-size", "2"],
    )

    assert result.exit_code == 1
    assert "committed=0 rolled_back=0" in result.output


def test_bulk_batch_interrupt_rolls_back_pending_accessions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Interrupting a batch rolls back pending rows while retaining earlier committed batches."""
    from pxaudit.cli import _audit_single as real_audit

    accessions = ["PXD000001", "PXD000002", "PXD000003"]
    cache_dir = tmp_path / "cache"
    _write_bulk_cache(cache_dir, accessions)
    input_path = tmp_path / "accessions.txt"
    input_path.write_text("\n".join(accessions) + "\n")
    database = tmp_path / "results.db"

    def interrupt_second(accession: str, db_path: str, **kwargs: Any) -> AuditData:
        if accession == "PXD000002":
            raise KeyboardInterrupt
        return real_audit(accession, db_path, **kwargs)

    monkeypatch.setattr("pxaudit.cli._audit_single", interrupt_second)
    result = CliRunner().invoke(
        main,
        [
            "--cache-dir",
            str(cache_dir),
            "bulk-audit",
            "--input",
            str(input_path),
            "--db",
            str(database),
            "--batch-size",
            "2",
        ],
    )

    assert result.exit_code == 130
    assert "rolled_back=1" in result.output
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM audit").fetchone() == (0,)


def test_bulk_batch_database_error_reports_and_rolls_back(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A persistence error rolls back the active batch and reports its progress."""
    from pxaudit.cli import _audit_single as real_audit
    from pxaudit.cli import insert_audit_record as real_insert

    accessions = ["PXD000001", "PXD000002"]
    cache_dir = tmp_path / "cache"
    _write_bulk_cache(cache_dir, accessions)
    input_path = tmp_path / "accessions.txt"
    input_path.write_text("\n".join(accessions) + "\n")
    database = tmp_path / "results.db"
    insert_calls = 0

    def fail_second_insert(*args: Any, **kwargs: Any) -> None:
        nonlocal insert_calls
        insert_calls += 1
        if insert_calls == 2:
            raise sqlite3.IntegrityError("synthetic write failure")
        real_insert(*args, **kwargs)

    monkeypatch.setattr("pxaudit.cli._audit_single", real_audit)
    monkeypatch.setattr("pxaudit.cli.insert_audit_record", fail_second_insert)
    result = CliRunner().invoke(
        main,
        [
            "--cache-dir",
            str(cache_dir),
            "bulk-audit",
            "--input",
            str(input_path),
            "--db",
            str(database),
            "--batch-size",
            "2",
        ],
    )

    assert result.exit_code == 1
    assert "committed=0 rolled_back=1" in result.output
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM audit").fetchone() == (0,)


def test_bulk_batch_disk_full_preserves_committed_batches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A failed batch commit reports progress and preserves earlier committed rows."""
    from pxaudit.cli import _audit_single as real_audit
    from pxaudit.db import TransactionBatch

    accessions = ["PXD000001", "PXD000002", "PXD000003"]
    cache_dir = tmp_path / "cache"
    _write_bulk_cache(cache_dir, accessions)
    input_path = tmp_path / "accessions.txt"
    input_path.write_text("\n".join(accessions) + "\n")
    database = tmp_path / "results.db"
    commit_calls = 0
    real_commit = TransactionBatch.commit

    def fail_final_commit(batch: TransactionBatch) -> None:
        nonlocal commit_calls
        commit_calls += 1
        if commit_calls == 2:
            raise sqlite3.OperationalError("database or disk is full")
        real_commit(batch)

    monkeypatch.setattr("pxaudit.cli._audit_single", real_audit)
    monkeypatch.setattr(TransactionBatch, "commit", fail_final_commit)
    result = CliRunner().invoke(
        main,
        [
            "--cache-dir",
            str(cache_dir),
            "bulk-audit",
            "--input",
            str(input_path),
            "--db",
            str(database),
            "--batch-size",
            "2",
        ],
    )

    assert result.exit_code == 1
    assert "database or disk is full" in result.output
    assert "committed=2 rolled_back=1" in result.output
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM audit").fetchone() == (2,)


def test_bulk_audit_database_open_failure_closes_no_connection(
    bulk_mocks: dict, tmp_path: Path
) -> None:
    """A database setup failure is reported without entering the accession loop."""
    bulk_mocks["get_or_create_db"].side_effect = sqlite3.DatabaseError("database unavailable")
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\n")

    result = CliRunner().invoke(main, ["bulk-audit", "--input", str(acc_file)])

    assert result.exit_code == 1
    assert "bulk audit failed" in result.stderr
    bulk_mocks["_audit_single"].assert_not_called()


def test_release_smoke_check_bulk_manifest_report(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The documented check-to-report workflow succeeds in a clean temporary directory."""
    monkeypatch.setattr("pxaudit.cli.fetch_project", MagicMock(return_value=_GOLD_PROJECT))
    monkeypatch.setattr("pxaudit.cli.fetch_files", MagicMock(return_value=_GOLD_FILES))

    db_path = tmp_path / "results.db"
    accessions = tmp_path / "accessions.txt"
    export_path = tmp_path / "bulk.tsv"
    report_dir = tmp_path / "report"
    accessions.write_text("PXD000001\n", encoding="utf-8")
    runner = CliRunner()

    check = runner.invoke(main, ["check", "PXD000001", "--db", str(db_path)])
    bulk = runner.invoke(
        main,
        [
            "bulk-audit",
            "--input",
            str(accessions),
            "--db",
            str(db_path),
            "--format",
            "tsv",
            "--output",
            str(export_path),
            "--delay",
            "0",
        ],
    )
    manifest = runner.invoke(main, ["manifest", "PXD000001", "--db", str(db_path)])
    report = runner.invoke(main, ["report", "--db", str(db_path), "--output", str(report_dir)])

    assert check.exit_code == 0, check.output
    assert bulk.exit_code == 0, bulk.output
    assert manifest.exit_code == 0, manifest.output
    assert report.exit_code == 0, report.output
    assert db_path.is_file()
    assert "PXD000001" in export_path.read_text(encoding="utf-8")
    assert "results.mzid" in manifest.output
    report_html = report_dir / "report.html"
    assert report_html.is_file()
    assert "PXAudit version" in report_html.read_text(encoding="utf-8")


def test_bulk_audit_happy_path_json(bulk_mocks: dict, tmp_path: Path) -> None:
    """A JSON bulk export contains the canonical accession and computed tier."""
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("pxd000001\n")
    out_path = tmp_path / "out.json"
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file), "--format", "json", "--output", str(out_path)],
    )
    assert result.exit_code == 0
    data = json.loads(out_path.read_text())
    assert len(data) == 1
    assert data[0]["accession"] == "PXD000001"
    assert data[0]["tier"] == "Gold"


def test_bulk_audit_happy_path_csv(bulk_mocks: dict, tmp_path: Path) -> None:
    """A CSV bulk export contains the audited accession in delimited output."""
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\n")
    out_path = tmp_path / "out.csv"
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file), "--format", "csv", "--output", str(out_path)],
    )
    assert result.exit_code == 0
    content = out_path.read_text()
    assert "PXD000001" in content
    assert "," in content


def test_bulk_audit_default_export_path(bulk_mocks: dict, tmp_path: Path) -> None:
    """A missing output option selects the generated default filename."""
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\n")
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            main,
            ["bulk-audit", "--input", str(acc_file), "--format", "tsv"],
        )
    assert result.exit_code == 0
    assert "pxaudit_bulk_" in result.output


def test_bulk_audit_continue_on_error(bulk_mocks: dict, tmp_path: Path) -> None:
    """--continue-on-error skips failures, includes them in summary."""
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\nUNKNOWN_ACC\nPXD000002\n")
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file), "--continue-on-error"],
    )
    assert result.exit_code == 0
    assert "Completed : 2" in result.output
    assert "Failed    : 1" in result.output


def test_bulk_audit_continue_on_incomplete_audit_records_failure(
    bulk_mocks: dict, tmp_path: Path
) -> None:
    """An incomplete audit participates in the per-accession continue-on-error contract."""
    from pxaudit.cli import _IncompleteAuditError

    bulk_mocks["_audit_single"].side_effect = _IncompleteAuditError(
        "files response unavailable; no database records were created or replaced"
    )
    accessions = tmp_path / "ids.txt"
    accessions.write_text("PXD000001\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        ["bulk-audit", "--input", str(accessions), "--continue-on-error"],
    )

    assert result.exit_code == 0
    assert "Warning" in result.stderr
    assert "Completed : 0" in result.stdout
    assert "Failed    : 1" in result.stdout


def test_bulk_audit_stop_on_error(bulk_mocks: dict, tmp_path: Path) -> None:
    """Without --continue-on-error, first failure exits 1."""
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("UNKNOWN_ACC\nPXD000001\n")
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file)],
    )
    assert result.exit_code == 1


def test_bulk_audit_stop_on_error_with_partial_results(bulk_mocks: dict, tmp_path: Path) -> None:
    """Without --continue-on-error, failure after some successes exits 1 and shows partial count."""
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\nUNKNOWN_ACC\nPXD000002\n")
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file)],
    )
    assert result.exit_code == 1
    assert "Partial results" in result.output


def test_bulk_audit_stop_on_error_writes_partial_export(bulk_mocks: dict, tmp_path: Path) -> None:
    """Without --continue-on-error and with --format, failure writes partial export."""
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\nUNKNOWN_ACC\n")
    export_path = tmp_path / "partial.tsv"
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bulk-audit",
            "--input",
            str(acc_file),
            "--format",
            "tsv",
            "--output",
            str(export_path),
        ],
    )
    assert result.exit_code == 1
    assert export_path.exists()
    assert "Partial export written" in result.output


def test_bulk_audit_keyboard_interrupt_writes_partial_export(
    bulk_mocks: dict, tmp_path: Path
) -> None:
    """KeyboardInterrupt after partial success writes partial export."""

    def _audit_one_then_interrupt(accession: str, db_path: str, **kw: object) -> object:
        if accession == "PXD000001":
            r = AuditResult(accession="PXD000001", tier="Gold", quant_tier="Partial")
            return AuditData(r, {}, MagicMock(), [], "2026-01-01T00:00:00+00:00", [], [], True)
        raise KeyboardInterrupt

    bulk_mocks["_audit_single"].side_effect = _audit_one_then_interrupt
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\nPXD000002\n")
    export_path = tmp_path / "partial_intr.tsv"
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bulk-audit",
            "--input",
            str(acc_file),
            "--format",
            "tsv",
            "--output",
            str(export_path),
        ],
    )
    assert result.exit_code == 130
    assert export_path.exists()
    assert "Partial export written" in result.output


def test_bulk_audit_keyboard_interrupt_reports_partial_export_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An export failure cannot replace interrupt exit 130 with an uncaught error."""
    completed = AuditData(
        AuditResult(accession="PXD000001", tier="Gold"),
        {},
        MagicMock(),
        [],
        "ts",
        [],
        [],
        False,
    )
    monkeypatch.setattr(
        "pxaudit.cli._audit_single",
        MagicMock(side_effect=[completed, KeyboardInterrupt()]),
    )
    monkeypatch.setattr(
        "pxaudit.cli._write_export", MagicMock(side_effect=OSError("disk unavailable"))
    )
    accessions = tmp_path / "ids.txt"
    accessions.write_text("PXD000001\nPXD000002\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "bulk-audit",
            "--input",
            str(accessions),
            "--format",
            "tsv",
            "--output",
            str(tmp_path / "partial.tsv"),
        ],
    )

    assert result.exit_code == 130
    assert "partial export could not be written" in result.stderr


def test_bulk_audit_fatal_failure_reports_partial_export_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A partial-export encoding failure remains a clean runtime error."""
    completed = AuditData(
        AuditResult(accession="PXD000001", tier="Gold"),
        {},
        MagicMock(),
        [],
        "ts",
        [],
        [],
        False,
    )
    monkeypatch.setattr(
        "pxaudit.cli._audit_single",
        MagicMock(side_effect=[completed, PrideAPIError("API unavailable")]),
    )
    monkeypatch.setattr(
        "pxaudit.cli._write_export", MagicMock(side_effect=ValueError("cannot encode export"))
    )
    accessions = tmp_path / "ids.txt"
    accessions.write_text("PXD000001\nPXD000002\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "bulk-audit",
            "--input",
            str(accessions),
            "--format",
            "tsv",
            "--output",
            str(tmp_path / "partial.tsv"),
        ],
    )

    assert result.exit_code == 1
    assert "partial export" in result.stderr
    assert "cannot encode export" in result.stderr
    assert isinstance(result.exception, SystemExit)


def test_bulk_audit_stdin_input(bulk_mocks: dict, tmp_path: Path) -> None:
    """--input - reads from stdin."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", "-"],
        input="PXD000001\nPXD000002\n",
    )
    assert result.exit_code == 0
    assert "Completed : 2" in result.output


def test_bulk_audit_empty_input(bulk_mocks: dict, tmp_path: Path) -> None:
    """An empty input file exits successfully with a warning."""
    acc_file = tmp_path / "empty.txt"
    acc_file.write_text("# nothing\n")
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file)],
    )
    assert result.exit_code == 0
    assert "no accessions" in result.output


def test_bulk_audit_missing_input_file(tmp_path: Path) -> None:
    """A missing input file exits with an input error."""
    missing = tmp_path / "missing.txt"
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(missing)],
    )
    assert result.exit_code == 2
    assert "not found" in result.output
    assert not missing.exists()


def test_bulk_audit_invalid_utf8_input_exits_two(tmp_path: Path) -> None:
    """An undecodable accession file is an input-validation error."""
    accessions = tmp_path / "invalid.txt"
    accessions.write_bytes(b"PXD000001\n\xff")

    result = CliRunner().invoke(main, ["bulk-audit", "--input", str(accessions)])

    assert result.exit_code == 2
    assert "cannot read input file" in result.stderr


def test_bulk_audit_database_failure_is_clean_runtime_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A shared database failure aborts the batch with runtime exit 1."""
    accessions = tmp_path / "ids.txt"
    accessions.write_text("PXD000001\n", encoding="utf-8")
    monkeypatch.setattr(
        "pxaudit.cli._audit_single",
        MagicMock(side_effect=sqlite3.DatabaseError("database unavailable")),
    )

    result = CliRunner().invoke(main, ["bulk-audit", "--input", str(accessions)])

    assert result.exit_code == 1
    assert "bulk audit failed" in result.stderr


def test_bulk_audit_export_failure_is_clean_runtime_error(
    bulk_mocks: dict, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An export write failure exits 1 after completed audits remain persisted."""
    accessions = tmp_path / "ids.txt"
    accessions.write_text("PXD000001\n", encoding="utf-8")
    output = tmp_path / "results.tsv"
    monkeypatch.setattr(
        "pxaudit.cli._write_export", MagicMock(side_effect=OSError("disk unavailable"))
    )

    result = CliRunner().invoke(
        main,
        [
            "bulk-audit",
            "--input",
            str(accessions),
            "--format",
            "tsv",
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 1
    assert "cannot write export" in result.stderr


def test_bulk_audit_duplicate_warning(bulk_mocks: dict, tmp_path: Path) -> None:
    """Duplicate accessions produce a warning and are processed once."""
    acc_file = tmp_path / "dups.txt"
    acc_file.write_text("PXD000001\nPXD000001\nPXD000002\n")
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file)],
    )
    assert result.exit_code == 0
    assert "duplicate" in result.output
    assert "Completed : 2" in result.output


def test_bulk_audit_deduplicates_canonical_accessions(
    bulk_mocks: dict,
    tmp_path: Path,
) -> None:
    """Case variants are processed once under their canonical uppercase identity."""
    acc_file = tmp_path / "dups.txt"
    acc_file.write_text("pxd000001\nPXD000001\nPxD000002\n", encoding="utf-8")

    result = CliRunner().invoke(main, ["bulk-audit", "--input", str(acc_file)])

    assert result.exit_code == 0
    assert "duplicate accession 'PXD000001'" in result.stderr
    assert "Completed : 2" in result.stdout
    calls = [entry.args[0] for entry in bulk_mocks["_audit_single"].call_args_list]
    assert calls == ["PXD000001", "PXD000002"]


def test_bulk_audit_malformed_line_stops_with_line_number(
    bulk_mocks: dict,
    tmp_path: Path,
) -> None:
    """Without continuation, malformed input exits 2 before auditing any accession."""
    acc_file = tmp_path / "invalid.txt"
    acc_file.write_text("# header\nPXD000001\nMSV/000001\n", encoding="utf-8")

    result = CliRunner().invoke(main, ["bulk-audit", "--input", str(acc_file)])

    assert result.exit_code == 2
    assert "line 3" in result.stderr
    assert "MSV/000001" in result.stderr
    bulk_mocks["_audit_single"].assert_not_called()


def test_bulk_audit_continue_skips_malformed_line_and_counts_failure(
    bulk_mocks: dict,
    tmp_path: Path,
) -> None:
    """Continuation reports and counts malformed records while auditing valid lines."""
    acc_file = tmp_path / "mixed-validity.txt"
    acc_file.write_text("\n# header\nMSV?000001\npxd000001\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        ["bulk-audit", "--input", str(acc_file), "--continue-on-error"],
    )

    assert result.exit_code == 0
    assert "line 3" in result.stderr
    assert "Total     : 2" in result.stdout
    assert "Completed : 1" in result.stdout
    assert "Failed    : 1" in result.stdout
    bulk_mocks["_audit_single"].assert_called_once()
    assert bulk_mocks["_audit_single"].call_args.args[0] == "PXD000001"


def test_bulk_audit_rejects_control_separator_as_part_of_one_physical_line(
    bulk_mocks: dict,
    tmp_path: Path,
) -> None:
    """A control separator cannot turn one malformed physical line into valid records."""
    acc_file = tmp_path / "control.txt"
    acc_file.write_text("PXD000001\x1cPXD000002\n", encoding="utf-8")

    result = CliRunner().invoke(main, ["bulk-audit", "--input", str(acc_file)])

    assert result.exit_code == 2
    assert "line 1" in result.stderr
    bulk_mocks["_audit_single"].assert_not_called()


def test_bulk_audit_mixed_pride_and_non_pride(bulk_mocks: dict, tmp_path: Path) -> None:
    """Mixed PRIDE and non-PRIDE accessions produce correct Unverifiable rows."""
    acc_file = tmp_path / "mixed.txt"
    acc_file.write_text("PXD000001\nMSV000001\nPXD000002\n")
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file)],
    )
    assert result.exit_code == 0
    assert "Completed : 3" in result.output
    assert "Unverifiable" in result.output
    assert "Gold" in result.output
    assert "Diamond" in result.output


def test_bulk_audit_overwrite_guard(bulk_mocks: dict, tmp_path: Path) -> None:
    """An existing output file is refused without the overwrite option."""
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\n")
    out_path = tmp_path / "out.tsv"
    out_path.touch()
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file), "--format", "tsv", "--output", str(out_path)],
    )
    assert result.exit_code == 2
    assert "already exists" in result.output


def test_bulk_audit_overwrite_allowed(bulk_mocks: dict, tmp_path: Path) -> None:
    """Existing output file with --overwrite succeeds."""
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\n")
    out_path = tmp_path / "out.tsv"
    out_path.write_text("old data\n")
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bulk-audit",
            "--input",
            str(acc_file),
            "--format",
            "tsv",
            "--output",
            str(out_path),
            "--overwrite",
        ],
    )
    assert result.exit_code == 0
    assert out_path.read_text().startswith("accession")


def test_bulk_audit_overwrite_refuses_directory_output(bulk_mocks: dict, tmp_path: Path) -> None:
    """Overwrite cannot replace a directory with an export file."""
    accessions = tmp_path / "ids.txt"
    accessions.write_text("PXD000001\n", encoding="utf-8")
    output = tmp_path / "out.tsv"
    output.mkdir()

    result = CliRunner().invoke(
        main,
        [
            "bulk-audit",
            "--input",
            str(accessions),
            "--format",
            "tsv",
            "--output",
            str(output),
            "--overwrite",
        ],
    )

    assert result.exit_code == 2
    assert "not a file" in result.stderr
    assert list(output.iterdir()) == []
    bulk_mocks["_audit_single"].assert_not_called()


def test_bulk_audit_overwrite_refuses_symlink_output(
    bulk_mocks: dict, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Overwrite refuses a target identified as a symbolic link."""
    accessions = tmp_path / "ids.txt"
    accessions.write_text("PXD000001\n", encoding="utf-8")
    output = tmp_path / "out.tsv"
    output.write_text("keep\n", encoding="utf-8")
    original_is_symlink = Path.is_symlink
    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda path: path == output or original_is_symlink(path),
    )

    result = CliRunner().invoke(
        main,
        [
            "bulk-audit",
            "--input",
            str(accessions),
            "--format",
            "tsv",
            "--output",
            str(output),
            "--overwrite",
        ],
    )

    assert result.exit_code == 2
    assert "symbolic link" in result.stderr
    assert output.read_text(encoding="utf-8") == "keep\n"
    bulk_mocks["_audit_single"].assert_not_called()


def test_bulk_audit_tier_distribution_in_summary(bulk_mocks: dict, tmp_path: Path) -> None:
    """Summary includes tier distribution."""
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\nPXD000002\n")
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file)],
    )
    assert result.exit_code == 0
    assert "Gold" in result.output
    assert "Diamond" in result.output


def test_bulk_audit_keyboard_interrupt(bulk_mocks: dict, tmp_path: Path) -> None:
    """Ctrl+C interrupts the batch cleanly."""

    def _interrupt(*args: object, **kw: object) -> object:
        raise KeyboardInterrupt

    bulk_mocks["_audit_single"].side_effect = _interrupt
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\nPXD000002\nPXD000003\n")
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file)],
    )
    assert result.exit_code == 130
    assert "Interrupted" in result.output


def test_manifest_no_files_errors(tmp_path: Path) -> None:
    """manifest on an accession with no files prints error and exits 1."""
    db_path = tmp_path / "empty.db"
    from pxaudit.db import get_or_create_db

    get_or_create_db(db_path).close()
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["manifest", "PXD000001", "--db", str(db_path)],
    )
    assert result.exit_code == 1
    assert "No files found" in result.output


@pytest.mark.parametrize("fmt", ["tsv", "json"])
def test_manifest_empty_study_emits_empty_manifest(tmp_path: Path, fmt: str) -> None:
    """manifest preserves an audited study with no files as an empty data result."""
    from pxaudit.db import get_or_create_db

    database = tmp_path / "empty-study.db"
    connection = get_or_create_db(database)
    connection.execute("INSERT INTO study (accession) VALUES (?)", ("PXD000001",))
    connection.commit()
    connection.close()

    result = CliRunner().invoke(
        main,
        ["manifest", "PXD000001", "--db", str(database), "--format", fmt],
    )

    assert result.exit_code == 0
    if fmt == "json":
        assert json.loads(result.stdout) == []
    else:
        assert result.stdout == (
            "file_name\tfile_category\tfile_extension\tftp_location\tfile_size\tchecksum\t"
            "checksum_type\n"
        )


def test_manifest_invalid_accession_exits_two_before_database_access(tmp_path: Path) -> None:
    """Manifest applies the shared grammar before opening its database."""
    database = tmp_path / "missing.db"
    result = CliRunner().invoke(
        main,
        ["manifest", "PXD/000001", "--db", str(database)],
    )
    assert result.exit_code == 2
    assert "invalid accession" in result.stderr
    assert not database.exists()


def test_manifest_missing_database_leaves_filesystem_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Manifest inspection refuses a missing database without creating artifacts."""
    database = tmp_path / "missing.db"
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "missing.toml"))

    result = CliRunner().invoke(
        main,
        ["manifest", "PXD000001", "--db", str(database)],
    )

    assert result.exit_code == 2
    assert "database not found" in result.stderr
    assert list(tmp_path.iterdir()) == []


def test_manifest_corrupt_database_is_preserved(tmp_path: Path) -> None:
    """Manifest reports an unreadable database without modifying its bytes."""
    database = tmp_path / "corrupt.db"
    original = b"not a sqlite database"
    database.write_bytes(original)

    result = CliRunner().invoke(
        main,
        ["manifest", "PXD000001", "--db", str(database)],
    )

    assert result.exit_code == 1
    assert "cannot read database" in result.stderr
    assert database.read_bytes() == original


def test_manifest_database_open_error_exits_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Manifest reports SQLite failures raised while opening an existing path."""
    database = tmp_path / "audit.db"
    database.touch()
    monkeypatch.setattr(
        "pxaudit.cli.open_existing_db",
        MagicMock(side_effect=sqlite3.DatabaseError("open failed")),
    )

    result = CliRunner().invoke(
        main,
        ["manifest", "PXD000001", "--db", str(database)],
    )

    assert result.exit_code == 1
    assert "cannot read database" in result.stderr


def test_manifest_invalid_text_bytes_are_clean_runtime_error(tmp_path: Path) -> None:
    """Invalid UTF-8 stored in a text field produces a clean manifest error."""
    from pxaudit.db import get_or_create_db

    database = tmp_path / "invalid-text.db"
    connection = get_or_create_db(database)
    connection.execute("INSERT INTO study (accession) VALUES (?)", ("PXD000001",))
    connection.execute(
        "INSERT INTO study_files (accession, file_name) VALUES (?, ?)",
        ("PXD000001", sqlite3.Binary(b"\xff")),
    )
    connection.commit()
    connection.close()

    result = CliRunner().invoke(
        main,
        ["manifest", "PXD000001", "--db", str(database), "--format", "json"],
    )

    assert result.exit_code == 1
    assert "cannot format manifest" in result.stderr
    assert isinstance(result.exception, SystemExit)


def _create_manifest_golden_db(tmp_path: Path) -> Path:
    """Create one deterministic manifest row for byte-level CLI assertions."""
    from pxaudit.db import get_or_create_db, insert_study, insert_study_files

    database = tmp_path / "manifest-golden.db"
    connection = get_or_create_db(database)
    try:
        insert_study(connection, {"accession": "PXD000001", "fetched_at": "now"})
        insert_study_files(
            connection,
            "PXD000001",
            pd.DataFrame(
                [
                    {
                        "accession": "PXD000001",
                        "file_name": "a.mzid",
                        "file_category": "RESULT",
                        "file_extension": ".mzid",
                        "ftp_location": None,
                        "file_size": 1,
                        "checksum": None,
                        "checksum_type": None,
                    }
                ]
            ),
        )
    finally:
        connection.close()
    return database


@pytest.mark.parametrize(
    ("global_flags", "use_no_color_env"),
    [
        ([], False),
        (["-q"], False),
        (["-v"], False),
        (["--no-color"], False),
        ([], True),
    ],
    ids=["default-nontty", "quiet", "verbose", "no-color-flag", "no-color-env"],
)
@pytest.mark.parametrize(
    ("fmt", "expected"),
    [
        (
            "tsv",
            "file_name\tfile_category\tfile_extension\tftp_location\tfile_size\tchecksum\t"
            "checksum_type\na.mzid\tRESULT\t.mzid\t\t1\t\t\n",
        ),
        (
            "json",
            '[\n  {\n    "file_name":"a.mzid",\n    "file_category":"RESULT",\n'
            '    "file_extension":".mzid",\n    "ftp_location":null,\n'
            '    "file_size":1,\n    "checksum":null,\n    "checksum_type":null\n  }\n]\n',
        ),
    ],
)
def test_manifest_body_golden_in_every_output_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    global_flags: list[str],
    use_no_color_env: bool,
    fmt: str,
    expected: str,
) -> None:
    """Manifest stdout is exact data with no status or ANSI contamination."""
    database = _create_manifest_golden_db(tmp_path)
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "missing.toml"))
    if use_no_color_env:
        monkeypatch.setenv("NO_COLOR", "1")
    else:
        monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setattr(os, "linesep", "\r\n")

    result = CliRunner().invoke(
        main,
        [*global_flags, "manifest", "PXD000001", "--db", str(database), "--format", fmt],
    )

    assert result.exit_code == 0
    assert result.stdout == expected
    assert result.stderr == ""
    assert "\x1b[" not in result.stdout


def test_check_stale_cache_fallback_on_files_failure(
    mocks: dict, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When files fetch fails, stale cached files must be served with warning."""
    monkeypatch.setattr("pxaudit.cli.fetch_files", MagicMock(side_effect=PrideAPIError("down")))
    monkeypatch.setattr(
        "pxaudit.cli.read_cache_stale_response",
        MagicMock(return_value=_cached([{"fileName": "stale.mzid"}], age=9999.0)),
    )
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert result.exit_code == 0
    assert "stale cached file list" in result.output


def test_quiet_and_verbose_mutually_exclusive(mocks: dict) -> None:
    """-q and -v together exit 2."""
    runner = CliRunner()
    result = runner.invoke(main, ["-q", "-v", "check", "PXD000001"])
    assert result.exit_code == 2
    assert "mutually exclusive" in result.output


def test_check_quiet_one_line_no_checklist(mocks: dict) -> None:
    """Quiet check: one status line, no Metadata/Files checklist body."""
    runner = CliRunner()
    result = runner.invoke(main, ["-q", "check", "PXD000001", "--db", "out.db"])
    assert result.exit_code == 0
    assert "Metadata" not in result.output
    assert "Files (" not in result.output
    assert "PXD000001" in result.output
    assert "db=out.db" in result.output
    assert "Gold" in result.output or "tier" in result.output.lower() or "Partial" in result.output


def test_check_verbose_includes_detail(mocks: dict) -> None:
    """Verbose check includes cache miss/fetch detail lines."""
    runner = CliRunner()
    result = runner.invoke(main, ["-v", "check", "PXD000001"])
    assert result.exit_code == 0
    assert "Metadata" in result.output
    assert "cache miss" in result.output or "fetch:" in result.output


@pytest.mark.parametrize("global_flags", [[], ["-q"], ["-v"]])
def test_check_stale_warning_survives_all_output_modes(
    mocks: dict, monkeypatch: pytest.MonkeyPatch, global_flags: list[str]
) -> None:
    """Default, quiet, and verbose modes do not suppress stale-cache warnings."""
    mocks["fetch_project"].side_effect = PrideAPIError("down")
    mocks["read_cache_stale_response"].return_value = _cached(_GOLD_PROJECT, age=99999.0)
    runner = CliRunner()
    result = runner.invoke(main, [*global_flags, "check", "PXD000001"])
    assert result.exit_code == 0
    assert "stale" in result.stderr.lower()
    assert "Warning" in result.stderr
    if global_flags == ["-q"]:
        assert "Metadata" not in result.stdout


def test_no_cache_help_mentions_reads_and_writes() -> None:
    """--no-cache help must mention reads and writes."""
    runner = CliRunner()
    result = runner.invoke(main, ["check", "--help"])
    assert result.exit_code == 0
    assert "reads and writes" in result.output


def test_refresh_help_mentions_still_write() -> None:
    """--refresh help distinguishes live fetches, writes, and stale fallback."""
    runner = CliRunner()
    result = runner.invoke(main, ["check", "--help"])
    assert "Skip fresh cache reads" in result.output
    assert "write successes" in result.output
    assert "stale fallback" in result.output


def test_audit_single_forwards_cache_and_delay(mocks: dict, tmp_path: Path) -> None:
    """_audit_single passes cache_dir, TTL, and request_delay through."""
    from pxaudit.cli import _audit_single

    _audit_single(
        "PXD000001",
        str(tmp_path / "db.sqlite"),
        cache_dir=str(tmp_path / "cache"),
        cache_ttl_seconds=123.0,
        request_delay=0.0,
    )
    assert mocks["read_cache_response"].called
    kwargs = mocks["read_cache_response"].call_args.kwargs
    assert kwargs["cache_dir"] == tmp_path / "cache"
    assert kwargs["max_age"] == 123.0
    assert mocks["fetch_project"].call_args.kwargs.get("delay") == 0.0
    assert mocks["write_cache"].called
    write_kwargs = mocks["write_cache"].call_args.kwargs
    assert write_kwargs["cache_dir"] == tmp_path / "cache"


def test_audit_single_no_click_echo(
    mocks: dict, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_audit_single must not call click.echo (warnings returned as data)."""
    import pxaudit.cli as cli_mod
    from pxaudit.cli import _audit_single

    echoed: list[str] = []

    def boom(*a: object, **k: object) -> None:
        echoed.append(str(a))

    monkeypatch.setattr(cli_mod.click, "echo", boom)
    mocks["fetch_project"].side_effect = PrideAPIError("down")
    mocks["read_cache_stale_response"].return_value = _cached(_GOLD_PROJECT, age=10.0)
    data = _audit_single("PXD000001", str(tmp_path / "db.sqlite"), request_delay=0.0)
    assert echoed == []
    assert data.warnings
    assert "stale" in data.warnings[0].lower()


def test_bulk_skips_delay_on_full_cache_hit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """bulk_delay sleep skipped when network_used is False."""
    sleeps: list[float] = []

    def fake_audit(accession: str, db_path: str, **kw: object) -> AuditData:
        r = AuditResult(accession=accession, tier="Gold", quant_tier="Partial")
        return AuditData(r, {}, MagicMock(), [], "ts", [], [], False)

    monkeypatch.setattr("pxaudit.cli._audit_single", MagicMock(side_effect=fake_audit))
    monkeypatch.setattr("pxaudit.cli.time.sleep", lambda s: sleeps.append(s))
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\nPXD000002\n")
    runner = CliRunner()
    result = runner.invoke(main, ["bulk-audit", "--input", str(acc), "--delay", "2.5"])
    assert result.exit_code == 0
    assert sleeps == []


def test_bulk_applies_delay_after_network(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """bulk_delay sleep runs when network_used is True."""
    sleeps: list[float] = []

    def fake_audit(accession: str, db_path: str, **kw: object) -> AuditData:
        r = AuditResult(accession=accession, tier="Gold", quant_tier="Partial")
        return AuditData(r, {}, MagicMock(), [], "ts", [], [], True)

    monkeypatch.setattr("pxaudit.cli._audit_single", MagicMock(side_effect=fake_audit))
    monkeypatch.setattr("pxaudit.cli.time.sleep", lambda s: sleeps.append(s))
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\n")
    runner = CliRunner()
    result = runner.invoke(main, ["bulk-audit", "--input", str(acc), "--delay", "2.5"])
    assert result.exit_code == 0
    assert sleeps == [2.5]


def test_bulk_quiet_summary_one_line(bulk_mocks: dict, tmp_path: Path) -> None:
    """Quiet bulk-audit: compact summary, no tier distribution block."""
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\nPXD000002\n")
    runner = CliRunner()
    result = runner.invoke(main, ["-q", "bulk-audit", "--input", str(acc)])
    assert result.exit_code == 0
    assert "bulk-audit" in result.output
    assert "completed=" in result.output
    assert "Batch audit complete" not in result.output


def test_bulk_quiet_disables_tqdm(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Quiet mode must not construct tqdm."""
    called: list[bool] = []

    class Boom:
        def __init__(self, *a: object, **k: object) -> None:
            called.append(True)

        def __iter__(self) -> Iterator[object]:
            return iter([])

    def fake_audit(accession: str, db_path: str, **kw: object) -> AuditData:
        r = AuditResult(accession=accession, tier="Gold")
        return AuditData(r, {}, MagicMock(), [], "ts", [], [], False)

    monkeypatch.setattr("pxaudit.cli.tqdm", Boom)
    monkeypatch.setattr("pxaudit.cli._audit_single", MagicMock(side_effect=fake_audit))
    monkeypatch.setattr("pxaudit.cli.time.sleep", lambda _: None)
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\n")
    runner = CliRunner()
    result = runner.invoke(main, ["-q", "bulk-audit", "--input", str(acc)])
    assert result.exit_code == 0
    assert called == []


@pytest.mark.parametrize("existing", [False, True], ids=["missing", "existing"])
def test_cache_info_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, existing: bool) -> None:
    """Cache information reports stable zero values for empty and missing roots."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    cache = tmp_path / "cache"
    if existing:
        cache.mkdir()
    runner = CliRunner()
    result = runner.invoke(main, ["--cache-dir", str(cache), "cache", "info"])
    assert result.exit_code == 0
    assert f"cache_dir={cache}" in result.output
    assert "files=0" in result.output
    assert "bytes=0" in result.output
    assert "oldest=n/a" in result.output


def test_cache_clear_decline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Declining confirmation leaves files intact."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    cache = tmp_path / "cache"
    cache.mkdir()
    write_cache("PXD000001", "project", {}, cache_dir=cache)
    target = cache / "PXD000001_project.json"
    runner = CliRunner()
    result = runner.invoke(main, ["--cache-dir", str(cache), "cache", "clear"], input="n\n")
    assert result.exit_code != 0 or target.exists()
    assert target.exists()


def test_cache_clear_missing_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """cache clear on missing dir is a no-op success."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    cache = tmp_path / "missing"
    runner = CliRunner()
    result = runner.invoke(main, ["--cache-dir", str(cache), "cache", "clear", "--yes"])
    assert result.exit_code == 0
    assert "nothing to delete" in result.output.lower() or "does not exist" in result.output


def test_cache_clear_help_keeps_safety_validation_under_yes() -> None:
    """The noninteractive option documents that cache safety still applies."""
    result = CliRunner().invoke(main, ["cache", "clear", "--help"])
    assert result.exit_code == 0
    assert "safety validation still applies" in result.output


def test_db_flag_overrides_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mocks: dict
) -> None:
    """--db flag wins over config file db_path."""
    cfg = tmp_path / "c.toml"
    cfg.write_text('db_path = "from_file.db"\n')
    monkeypatch.setenv("PXAUDIT_CONFIG", str(cfg))
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001", "--db", str(tmp_path / "flag.db")])
    assert result.exit_code == 0
    assert mocks["get_or_create_db"].call_args.args[0] == str(tmp_path / "flag.db")


def test_bulk_quiet_with_export_skips_exported_line(bulk_mocks: dict, tmp_path: Path) -> None:
    """Quiet bulk with export: no 'Exported N results' line."""
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\n")
    out = tmp_path / "out.tsv"
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["-q", "bulk-audit", "--input", str(acc), "--format", "tsv", "--output", str(out)],
    )
    assert result.exit_code == 0
    assert "Exported" not in result.output
    assert "export=" in result.output
    assert out.exists()


def test_config_warning_emitted_on_check(
    mocks: dict, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Unknown config keys warn during check."""
    cfg = tmp_path / "c.toml"
    cfg.write_text('weird = 1\ndb_path = "x.db"\n')
    monkeypatch.setenv("PXAUDIT_CONFIG", str(cfg))
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001", "--db", str(tmp_path / "o.db")])
    assert result.exit_code == 0
    assert "weird" in result.output


def test_bulk_verbose_continue_on_error_detail(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Verbose + continue-on-error emits skipped detail."""

    def fake_audit(accession: str, db_path: str, **kw: object) -> AuditData:
        raise PrideAPIError("boom")

    monkeypatch.setattr("pxaudit.cli._audit_single", MagicMock(side_effect=fake_audit))
    monkeypatch.setattr("pxaudit.cli.time.sleep", lambda _: None)
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\n")
    runner = CliRunner()
    result = runner.invoke(main, ["-v", "bulk-audit", "--input", str(acc), "--continue-on-error"])
    assert result.exit_code == 0
    assert "skipped: PXD000001" in result.output


def test_bulk_emits_audit_warnings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Warnings returned by _audit_single are printed in bulk-audit."""

    def fake_audit(accession: str, db_path: str, **kw: object) -> AuditData:
        r = AuditResult(accession=accession, tier="Gold")
        return AuditData(r, {}, MagicMock(), [], "ts", ["Warning: demo"], ["detail-x"], False)

    monkeypatch.setattr("pxaudit.cli._audit_single", MagicMock(side_effect=fake_audit))
    monkeypatch.setattr("pxaudit.cli.time.sleep", lambda _: None)
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\n")
    runner = CliRunner()
    result = runner.invoke(main, ["-v", "bulk-audit", "--input", str(acc)])
    assert result.exit_code == 0
    assert "Warning: demo" in result.output
    assert "detail-x" in result.output


def test_report_missing_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """report exits 2 when database is missing."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    runner = CliRunner()
    database = tmp_path / "no.db"
    result = runner.invoke(main, ["report", "--db", str(database)])
    assert result.exit_code == 2
    assert "database not found" in result.output
    assert not database.exists()
    assert list(tmp_path.iterdir()) == []


def test_report_database_path_must_be_file(tmp_path: Path) -> None:
    """An existing database directory is rejected as invalid input."""
    database = tmp_path / "database"
    database.mkdir()

    result = CliRunner().invoke(main, ["report", "--db", str(database)])

    assert result.exit_code == 2
    assert "database path is not a file" in result.stderr
    assert list(database.iterdir()) == []


def test_report_database_access_error_exits_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A database metadata-access failure is operational, not a missing path."""
    database = tmp_path / "audit.db"
    database.touch()
    original_stat = Path.stat

    def fail_database_stat(path: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        if path == database:
            raise PermissionError("metadata denied")
        return original_stat(path, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(Path, "stat", fail_database_stat)

    result = CliRunner().invoke(main, ["report", "--db", str(database)])

    assert result.exit_code == 1
    assert "cannot access database" in result.stderr
    assert isinstance(result.exception, SystemExit)


def test_report_existing_output_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An existing directory is allowed unless its report file conflicts."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    db = tmp_path / "x.db"
    db.write_text("")
    out = tmp_path / "outdir"
    out.mkdir()
    report = out / "report.html"
    report.write_text("keep", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(main, ["report", "--db", str(db), "--output", str(out)])
    assert result.exit_code == 2
    assert "already exists" in result.output
    assert report.read_text(encoding="utf-8") == "keep"


def test_report_overwrite_refuses_symlink_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Report overwrite refuses a target identified as a symbolic link."""
    from pxaudit.db import get_or_create_db

    database = tmp_path / "audit.db"
    get_or_create_db(database).close()
    output = tmp_path / "report"
    output.mkdir()
    report_target = output / "report.html"
    report_target.write_text("keep", encoding="utf-8")
    original_is_symlink = Path.is_symlink
    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda path: path == report_target or original_is_symlink(path),
    )
    generate = MagicMock()
    monkeypatch.setattr("pxaudit.report.generate_report", generate)

    result = CliRunner().invoke(
        main,
        ["report", "--db", str(database), "--output", str(output), "--overwrite"],
    )

    assert result.exit_code == 2
    assert "symbolic link" in result.stderr
    assert report_target.read_text(encoding="utf-8") == "keep"
    generate.assert_not_called()


def test_report_success_and_verbose(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A verbose report run emits its destination and generation details."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    db = tmp_path / "x.db"
    from pxaudit.db import get_or_create_db

    get_or_create_db(db).close()
    out = tmp_path / "outdir"
    monkeypatch.setattr(
        "pxaudit.report.generate_report",
        lambda *a, **k: str(out / "report.html"),
    )
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["-v", "report", "--db", str(db), "--output", str(out), "--overwrite"],
    )
    assert result.exit_code == 0
    assert "Report written to" in result.output
    assert "report rows=" in result.output
    assert "files=" in result.output


@pytest.mark.parametrize(
    ("global_flags", "use_no_color_env", "verbose"),
    [
        ([], False, False),
        (["-q"], False, False),
        (["-v"], False, True),
        (["--no-color"], False, False),
        ([], True, False),
    ],
    ids=["default-nontty", "quiet", "verbose", "no-color-flag", "no-color-env"],
)
def test_report_plain_text_golden_in_every_output_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    global_flags: list[str],
    use_no_color_env: bool,
    verbose: bool,
) -> None:
    """Report status and verbose detail have stable plain-text streams."""
    from pxaudit.db import get_or_create_db

    database = tmp_path / "report.db"
    get_or_create_db(database).close()
    report_path = tmp_path / "output" / "report.html"
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "missing.toml"))
    if use_no_color_env:
        monkeypatch.setenv("NO_COLOR", "1")
    else:
        monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setattr("pxaudit.report.generate_report", MagicMock(return_value=report_path))

    result = CliRunner().invoke(
        main,
        [*global_flags, "report", "--db", str(database), "--output", str(report_path.parent)],
    )

    expected = f"Report written to {report_path}\n"
    if verbose:
        expected += f"report rows=0 files=0 db={database} output={report_path}\n"
    assert result.exit_code == 0
    assert result.stdout == expected
    assert result.stderr == ""
    assert "\x1b[" not in result.stdout


@pytest.mark.parametrize(
    ("exc", "code"),
    [
        (ValueError("bad"), 1),
        (ImportError("no jinja"), 1),
        (FileNotFoundError("gone"), 1),
        (PermissionError("denied"), 1),
        (sqlite3.DatabaseError("corrupt"), 1),
    ],
)
def test_report_error_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exc: Exception, code: int
) -> None:
    """Expected report-generation failures produce operational exit codes."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    db = tmp_path / "x.db"
    db.write_text("")
    out = tmp_path / "outdir"

    def boom(*a: object, **k: object) -> str:
        raise exc

    monkeypatch.setattr("pxaudit.report.generate_report", boom)
    runner = CliRunner()
    result = runner.invoke(main, ["report", "--db", str(db), "--output", str(out), "--overwrite"])
    assert result.exit_code == code


def test_check_trailing_quiet_flag_rejected(mocks: dict) -> None:
    """Group flags after the subcommand are rejected by Click."""
    runner = CliRunner()
    result = runner.invoke(main, ["check", "-q", "PXD000001"])
    assert result.exit_code == 2
    assert "No such option" in result.output


def test_main_help_mentions_global_flag_order() -> None:
    """Group help epilog documents that global flags precede the subcommand."""
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "before the subcommand" in result.output


def test_bulk_delay_flag_overrides_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """--delay overrides config bulk_delay end-to-end."""
    cfg = tmp_path / "c.toml"
    cfg.write_text("bulk_delay = 9.0\n")
    monkeypatch.setenv("PXAUDIT_CONFIG", str(cfg))
    sleeps: list[float] = []

    def fake_audit(accession: str, db_path: str, **kw: object) -> AuditData:
        r = AuditResult(accession=accession, tier="Gold", quant_tier="Partial")
        return AuditData(r, {}, MagicMock(), [], "ts", [], [], True)

    monkeypatch.setattr("pxaudit.cli._audit_single", MagicMock(side_effect=fake_audit))
    monkeypatch.setattr("pxaudit.cli.time.sleep", lambda s: sleeps.append(s))
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\n")
    runner = CliRunner()
    result = runner.invoke(main, ["bulk-audit", "--input", str(acc), "--delay", "2.5"])
    assert result.exit_code == 0
    assert sleeps == [2.5]


@pytest.mark.parametrize("delay", ["-1", "nan", "inf", "-inf"])
def test_bulk_invalid_delay_exits_two_before_audit(
    bulk_mocks: dict,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    delay: str,
) -> None:
    """Negative and non-finite delays are rejected before any audit starts."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\n")
    runner = CliRunner()
    result = runner.invoke(main, ["bulk-audit", "--input", str(acc), "--delay", delay])
    assert result.exit_code == 2
    assert "finite and non-negative" in result.output
    bulk_mocks["_audit_single"].assert_not_called()


def test_stale_fallback_sets_network_used_and_sleeps(
    mocks: dict, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """API failure with stale cache still counts as network_used for bulk_delay."""
    from pxaudit.cli import _audit_single

    mocks["fetch_project"].side_effect = PrideAPIError("down")
    mocks["read_cache_stale_response"].return_value = _cached(_GOLD_PROJECT, age=50.0)
    mocks["read_cache_response"].return_value = None
    data = _audit_single(
        "PXD000001",
        str(tmp_path / "db.sqlite"),
        request_delay=0.0,
    )
    assert data.network_used is True
    assert data.warnings


def test_bulk_sleeps_after_stale_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """bulk-audit applies delay when _audit_single reports network_used after stale path."""
    sleeps: list[float] = []

    def fake_audit(accession: str, db_path: str, **kw: object) -> AuditData:
        r = AuditResult(accession=accession, tier="Gold")
        return AuditData(r, {}, MagicMock(), [], "ts", ["Warning: stale"], [], True)

    monkeypatch.setattr("pxaudit.cli._audit_single", MagicMock(side_effect=fake_audit))
    monkeypatch.setattr("pxaudit.cli.time.sleep", lambda s: sleeps.append(s))
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\n")
    runner = CliRunner()
    result = runner.invoke(main, ["bulk-audit", "--input", str(acc), "--delay", "1.5"])
    assert result.exit_code == 0
    assert sleeps == [1.5]


def test_bulk_tqdm_used_when_tty(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """When stderr is a TTY and not quiet, tqdm wraps the accession list."""
    called: list[bool] = []

    class FakeTqdm:
        def __init__(self, iterable: Iterable[object], **kwargs: object) -> None:
            called.append(True)
            self._it = list(iterable)

        def __iter__(self) -> Iterator[object]:
            return iter(self._it)

    def fake_audit(accession: str, db_path: str, **kw: object) -> AuditData:
        r = AuditResult(accession=accession, tier="Gold")
        return AuditData(r, {}, MagicMock(), [], "ts", [], [], False)

    monkeypatch.setattr("pxaudit.cli.tqdm", FakeTqdm)
    monkeypatch.setattr("pxaudit.cli._stderr_is_tty", lambda: True)
    monkeypatch.setattr("pxaudit.cli._audit_single", MagicMock(side_effect=fake_audit))
    monkeypatch.setattr("pxaudit.cli.time.sleep", lambda _: None)
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\n")
    runner = CliRunner()
    result = runner.invoke(main, ["bulk-audit", "--input", str(acc)])
    assert result.exit_code == 0
    assert called == [True]


def test_bulk_continue_on_error_applies_delay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Hard PrideAPIError with --continue-on-error still applies bulk_delay."""
    sleeps: list[float] = []

    def boom(accession: str, db_path: str, **kw: object) -> AuditData:
        raise PrideAPIError("down")

    monkeypatch.setattr("pxaudit.cli._audit_single", MagicMock(side_effect=boom))
    monkeypatch.setattr("pxaudit.cli.time.sleep", lambda s: sleeps.append(s))
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\nPXD000002\n")
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc), "--continue-on-error", "--delay", "1.25"],
    )
    assert result.exit_code == 0
    assert sleeps == [1.25, 1.25]


def test_config_request_delay_reaches_fetch(
    mocks: dict, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """TOML request_delay is forwarded to fetch_project via check."""
    cfg = tmp_path / "c.toml"
    cfg.write_text("request_delay = 0.0\n")
    monkeypatch.setenv("PXAUDIT_CONFIG", str(cfg))
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001", "--db", str(tmp_path / "o.db")])
    assert result.exit_code == 0
    assert mocks["fetch_project"].call_args.kwargs.get("delay") == 0.0


def test_config_cache_ttl_reaches_read_cache(
    mocks: dict, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """TOML cache_ttl_seconds is forwarded to read_cache max_age."""
    cfg = tmp_path / "c.toml"
    cfg.write_text("cache_ttl_seconds = 42\n")
    monkeypatch.setenv("PXAUDIT_CONFIG", str(cfg))
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--cache-dir",
            str(tmp_path / "cache"),
            "check",
            "PXD000001",
            "--db",
            str(tmp_path / "o.db"),
        ],
    )
    assert result.exit_code == 0
    assert mocks["read_cache_response"].call_args.kwargs.get("max_age") == 42.0


def test_config_export_format_triggers_bulk_export(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """TOML export_format alone causes bulk-audit to write an export file."""
    cfg = tmp_path / "c.toml"
    cfg.write_text('export_format = "tsv"\n')
    monkeypatch.setenv("PXAUDIT_CONFIG", str(cfg))

    def fake_audit(accession: str, db_path: str, **kw: object) -> AuditData:
        r = AuditResult(accession=accession, tier="Gold", quant_tier="Partial")
        return AuditData(r, {}, MagicMock(), [], "ts", [], [], False)

    monkeypatch.setattr("pxaudit.cli._audit_single", MagicMock(side_effect=fake_audit))
    monkeypatch.setattr("pxaudit.cli.time.sleep", lambda _: None)
    monkeypatch.setattr(
        "pxaudit.cli._default_export_path",
        lambda fmt: str(tmp_path / f"from_config.{fmt}"),
    )
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\n")
    runner = CliRunner()
    result = runner.invoke(main, ["bulk-audit", "--input", str(acc)])
    assert result.exit_code == 0
    assert (tmp_path / "from_config.tsv").exists()


def test_cache_mixed_directory_info_and_clear_share_owned_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cache information and cleanup agree while preserving every unowned entry type."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    cache = tmp_path / "cache"
    write_cache("PXD000001", "project", {"title": "owned"}, cache_dir=cache)
    write_cache("PXD000001", "files", [], cache_dir=cache)
    unrelated = cache / "notes.txt"
    unrelated.write_text("keep", encoding="utf-8")
    legacy = cache / "PXD000002_project.json"
    legacy.write_text(json.dumps({"cache_version": 1, "data": {}}), encoding="utf-8")
    corrupt = cache / "PXD000003_project.json"
    corrupt.write_text("{broken", encoding="utf-8")
    temporary = cache / ".PXD000001_project.json.orphan.tmp"
    temporary.write_text("partial", encoding="utf-8")
    subdirectory = cache / "subdir"
    subdirectory.mkdir()

    runner = CliRunner()
    info = runner.invoke(main, ["--cache-dir", str(cache), "cache", "info"])
    cleared = runner.invoke(main, ["--cache-dir", str(cache), "cache", "clear", "--yes"])

    assert info.exit_code == 0
    assert "files=2" in info.output
    assert "ignored=5" in info.output
    assert "bytes=" in info.output
    assert "oldest=" in info.output
    assert "newest=" in info.output
    assert "n/a" not in info.output
    assert cleared.exit_code == 0
    assert f"cache_dir={cache}" in cleared.output
    assert "Removed 2" in cleared.output
    assert "Ignored entries: 5" in cleared.output
    assert not (cache / "PXD000001_project.json").exists()
    assert not (cache / "PXD000001_files.json").exists()
    assert unrelated.read_text() == "keep"
    assert legacy.exists()
    assert corrupt.exists()
    assert temporary.exists()
    assert subdirectory.is_dir()


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Issue #18: Windows symlink creation is restricted",
)
def test_cache_clear_never_follows_or_deletes_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cache-shaped symlink and its external target both survive cleanup."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    cache = tmp_path / "cache"
    cache.mkdir()
    external = tmp_path / "external.json"
    external.write_text("keep", encoding="utf-8")
    link = cache / "PXD000001_project.json"
    link.symlink_to(external)

    result = CliRunner().invoke(main, ["--cache-dir", str(cache), "cache", "clear", "--yes"])

    assert result.exit_code == 0
    assert "Removed 0" in result.output
    assert "Ignored entries: 1" in result.output
    assert link.is_symlink()
    assert external.read_text() == "keep"


@pytest.mark.parametrize("command", [["cache", "info"], ["cache", "clear", "--yes"]])
def test_cache_commands_refuse_filesystem_root(command: list[str]) -> None:
    """Information and cleanup reject a filesystem root before cache traversal."""
    root = Path.cwd().anchor
    result = CliRunner().invoke(main, ["--cache-dir", root, *command])
    assert result.exit_code == 2
    assert "unsafe cache directory" in result.output


def test_cache_clear_yes_refuses_current_directory_without_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The noninteractive flag cannot bypass current-directory safety validation."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    sentinel = tmp_path / "sentinel.txt"
    sentinel.write_text("keep", encoding="utf-8")

    result = CliRunner().invoke(main, ["--cache-dir", str(tmp_path), "cache", "clear", "--yes"])

    assert result.exit_code == 2
    assert sentinel.read_text() == "keep"


def test_cache_clear_refuses_empty_configured_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A blank configured cache path is rejected rather than normalized to the working directory."""
    config = tmp_path / "config.toml"
    config.write_text('cache_dir = ""\n', encoding="utf-8")
    monkeypatch.setenv("PXAUDIT_CONFIG", str(config))

    result = CliRunner().invoke(main, ["cache", "clear", "--yes"])

    assert result.exit_code == 2
    assert "cache directory is empty" in result.output


def test_cache_clear_with_only_ignored_entries_does_not_prompt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cleanup needs no confirmation when no validated entry can be removed."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "notes.txt").write_text("keep", encoding="utf-8")

    result = CliRunner().invoke(main, ["--cache-dir", str(cache), "cache", "clear"])

    assert result.exit_code == 0
    assert "Delete " not in result.output
    assert "Ignored entries: 1" in result.output


def test_cache_clear_reports_validated_unlink_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cleanup exits nonzero when an owned entry cannot be removed."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    cache = tmp_path / "cache"
    write_cache("PXD000001", "project", {}, cache_dir=cache)
    monkeypatch.setattr("pxaudit.cli.clear_cache", MagicMock(return_value=(0, 0, 1)))

    result = CliRunner().invoke(main, ["--cache-dir", str(cache), "cache", "clear", "--yes"])

    assert result.exit_code == 1
    assert "failed to remove 1" in result.output


def test_cache_clear_reports_revalidation_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cleanup reports when the cache root becomes unsafe after inspection."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    cache = tmp_path / "cache"
    write_cache("PXD000001", "project", {}, cache_dir=cache)
    monkeypatch.setattr(
        "pxaudit.cli.clear_cache", MagicMock(side_effect=CacheSafetyError("changed"))
    )

    result = CliRunner().invoke(main, ["--cache-dir", str(cache), "cache", "clear", "--yes"])

    assert result.exit_code == 1
    assert "became unsafe" in result.output


@pytest.mark.parametrize("failure", [CacheWriteError("secret/path"), CacheSafetyError("unsafe")])
def test_cache_write_failure_does_not_fail_successful_audit(
    mocks: dict, failure: Exception
) -> None:
    """A cache write failure warns while the successful API audit still persists."""
    mocks["write_cache"].side_effect = failure

    result = CliRunner().invoke(main, ["check", "PXD000001"])

    assert result.exit_code == 0
    assert result.output.count("cache write failed") == 2
    assert "secret/path" not in result.output
    mocks["insert_audit_record"].assert_called_once()
