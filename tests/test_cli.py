"""Command-line orchestration, persistence, and output contract tests."""

from __future__ import annotations

import json
import sqlite3
import sys
from collections.abc import Generator, Iterable, Iterator
from io import StringIO
from pathlib import Path
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
    main,
)
from pxaudit.pride_client import PrideAPIError
from pxaudit.tier_engine import AuditResult

# ---------------------------------------------------------------------------
# Output mode reset (module globals on _output)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_output_mode() -> Generator[None, None, None]:
    from pxaudit import _output

    _output.configure(quiet=False, verbose=False, no_color=False)
    yield
    _output.configure(quiet=False, verbose=False, no_color=False)


# ---------------------------------------------------------------------------
# Synthetic PRIDE API payloads
# ---------------------------------------------------------------------------

_GOLD_PROJECT: dict = {
    "title": "TMT spikes study",
    "submissionDate": "2020-01-15",
    "keywords": ["proteomics", "phospho"],
    "organisms": [{"@type": "CvParam", "name": "Homo sapiens", "accession": "NEWT:9606"}],
    "instruments": [{"@type": "CvParam", "name": "Orbitrap Fusion"}],
}

# Diamond fixture: every flag True : used for "no ✘" output tests.
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mocks(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Patch all external I/O (cache, API, DB) for CLI integration tests.

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


# ---------------------------------------------------------------------------
# 1. Accession validation  (branch A)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["", "PXD12345", "PXDABCDEF", "MSV/000001"])
def test_check_invalid_accession_exits_two(bad: str, mocks: dict) -> None:
    """Malformed or unsafe accessions exit 2 before any I/O."""
    runner = CliRunner()
    result = runner.invoke(main, ["check", bad])
    assert result.exit_code == 2
    mocks["fetch_project"].assert_not_called()
    mocks["fetch_files"].assert_not_called()


# ---------------------------------------------------------------------------
# 2. Happy-path Gold run  (branches A-False, B-True, C-True, D-True, E-normal,
#    F-True, G-normal, H-False)
# ---------------------------------------------------------------------------


def test_check_valid_pxd_exits_zero(mocks: dict) -> None:
    """Valid PXD, full cache miss, all API success → exit 0."""
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert result.exit_code == 0


def test_check_valid_pxd_stdout_contains_accession_and_tier(mocks: dict) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert "PXD000001" in result.output
    assert "Gold" in result.output


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


def test_check_gold_stdout_contains_checkmarks(mocks: dict) -> None:
    """Gold tier: at least one ✔ must appear in the output.

    Gold requires SDRF + PSI results but is missing open spectra and organism
    part, so the output will contain both ✔ and ✘ symbols.  The assertion
    deliberately only checks that ✔ appears; for the "no ✘" invariant see
    test_check_diamond_stdout_no_crossmarks.
    """
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert "\u2714" in result.output


def test_check_diamond_stdout_no_crossmarks(mocks: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """Diamond tier with all flags True : output must contain only ✔, no ✘."""
    from unittest.mock import MagicMock

    monkeypatch.setattr("pxaudit.cli.fetch_project", MagicMock(return_value=_DIAMOND_PROJECT))
    monkeypatch.setattr("pxaudit.cli.fetch_files", MagicMock(return_value=_DIAMOND_FILES))
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert result.exit_code == 0
    assert "Diamond" in result.output
    assert "\u2714" in result.output
    assert "\u2718" not in result.output


def test_check_stdout_shows_quant_tier(mocks: dict) -> None:
    """Quant Tier line must appear in stdout for a Gold (Partial quant) run."""
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert "Quant Tier" in result.output
    assert "Partial" in result.output


def test_check_write_cache_called_on_miss(mocks: dict) -> None:
    """write_cache must be called twice (project + files) on a full cache miss."""
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001"])
    assert mocks["write_cache"].call_count == 2


def test_check_insert_functions_all_called(mocks: dict) -> None:
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001"])
    mocks["insert_audit_record"].assert_called_once()


# ---------------------------------------------------------------------------
# 3. Project API failure  (branch E-exception)
# ---------------------------------------------------------------------------


def test_check_project_api_failure_exits_one(mocks: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """PrideAPIError from fetch_project must produce exit code 1."""
    monkeypatch.setattr(
        "pxaudit.cli.fetch_project", MagicMock(side_effect=PrideAPIError("server error"))
    )
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert result.exit_code == 1


def test_check_project_api_failure_message_on_stderr(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("pxaudit.cli.fetch_project", MagicMock(side_effect=PrideAPIError("boom")))
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert "Error" in result.output


# ---------------------------------------------------------------------------
# 4. Files API failure  (branch G-exception, H-True)
# ---------------------------------------------------------------------------


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
    assert "audit is incomplete" in result.stderr
    assert "Tier" not in result.stdout
    compute.assert_not_called()
    mocks["get_or_create_db"].assert_not_called()
    mocks["insert_audit_record"].assert_not_called()


def test_check_files_api_failure_prints_warning(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An incomplete files response reports that no database records changed."""
    monkeypatch.setattr(
        "pxaudit.cli.fetch_files", MagicMock(side_effect=PrideAPIError("files down"))
    )
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert result.exit_code == 1
    assert "Warning" in result.stderr
    assert "no database records were created or replaced" in result.stderr


def test_check_files_api_failure_does_not_write_files_cache(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """write_cache must be called only once (project) when files endpoint fails."""
    monkeypatch.setattr(
        "pxaudit.cli.fetch_files", MagicMock(side_effect=PrideAPIError("files down"))
    )
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001"])
    assert mocks["write_cache"].call_count == 1


# ---------------------------------------------------------------------------
# 5. Cache hit paths  (branches D-False, F-False)
# ---------------------------------------------------------------------------


def test_check_project_cache_hit_skips_fetch_project(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If project is cached, fetch_project must not be called.  (branch D-False)."""
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
    """If files are cached, fetch_files must not be called.  (branch F-False)."""
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
    """Full cache hit → neither fetch_project nor fetch_files called."""
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


def test_full_cache_hit_persists_retrieval_time_with_real_cache_and_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real cache-to-SQLite workflow preserves project retrieval provenance."""
    import sqlite3

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
    with sqlite3.connect(database) as connection:
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


# ---------------------------------------------------------------------------
# --no-cache flag
# ---------------------------------------------------------------------------


def test_check_no_cache_skips_read_cache(mocks: dict) -> None:
    """Disabled cache mode skips both fresh and stale cache reads."""
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--no-cache"])
    mocks["read_cache_response"].assert_not_called()
    mocks["read_cache_stale_response"].assert_not_called()


def test_check_no_cache_does_not_write_cache(mocks: dict) -> None:
    """Disabled cache mode fetches live responses without writing them."""
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--no-cache"])
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


# ---------------------------------------------------------------------------
# --refresh flag
# ---------------------------------------------------------------------------


def test_check_refresh_skips_read_cache(mocks: dict) -> None:
    """Refresh mode skips fresh cache reads."""
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--refresh"])
    mocks["read_cache_response"].assert_not_called()


def test_check_refresh_still_fetches_and_writes(mocks: dict) -> None:
    """Refresh skips fresh reads but fetches live responses and writes successes."""
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--refresh"])
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


# ---------------------------------------------------------------------------
# 8. --db flag
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# 9. Non-PXD prefix  (branch B-False)
# ---------------------------------------------------------------------------


def test_check_non_pxd_exits_zero(mocks: dict) -> None:
    """Non-PXD accessions are Unverifiable : exit 0."""
    runner = CliRunner()
    result = runner.invoke(main, ["check", "MSV000001"])
    assert result.exit_code == 0
    assert "Unverifiable" in result.output


def test_check_non_pxd_makes_no_api_calls(mocks: dict) -> None:
    """Non-PXD accessions must not trigger cache reads or any API calls.  (branch B-False)."""
    runner = CliRunner()
    runner.invoke(main, ["check", "MSV000001"])
    mocks["read_cache_response"].assert_not_called()
    mocks["fetch_project"].assert_not_called()
    mocks["fetch_files"].assert_not_called()


# ---------------------------------------------------------------------------
# 10. _extract_study unit tests  (branches I, J, K, L)
# ---------------------------------------------------------------------------


def test_extract_study_all_fields_populated() -> None:
    """Full project dict : every field must be correctly extracted.  (I/J/K/L True)."""
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


def test_extract_study_empty_organisms_gives_none() -> None:
    """No organisms → organism and organism_id must be None.  (branch I-False)."""
    row = _extract_study("PXD000001", {"organisms": []}, "ts")
    assert row["organism"] is None
    assert row["organism_id"] is None


def test_extract_study_empty_instruments_gives_none() -> None:
    """No instruments → instrument must be None.  (branch J-False)."""
    row = _extract_study("PXD000001", {"instruments": []}, "ts")
    assert row["instrument"] is None


def test_extract_study_empty_keywords_gives_none() -> None:
    """Empty keywords list → keywords column must be None.  (branch K-False)."""
    row = _extract_study("PXD000001", {"keywords": []}, "ts")
    assert row["keywords"] is None


def test_extract_study_missing_date_gives_none_year() -> None:
    """No submissionDate → submission_year must be None.  (branch L-False)."""
    row = _extract_study("PXD000001", {}, "ts")
    assert row["submission_year"] is None


def test_extract_study_malformed_date_gives_none_year() -> None:
    """Malformed submissionDate must not crash; submission_year is None."""
    row = _extract_study("PXD000001", {"submissionDate": "not-a-date"}, "ts")
    assert row["submission_year"] is None


def test_extract_study_short_date_gives_none_year() -> None:
    """Date string shorter than 4 chars → submission_year is None."""
    row = _extract_study("PXD000001", {"submissionDate": "20"}, "ts")
    assert row["submission_year"] is None


def test_extract_study_non_digit_prefix_gives_none_year() -> None:
    """Date starting with non-digits → submission_year is None."""
    row = _extract_study("PXD000001", {"submissionDate": "abcd-ef-gh"}, "ts")
    assert row["submission_year"] is None


def test_extract_study_multi_keyword_joined() -> None:
    """Multiple keywords → joined with ', '."""
    row = _extract_study("PXD000001", {"keywords": ["a", "b", "c"]}, "ts")
    assert row["keywords"] == "a, b, c"


def test_extract_study_records_pride_repository() -> None:
    row = _extract_study("PXD999", {}, "ts")
    assert row["repository"] == "PRIDE"


@pytest.mark.parametrize(
    ("accession", "repository"),
    [
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
    """submissionType present in project → stored in submission_type field."""
    row = _extract_study("PXD000001", {"submissionType": "PARTIAL"}, "ts")
    assert row["submission_type"] == "PARTIAL"


def test_extract_study_missing_submission_type_gives_none() -> None:
    """No submissionType key → submission_type is None (not KeyError)."""
    row = _extract_study("PXD000001", {}, "ts")
    assert row["submission_type"] is None


# ---------------------------------------------------------------------------
# 11. _extract_files_df unit tests  (branches M, N)
# ---------------------------------------------------------------------------


def test_extract_files_df_empty_gives_empty_dataframe() -> None:
    """Empty files list → DataFrame with correct columns, zero rows.  (branch M-True)."""
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


def test_extract_files_df_shape() -> None:
    """Non-empty files list → one row per file.  (branch M-False)."""
    df = _extract_files_df("PXD000001", _GOLD_FILES)
    assert len(df) == len(_GOLD_FILES)


def test_extract_files_df_columns_present() -> None:
    df = _extract_files_df("PXD000001", _GOLD_FILES)
    expected = {
        "accession",
        "file_name",
        "file_category",
        "file_extension",
        "ftp_location",
        "file_size",
        "checksum",
        "checksum_type",
    }
    assert set(df.columns) == expected


def test_extract_files_df_ftp_location_extracted() -> None:
    """FTP Protocol entry in publicFileLocations must populate ftp_location.  (branch N-found)."""
    df = _extract_files_df("PXD000001", _GOLD_FILES)
    assert df.loc[0, "ftp_location"] == "ftp://ftp.ebi.ac.uk/results.mzid"


def test_extract_files_df_no_ftp_gives_none() -> None:
    """No FTP Protocol in publicFileLocations → ftp_location is None.  (branch N-not-found)."""
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
    """file_extension must be the suffix of fileName."""
    file = {
        "fileName": "results.mzTab",
        "fileCategory": {"value": "RESULT"},
        "fileSizeBytes": 100,
        "publicFileLocations": [],
    }
    df = _extract_files_df("PXD000001", [file])
    assert df.loc[0, "file_extension"] == ".mzTab"


def test_extract_files_df_missing_filename_gives_empty_name() -> None:
    """File with no fileName key → file_name is empty string, extension is None."""
    file = {"fileCategory": {"value": "RAW"}, "fileSizeBytes": 0, "publicFileLocations": []}
    df = _extract_files_df("PXD000001", [file])
    assert df.loc[0, "file_name"] == ""
    assert df.loc[0, "file_extension"] is None


def test_extract_files_df_accession_column_correct() -> None:
    df = _extract_files_df("PXD999999", _GOLD_FILES)
    assert (df["accession"] == "PXD999999").all()


# ---------------------------------------------------------------------------
# 12. KeyboardInterrupt handling  (branch #13)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# 13. bulk-audit helpers : unit tests
# ---------------------------------------------------------------------------


def test_default_export_path_tsv() -> None:
    """Default export path has expected format for TSV."""
    path = _default_export_path("tsv")
    assert path.startswith("pxaudit_bulk_")
    assert path.endswith(".tsv")
    assert len(path) == len("pxaudit_bulk_20260525.tsv")


def test_default_export_path_json() -> None:
    path = _default_export_path("json")
    assert path.endswith(".json")


def test_read_accessions_file(tmp_path: Path) -> None:
    """Read accessions from a file, skipping blanks and comments."""
    f = tmp_path / "accessions.txt"
    f.write_text("PXD000001\n\n# comment\nPXD000002\n  PXD000003  \n")
    result = _read_accessions(str(f))
    assert result == [(1, "PXD000001"), (4, "PXD000002"), (5, "PXD000003")]


def test_read_accessions_all_blank(tmp_path: Path) -> None:
    f = tmp_path / "empty.txt"
    f.write_text("# only comment\n\n  \n")
    result = _read_accessions(str(f))
    assert result == []


def test_read_accessions_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        _read_accessions(str(tmp_path / "nope.txt"))


def test_read_accessions_stdin_preserves_source_line_numbers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stdin records retain physical line numbers after blank and comment filtering."""
    monkeypatch.setattr(sys, "stdin", StringIO("# note\npxd000001\n"))
    assert _read_accessions("-") == [(2, "pxd000001")]


def test_result_to_row_keys_match_export_cols() -> None:
    from pxaudit.cli import _EXPORT_COLS

    result = AuditResult(accession="PXD000001", tier="Diamond")
    row = _result_to_row(result)
    assert list(row.keys()) == list(_EXPORT_COLS)
    assert row["accession"] == "PXD000001"
    assert row["tier"] == "Diamond"


def test_export_tsv(tmp_path: Path) -> None:
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
    assert lines[0].startswith("accession")  # header


def test_export_csv(tmp_path: Path) -> None:
    results = [AuditResult(accession="PXD000001", tier="Raw")]
    path = str(tmp_path / "out.csv")
    _export_csv(results, path)
    content = Path(path).read_text()
    assert "PXD000001" in content
    assert "," in content  # CSV delimiter


def test_export_json(tmp_path: Path) -> None:
    results = [AuditResult(accession="PXD000001", tier="Diamond")]
    path = str(tmp_path / "out.json")
    _export_json(results, path)
    data = json.loads(Path(path).read_text())
    assert len(data) == 1
    assert data[0]["accession"] == "PXD000001"
    assert data[0]["tier"] == "Diamond"


# ---------------------------------------------------------------------------
# 14. bulk-audit command : integration tests
# ---------------------------------------------------------------------------


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

    m: dict = {"_audit_single": MagicMock(side_effect=fake_audit)}
    monkeypatch.setattr("pxaudit.cli._audit_single", m["_audit_single"])
    monkeypatch.setattr("pxaudit.cli.time.sleep", lambda _: None)
    return m


def test_bulk_audit_happy_path_tsv(bulk_mocks: dict, tmp_path: Path) -> None:
    """Two accessions, export TSV → exit 0, file written."""
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


def test_bulk_audit_happy_path_json(bulk_mocks: dict, tmp_path: Path) -> None:
    """Export JSON format works."""
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
    """Export CSV format works."""
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
    """No --output given → default filename generated."""
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
    # Swap order: PXD000001 first (succeeds), then UNKNOWN_ACC fails
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
    """Empty input file → exit 0 with warning."""
    acc_file = tmp_path / "empty.txt"
    acc_file.write_text("# nothing\n")
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file)],
    )
    assert result.exit_code == 0
    assert "no accessions" in result.output


def test_bulk_audit_missing_input_file() -> None:
    """Non-existent input file → exit 2."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", "/nonexistent/file.txt"],
    )
    assert result.exit_code == 2
    assert "not found" in result.output


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
    """Existing output file without --overwrite → exit 2."""
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


# ---------------------------------------------------------------------------
# 15. manifest command tests
# ---------------------------------------------------------------------------


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

    assert result.exit_code == 2
    assert "cannot read database" in result.stderr
    assert database.read_bytes() == original


def test_manifest_database_open_error_exits_two(
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

    assert result.exit_code == 2
    assert "cannot read database" in result.stderr


def test_manifest_tsv_output(tmp_path: Path) -> None:
    """manifest --format tsv prints tab-separated file listing."""
    from pxaudit.db import get_or_create_db, insert_study, insert_study_files

    db_path = tmp_path / "test.db"
    conn = get_or_create_db(db_path)
    try:
        insert_study(conn, {"accession": "PXD000001", "fetched_at": "now"})
        df = pd.DataFrame(
            [
                {
                    "accession": "PXD000001",
                    "file_name": "test.raw",
                    "file_category": "RAW",
                    "file_extension": ".raw",
                    "ftp_location": "ftp://example/test.raw",
                    "file_size": 1024,
                    "checksum": "abc123",
                    "checksum_type": "MD5",
                }
            ]
        )
        insert_study_files(conn, "PXD000001", df)
    finally:
        conn.close()

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["manifest", "pxd000001", "--db", str(db_path), "--format", "tsv"],
    )
    assert result.exit_code == 0
    assert "test.raw" in result.output
    assert ".raw" in result.output
    assert "abc123" in result.output


def test_manifest_json_output(tmp_path: Path) -> None:
    """manifest --format json prints JSON file listing."""
    from pxaudit.db import get_or_create_db, insert_study, insert_study_files

    db_path = tmp_path / "test.db"
    conn = get_or_create_db(db_path)
    try:
        insert_study(conn, {"accession": "PXD000001", "fetched_at": "now"})
        df = pd.DataFrame(
            [
                {
                    "accession": "PXD000001",
                    "file_name": "test.raw",
                    "file_category": "RAW",
                    "file_extension": ".raw",
                    "ftp_location": "ftp://example/test.raw",
                    "file_size": 1024,
                    "checksum": None,
                    "checksum_type": None,
                }
            ]
        )
        insert_study_files(conn, "PXD000001", df)
    finally:
        conn.close()

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["manifest", "PXD000001", "--db", str(db_path), "--format", "json"],
    )
    assert result.exit_code == 0
    assert "test.raw" in result.output
    assert "file_name" in result.output


# ---------------------------------------------------------------------------
# 16. Stale cache fallback tests
# ---------------------------------------------------------------------------


def test_check_stale_cache_fallback_on_project_failure(
    mocks: dict, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When project fetch fails, stale cached project data must be served with warning."""
    monkeypatch.setattr("pxaudit.cli.fetch_project", MagicMock(side_effect=PrideAPIError("down")))
    monkeypatch.setattr(
        "pxaudit.cli.read_cache_stale_response",
        MagicMock(return_value=_cached({"title": "stale"}, age=9999.0)),
    )
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert result.exit_code == 0
    assert "stale cached project data" in result.output


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


def test_check_stale_cache_fallback_project_fails_no_cache(
    mocks: dict, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When project fetch fails and no stale cache, must exit 1."""
    monkeypatch.setattr("pxaudit.cli.fetch_project", MagicMock(side_effect=PrideAPIError("down")))
    monkeypatch.setattr(
        "pxaudit.cli.read_cache_stale_response",
        MagicMock(return_value=None),
    )
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert result.exit_code == 1


def test_bulk_audit_stale_cache_fallback_on_project_failure(
    bulk_mocks: dict, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """bulk-audit with stale fallback must continue with warning."""
    acc_file = tmp_path / "ids.txt"
    acc_file.write_text("PXD000001\nPXD000002\n")
    # Fake audit that fails for PXD000001 but has stale cache.
    from pxaudit.cli import AuditData
    from pxaudit.tier_engine import AuditResult

    call_count = [0]

    def fake_audit(acc: str, db: str, **kw: object) -> AuditData:
        call_count[0] += 1
        if acc == "PXD000001":
            raise PrideAPIError("down")
        return AuditData(
            AuditResult(accession=acc, tier="Diamond"),
            {},
            MagicMock(),
            [],
            "ts",
            [],
            [],
            True,
        )

    monkeypatch.setattr("pxaudit.cli._audit_single", MagicMock(side_effect=fake_audit))
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file), "--continue-on-error"],
    )
    assert result.exit_code == 0
    assert "Failed    : 1" in result.output


# ---------------------------------------------------------------------------
# Group flags, quiet/verbose matrix, cache commands, delay skip
# ---------------------------------------------------------------------------


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


def test_cache_info_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """cache info on empty/missing dir exits 0 with zeros."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    cache = tmp_path / "cache"
    runner = CliRunner()
    result = runner.invoke(main, ["--cache-dir", str(cache), "cache", "info"])
    assert result.exit_code == 0
    assert f"cache_dir={cache}" in result.output
    assert "files=0" in result.output
    assert "bytes=0" in result.output


def test_cache_info_with_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """cache info reports owned count, ignored count, bytes, and modification times."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    cache = tmp_path / "cache"
    cache.mkdir()
    write_cache("PXD000001", "project", {"title": "one"}, cache_dir=cache)
    write_cache("PXD000001", "files", [], cache_dir=cache)
    (cache / "notes.txt").write_text("keep")
    runner = CliRunner()
    result = runner.invoke(main, ["--cache-dir", str(cache), "cache", "info"])
    assert result.exit_code == 0
    assert "files=2" in result.output
    assert "ignored=1" in result.output
    assert "bytes=" in result.output
    assert "oldest=" in result.output
    assert "newest=" in result.output
    assert "n/a" not in result.output


def test_cache_clear_yes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """cache clear --yes deletes a validated entry after printing its root."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    cache = tmp_path / "cache"
    cache.mkdir()
    write_cache("PXD000001", "project", {}, cache_dir=cache)
    runner = CliRunner()
    result = runner.invoke(main, ["--cache-dir", str(cache), "cache", "clear", "--yes"])
    assert result.exit_code == 0
    assert f"cache_dir={cache}" in result.output
    assert not (cache / "PXD000001_project.json").exists()
    assert "Removed 1" in result.output


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


def test_manifest_unaffected_by_quiet(tmp_path: Path) -> None:
    """manifest under -q still emits pure TSV body."""
    from pxaudit.db import get_or_create_db, insert_audit_record
    from pxaudit.tier_engine import compute_audit

    db = tmp_path / "m.db"
    conn = get_or_create_db(str(db))
    try:
        project = {
            "title": "t",
            "organisms": [{"name": "Homo sapiens", "accession": "NEWT:9606"}],
            "instruments": [{"name": "Orbitrap"}],
            "submissionDate": "2020-01-01",
        }
        files = [
            {
                "fileName": "a.mzid",
                "fileCategory": {"value": "RESULT"},
                "fileSizeBytes": 1,
                "publicFileLocations": [],
            }
        ]
        result = compute_audit("PXD000009", project, files, files_fetch_failed=False)
        study = {
            "accession": "PXD000009",
            "title": "t",
            "organism": "Homo sapiens",
            "organism_id": "NEWT:9606",
            "instrument": "Orbitrap",
            "submission_year": 2020,
            "submission_type": None,
            "keywords": None,
            "repository": "PRIDE",
            "fetched_at": "ts",
        }
        import pandas as pd

        files_df = pd.DataFrame(
            [
                {
                    "accession": "PXD000009",
                    "file_name": "a.mzid",
                    "file_category": "RESULT",
                    "file_extension": ".mzid",
                    "ftp_location": None,
                    "file_size": 1,
                    "checksum": None,
                    "checksum_type": None,
                }
            ]
        )
        insert_audit_record(conn, study, "PXD000009", files_df, result.__dict__)
    finally:
        conn.close()

    runner = CliRunner()
    out = runner.invoke(main, ["-q", "manifest", "PXD000009", "--db", str(db)])
    assert out.exit_code == 0
    assert "file_name" in out.output
    assert "a.mzid" in out.output
    assert "Metadata" not in out.output


def test_cache_info_empty_existing_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Existing empty cache dir reports zeros and n/a mtimes."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    cache = tmp_path / "cache"
    cache.mkdir()
    runner = CliRunner()
    result = runner.invoke(main, ["--cache-dir", str(cache), "cache", "info"])
    assert result.exit_code == 0
    assert "files=0" in result.output
    assert "oldest=n/a" in result.output


def test_cache_stats_stat_oserror(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """_cache_stats reports invalid files as ignored and returns no timestamps."""
    from pxaudit.cli import _cache_stats

    cache = tmp_path / "cache"
    cache.mkdir()
    bad = cache / "PXD000001_project.json"
    bad.write_text("x")

    count, ignored, total, oldest, newest = _cache_stats(cache)
    assert count == 0
    assert ignored == 1
    assert total == 0
    assert oldest is None
    assert newest is None


def test_cache_stats_all_stat_fail(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """_cache_stats aggregates one validated entry."""
    from pxaudit.cli import _cache_stats

    cache = tmp_path / "cache"
    cache.mkdir()
    write_cache("PXD000001", "project", {}, cache_dir=cache)

    count, ignored, total, oldest, newest = _cache_stats(cache)
    assert count == 1
    assert ignored == 0
    assert total > 0
    assert oldest is not None
    assert newest is not None


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


def test_cache_clear_skips_directories(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """cache clear only unlinks files, not subdirectories."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    cache = tmp_path / "cache"
    cache.mkdir()
    write_cache("PXD000001", "project", {}, cache_dir=cache)
    (cache / "subdir").mkdir()
    runner = CliRunner()
    result = runner.invoke(main, ["--cache-dir", str(cache), "cache", "clear", "--yes"])
    assert result.exit_code == 0
    assert not (cache / "PXD000001_project.json").exists()
    assert (cache / "subdir").is_dir()


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


def test_report_existing_output_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """report exits 2 when output dir exists without --overwrite."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    db = tmp_path / "x.db"
    db.write_text("")
    out = tmp_path / "outdir"
    out.mkdir()
    runner = CliRunner()
    result = runner.invoke(main, ["report", "--db", str(db), "--output", str(out)])
    assert result.exit_code == 2
    assert "already exists" in result.output


def test_report_success_and_verbose(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """report success path and verbose detail."""
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
    ("exc", "code"),
    [
        (ValueError("bad"), 1),
        (ImportError("no jinja"), 1),
        (FileNotFoundError("gone"), 2),
        (PermissionError("denied"), 2),
    ],
)
def test_report_error_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exc: Exception, code: int
) -> None:
    """report maps generate_report exceptions to exit codes."""
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


def test_report_sqlite_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """sqlite3.DatabaseError maps to exit 2."""
    import sqlite3

    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    db = tmp_path / "x.db"
    db.write_text("")
    out = tmp_path / "outdir"

    def boom(*a: object, **k: object) -> str:
        raise sqlite3.DatabaseError("corrupt")

    monkeypatch.setattr("pxaudit.report.generate_report", boom)
    runner = CliRunner()
    result = runner.invoke(main, ["report", "--db", str(db), "--output", str(out), "--overwrite"])
    assert result.exit_code == 2


def test_cache_stats_skips_directories(tmp_path: Path) -> None:
    """Directories inside the cache are ignored by _cache_stats."""
    from pxaudit.cli import _cache_stats

    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "subdir").mkdir()
    write_cache("PXD000001", "project", {}, cache_dir=cache)
    count, ignored, total, oldest, newest = _cache_stats(cache)
    assert count == 1
    assert ignored == 1
    assert total > 0
    assert oldest is not None


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


def test_bulk_negative_delay_exits_two(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """--delay must be non-negative."""
    monkeypatch.setenv("PXAUDIT_CONFIG", str(tmp_path / "none.toml"))
    acc = tmp_path / "ids.txt"
    acc.write_text("PXD000001\n")
    runner = CliRunner()
    result = runner.invoke(main, ["bulk-audit", "--input", str(acc), "--delay", "-1"])
    assert result.exit_code == 2
    assert "non-negative" in result.output


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


def test_manifest_unaffected_by_verbose(tmp_path: Path) -> None:
    """manifest under -v still emits pure TSV body (no status chrome)."""
    import pandas as pd

    from pxaudit.db import get_or_create_db, insert_audit_record
    from pxaudit.tier_engine import compute_audit

    db = tmp_path / "m.db"
    conn = get_or_create_db(str(db))
    try:
        project = {
            "title": "t",
            "organisms": [{"name": "Homo sapiens", "accession": "NEWT:9606"}],
            "instruments": [{"name": "Orbitrap"}],
            "submissionDate": "2020-01-01",
        }
        files = [
            {
                "fileName": "a.mzid",
                "fileCategory": {"value": "RESULT"},
                "fileSizeBytes": 1,
                "publicFileLocations": [],
            }
        ]
        result = compute_audit("PXD000009", project, files, files_fetch_failed=False)
        study = {
            "accession": "PXD000009",
            "title": "t",
            "organism": "Homo sapiens",
            "organism_id": "NEWT:9606",
            "instrument": "Orbitrap",
            "submission_year": 2020,
            "submission_type": None,
            "keywords": None,
            "repository": "PRIDE",
            "fetched_at": "ts",
        }
        files_df = pd.DataFrame(
            [
                {
                    "accession": "PXD000009",
                    "file_name": "a.mzid",
                    "file_category": "RESULT",
                    "file_extension": ".mzid",
                    "ftp_location": None,
                    "file_size": 1,
                    "checksum": None,
                    "checksum_type": None,
                }
            ]
        )
        insert_audit_record(conn, study, "PXD000009", files_df, result.__dict__)
    finally:
        conn.close()

    runner = CliRunner()
    out = runner.invoke(main, ["-v", "manifest", "PXD000009", "--db", str(db)])
    assert out.exit_code == 0
    assert "file_name" in out.stdout
    assert "a.mzid" in out.stdout
    assert "Metadata" not in out.stdout
    assert "cache" not in out.stdout.lower()


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
    assert cleared.exit_code == 0
    assert "Removed 2" in cleared.output
    assert "Ignored entries: 5" in cleared.output
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
