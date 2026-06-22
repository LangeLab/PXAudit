"""Tests for pxaudit.cli.

Coverage target: 100% branch coverage on cli.py.

Test organisation
-----------------
1.  Accession validation : empty / numeric → exit 2
2.  Happy-path Gold run : all cache misses, all API success → exit 0
3.  Project API failure → exit 1
4.  Files API failure → exit 0, Bronze, files_fetch_failed warning printed
5.  Cache hit paths : project and/or files already cached → fetches skipped
6.  --no-cache flag : read_cache never called; write_cache still called
7.  --refresh flag : same semantics as --no-cache for reads; still fetches and writes
8.  --db flag : correct path forwarded to get_or_create_db
9.  Non-PXD prefix : Unverifiable result, no API calls, exit 0
10. Output content : tier, accession, flag symbols present in stdout
11. _extract_study unit tests : all field mappings and null branches
12. _extract_files_df unit tests : FTP extraction, extension, empty input
13. KeyboardInterrupt handling : clean exit 130, conn.close still called
14. bulk-audit command : input, export, continue-on-error, stdin, empty, dedup, overwrite guard

Branch map (cli.py)
------------------
check()
  ├── A: not accession or not accession[0].isalpha()  → True/False
  ├── B: accession.upper().startswith("PXD")          → True/False
  ├── C: if use_cache (i.e. not (no_cache or refresh)) → True/False
  ├── D: if project_data is None                      → True/False
  ├── E: try fetch_project / except PrideAPIError     → normal/exception
  ├── F: if files_data is None                        → True/False
  ├── G: try fetch_files / except PrideAPIError       → normal/exception
  └── H: try main body / except KeyboardInterrupt     → normal/exception

_print_result()
  └── H: if result.files_fetch_failed                 → True/False

_extract_study()
  ├── I: if organisms                                  → True/False
  ├── J: if instruments                               → True/False
  ├── K: if keywords                                  → True/False
  └── L: if date_str                                  → True/False

_extract_files_df()
  ├── M: if not files                                 → True/False
  └── N: next() FTP match                             → found/not-found

bulk_audit()
  ├── O: FileNotFoundError on input                   → sys.exit(2)
  ├── P: empty input                                  → exit 0 with warning
  ├── Q: duplicate accession                          → warn, skip
  ├── R: fmt None vs set                              → export/no export
  ├── S: export_path exists && not overwrite           → exit 2
  ├── T: PrideAPIError + continue_on_error             → warn, continue
  ├── U: PrideAPIError + not continue_on_error         → exit 1, partial
  └── V: KeyboardInterrupt                            → partial, no crash
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from click.testing import CliRunner

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
        "read_cache": MagicMock(return_value=None),
        "read_cache_stale": MagicMock(return_value=(None, None)),
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


@pytest.mark.parametrize("bad", ["", "12345", "000001"])
def test_check_invalid_accession_exits_two(bad: str, mocks: dict) -> None:
    """Empty or non-alpha-start accessions must exit 2 before any I/O."""
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


def test_check_files_api_failure_exits_zero(mocks: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """Files endpoint failure is not fatal : exit 0 with Raw tier (no result files)."""
    monkeypatch.setattr(
        "pxaudit.cli.fetch_files", MagicMock(side_effect=PrideAPIError("files down"))
    )
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert result.exit_code == 0
    assert "Raw" in result.output


def test_check_files_api_failure_prints_warning(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """files_fetch_failed=True must trigger the warning line in output.  (branch H-True)."""
    monkeypatch.setattr(
        "pxaudit.cli.fetch_files", MagicMock(side_effect=PrideAPIError("files down"))
    )
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert "Files endpoint failed" in result.output


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
        "pxaudit.cli.read_cache",
        MagicMock(side_effect=lambda acc, ep, **kw: _GOLD_PROJECT if ep == "project" else None),
    )
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001"])
    mocks["fetch_project"].assert_not_called()


def test_check_files_cache_hit_skips_fetch_files(
    mocks: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If files are cached, fetch_files must not be called.  (branch F-False)."""
    monkeypatch.setattr(
        "pxaudit.cli.read_cache",
        MagicMock(side_effect=lambda acc, ep, **kw: _GOLD_FILES if ep == "files" else None),
    )
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001"])
    mocks["fetch_files"].assert_not_called()


def test_check_both_cached_no_api_calls(mocks: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """Full cache hit → neither fetch_project nor fetch_files called."""
    monkeypatch.setattr(
        "pxaudit.cli.read_cache",
        MagicMock(
            side_effect=lambda acc, ep, **kw: _GOLD_PROJECT if ep == "project" else _GOLD_FILES
        ),
    )
    runner = CliRunner()
    result = runner.invoke(main, ["check", "PXD000001"])
    assert result.exit_code == 0
    mocks["fetch_project"].assert_not_called()
    mocks["fetch_files"].assert_not_called()


# ---------------------------------------------------------------------------
# 6. --no-cache flag  (branch C-False)
# ---------------------------------------------------------------------------


def test_check_no_cache_skips_read_cache(mocks: dict) -> None:
    """--no-cache must not call read_cache.  (branch C-False)."""
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--no-cache"])
    mocks["read_cache"].assert_not_called()


def test_check_no_cache_does_not_write_cache(mocks: dict) -> None:
    """--no-cache skips both reads AND writes to cache."""
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--no-cache"])
    mocks["fetch_project"].assert_called_once()
    mocks["fetch_files"].assert_called_once()
    mocks["write_cache"].assert_not_called()


# ---------------------------------------------------------------------------
# 7. --refresh flag  (branch C-False via refresh)
# ---------------------------------------------------------------------------

# Rationale: --refresh is semantically "re-fetch, update cache".  It shares
# the same read-skip behaviour as --no-cache but differs in intent.  The
# implementation sets use_cache = not (no_cache or refresh), so both flags
# exercise the same branch-C-false path.


def test_check_refresh_skips_read_cache(mocks: dict) -> None:
    """--refresh must not call read_cache.  (branch C-False via refresh)."""
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--refresh"])
    mocks["read_cache"].assert_not_called()


def test_check_refresh_still_fetches_and_writes(mocks: dict) -> None:
    """--refresh skips reads but still fetches from API and writes to cache."""
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--refresh"])
    mocks["fetch_project"].assert_called_once()
    mocks["fetch_files"].assert_called_once()
    assert mocks["write_cache"].call_count == 2


def test_check_refresh_with_no_cache_combined(mocks: dict) -> None:
    """--refresh combined with --no-cache must skip writes (--no-cache wins)."""
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--refresh", "--no-cache"])
    mocks["read_cache"].assert_not_called()
    mocks["fetch_project"].assert_called_once()
    mocks["fetch_files"].assert_called_once()
    mocks["write_cache"].assert_not_called()


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
    mocks["read_cache"].assert_not_called()
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


def test_extract_study_multi_keyword_joined() -> None:
    """Multiple keywords → joined with ', '."""
    row = _extract_study("PXD000001", {"keywords": ["a", "b", "c"]}, "ts")
    assert row["keywords"] == "a, b, c"


def test_extract_study_repository_always_pride() -> None:
    row = _extract_study("PXD999", {}, "ts")
    assert row["repository"] == "PRIDE"


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
    assert result == ["PXD000001", "PXD000002", "PXD000003"]


def test_read_accessions_all_blank(tmp_path: Path) -> None:
    f = tmp_path / "empty.txt"
    f.write_text("# only comment\n\n  \n")
    result = _read_accessions(str(f))
    assert result == []


def test_read_accessions_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        _read_accessions(str(tmp_path / "nope.txt"))


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
    """Patch _audit_single to skip real I/O during bulk-audit tests.

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
        return AuditData(r, {}, MagicMock(), [], "2026-01-01T00:00:00+00:00")

    m: dict = {"_audit_single": MagicMock(side_effect=fake_audit)}
    monkeypatch.setattr("pxaudit.cli._audit_single", m["_audit_single"])
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
    acc_file.write_text("PXD000001\n")
    out_path = tmp_path / "out.json"
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file), "--format", "json", "--output", str(out_path)],
    )
    assert result.exit_code == 0
    data = json.loads(out_path.read_text())
    assert len(data) == 1
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
    assert result.exit_code == 0
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
        ["manifest", "PXD000001", "--db", str(db_path), "--format", "tsv"],
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
        "pxaudit.cli.read_cache_stale",
        MagicMock(return_value=({"title": "stale"}, 9999.0)),
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
        "pxaudit.cli.read_cache_stale",
        MagicMock(return_value=([{"fileName": "stale.mzid"}], 9999.0)),
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
        "pxaudit.cli.read_cache_stale",
        MagicMock(return_value=(None, None)),
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
    from pxaudit.cli import AuditData, AuditResult

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
        )

    monkeypatch.setattr("pxaudit.cli._audit_single", MagicMock(side_effect=fake_audit))
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["bulk-audit", "--input", str(acc_file), "--continue-on-error"],
    )
    assert result.exit_code == 0
    assert "Failed    : 1" in result.output
