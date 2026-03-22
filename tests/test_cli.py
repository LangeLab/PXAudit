"""Tests for pxaudit.cli.

Coverage target: 100% branch coverage on cli.py.

Test organisation
-----------------
1.  Accession validation — empty / numeric → exit 2
2.  Happy-path Gold run — all cache misses, all API success → exit 0
3.  Project API failure → exit 1
4.  Files API failure → exit 0, Bronze, files_fetch_failed warning printed
5.  Cache hit paths — project and/or files already cached → fetches skipped
6.  --no-cache flag — read_cache never called; write_cache still called
7.  --db flag — correct path forwarded to get_or_create_db
8.  Non-PXD prefix — Unverifiable result, no API calls, exit 0
9.  Output content — tier, accession, flag symbols present in stdout
10. _extract_study unit tests — all field mappings and null branches
11. _extract_files_df unit tests — FTP extraction, extension, empty input

Branch map (cli.py)
-------------------
check()
  ├── A: not accession or not accession[0].isalpha()  → True/False
  ├── B: accession.upper().startswith("PXD")          → True/False
  ├── C: if not no_cache                              → True/False
  ├── D: if project_data is None                      → True/False
  ├── E: try fetch_project / except PrideAPIError     → normal/exception
  ├── F: if files_data is None                        → True/False
  └── G: try fetch_files / except PrideAPIError       → normal/exception

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
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from pxaudit.cli import _extract_files_df, _extract_study, main
from pxaudit.pride_client import PrideAPIError

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

# Diamond fixture: every flag True — used for "no ✘" output tests.
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
        "write_cache": MagicMock(),
        "fetch_project": MagicMock(return_value=_GOLD_PROJECT),
        "fetch_files": MagicMock(return_value=_GOLD_FILES),
        "get_or_create_db": MagicMock(return_value=MagicMock()),
        "insert_study": MagicMock(),
        "insert_study_files": MagicMock(),
        "insert_audit": MagicMock(),
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
    """Diamond tier with all flags True — output must contain only ✔, no ✘."""
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
    mocks["insert_study"].assert_called_once()
    mocks["insert_study_files"].assert_called_once()
    mocks["insert_audit"].assert_called_once()


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
    """Files endpoint failure is not fatal — exit 0 with Raw tier (no result files)."""
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


def test_check_no_cache_still_fetches_and_writes(mocks: dict) -> None:
    """--no-cache skips reads but still fetches from API and writes to cache."""
    runner = CliRunner()
    runner.invoke(main, ["check", "PXD000001", "--no-cache"])
    mocks["fetch_project"].assert_called_once()
    mocks["fetch_files"].assert_called_once()
    assert mocks["write_cache"].call_count == 2


# ---------------------------------------------------------------------------
# 7. --db flag
# ---------------------------------------------------------------------------


def test_check_db_path_forwarded_to_get_or_create_db(
    mocks: dict, tmp_path: pytest.TempPathFactory
) -> None:
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
# 8. Non-PXD prefix  (branch B-False)
# ---------------------------------------------------------------------------


def test_check_non_pxd_exits_zero(mocks: dict) -> None:
    """Non-PXD accessions are Unverifiable — exit 0."""
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
# 9. _extract_study unit tests  (branches I, J, K, L)
# ---------------------------------------------------------------------------


def test_extract_study_all_fields_populated() -> None:
    """Full project dict — every field must be correctly extracted.  (I/J/K/L True)."""
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
# 10. _extract_files_df unit tests  (branches M, N)
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
