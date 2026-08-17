"""Explicit live verification against PRIDE Archive REST API v3.

The reviewed baseline is dated 2026-07-18 UTC and covers six representative
submissions. Set ``PXAUDIT_LIVE_RECORD`` to write a JSON record of observed tier
and evidence drift.

Run with ``uv run pytest -m integration -v --no-cov``. These tests are excluded
from default offline runs.
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cache
from pathlib import Path

import pytest
from click.testing import CliRunner

from pxaudit.cli import _extract_files_df, main
from pxaudit.pride_client import fetch_files, fetch_project
from pxaudit.tier_engine import AuditResult, FlagOutcome, compute_audit

pytestmark = pytest.mark.integration

_BASELINE_DATE = "2026-07-18"


@dataclass(frozen=True)
class _LiveCase:
    """Reviewed tier profile for one live accession."""

    accession: str
    tier: str
    quant_tier: str
    evidence: tuple[tuple[str, bool], ...]


_LIVE_CASES = (
    _LiveCase(
        "PXD057701",
        "Raw",
        "No Quant",
        (
            ("has_result_files", False),
            ("has_psi_results", False),
            ("has_tabular_quant", False),
        ),
    ),
    _LiveCase(
        "PXD002244",
        "Bronze",
        "No Quant",
        (
            ("has_result_files", True),
            ("has_psi_results", False),
            ("has_tabular_quant", False),
        ),
    ),
    _LiveCase(
        "PXD000001",
        "Silver",
        "Partial",
        (
            ("has_result_files", True),
            ("has_psi_results", True),
            ("has_sdrf", False),
            ("has_tabular_quant", False),
        ),
    ),
    _LiveCase(
        "PXD004683",
        "Diamond",
        "Partial",
        (
            ("has_result_files", True),
            ("has_psi_results", True),
            ("has_sdrf", True),
            ("has_open_spectra", True),
            ("has_organism_part", True),
            ("has_publication", True),
            ("has_tabular_quant", False),
        ),
    ),
    _LiveCase(
        "PXD073444",
        "Platinum",
        "Partial",
        (
            ("has_result_files", True),
            ("has_psi_results", True),
            ("has_sdrf", True),
            ("has_open_spectra", True),
            ("has_organism_part", True),
            ("has_publication", False),
            ("has_tabular_quant", False),
        ),
    ),
    _LiveCase(
        "PXD075811",
        "Platinum",
        "Partial",
        (
            ("has_result_files", True),
            ("has_psi_results", True),
            ("has_sdrf", True),
            ("has_open_spectra", True),
            ("has_organism_part", True),
            ("has_publication", False),
            ("has_tabular_quant", False),
        ),
    ),
)

_LIVE_OBSERVATIONS: list[dict[str, object]] = []


@cache
def _fetch_live_audit(accession: str) -> tuple[list[dict], AuditResult]:
    """Fetch one accession once and compute its audit profile."""
    project = fetch_project(accession)
    files = fetch_files(accession)
    return files, compute_audit(accession, project, files)


@pytest.fixture(scope="module", autouse=True)
def _write_live_verification_record() -> Iterator[None]:
    """Write requested live evidence even when the observed profile has drifted."""
    _fetch_live_audit.cache_clear()
    _LIVE_OBSERVATIONS.clear()
    yield

    destination = os.environ.get("PXAUDIT_LIVE_RECORD")
    if not destination:
        return

    observations = sorted(_LIVE_OBSERVATIONS, key=lambda item: str(item["accession"]))
    observed_accessions = [str(item["accession"]) for item in observations]
    missing_accessions = [
        case.accession for case in _LIVE_CASES if case.accession not in observed_accessions
    ]
    record = {
        "run_date": datetime.now(UTC).date().isoformat(),
        "baseline_date": _BASELINE_DATE,
        "api_version": "PRIDE Archive REST API v3",
        "api_base_url": "https://www.ebi.ac.uk/pride/ws/archive/v3",
        "accession_inventory": [case.accession for case in _LIVE_CASES],
        "observed_accessions": observed_accessions,
        "missing_accessions": missing_accessions,
        "complete": not missing_accessions,
        "observations": observations,
        "tier_changes": [item for item in observations if item["tier_changed"]],
        "evidence_changes": [item for item in observations if item["evidence_changed"]],
    }
    record_path = Path(destination)
    record_path.parent.mkdir(parents=True, exist_ok=True)
    record_path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")


@pytest.mark.parametrize("case", _LIVE_CASES, ids=lambda case: case.accession)
def test_live_audit_profiles_match_reviewed_baseline(case: _LiveCase) -> None:
    """Live tiers and their driving evidence match the dated reviewed profile."""
    _, result = _fetch_live_audit(case.accession)
    expected_evidence = dict(case.evidence)
    observed_evidence = {field: getattr(result, field) for field in expected_evidence}
    tier_changed = (result.tier, result.quant_tier) != (case.tier, case.quant_tier)
    evidence_changed = any(
        observed_evidence[field].value != ("passed" if expected else "failed")
        for field, expected in case.evidence
    )
    _LIVE_OBSERVATIONS.append(
        {
            "accession": case.accession,
            "expected_tier": case.tier,
            "observed_tier": result.tier,
            "expected_quant_tier": case.quant_tier,
            "observed_quant_tier": result.quant_tier,
            "expected_evidence": expected_evidence,
            "observed_evidence": observed_evidence,
            "tier_changed": tier_changed,
            "evidence_changed": evidence_changed,
        }
    )

    assert (result.tier, result.quant_tier) == (case.tier, case.quant_tier)
    assert all(isinstance(value, FlagOutcome) for value in observed_evidence.values())
    assert not evidence_changed


def test_live_bulk_audit_persists_exact_export_and_provenance(tmp_path: Path) -> None:
    """A live bulk run writes exact TSV, audit, file, and retrieval-time records."""
    accession_file = tmp_path / "accessions.txt"
    accession_file.write_text("PXD000001\nPXD075811\nMSV000079514\n", encoding="utf-8")
    database = tmp_path / "results.db"
    export = tmp_path / "bulk_results.tsv"

    started_at = datetime.now(UTC)
    result = CliRunner().invoke(
        main,
        [
            "bulk-audit",
            "--input",
            str(accession_file),
            "--db",
            str(database),
            "--format",
            "tsv",
            "--output",
            str(export),
            "--delay",
            "0",
        ],
    )
    finished_at = datetime.now(UTC)

    assert result.exit_code == 0, f"{result.output}\n{result.exception!r}"
    assert "Completed : 3" in result.output
    with export.open(encoding="utf-8", newline="") as handle:
        exported = {row["accession"]: row for row in csv.DictReader(handle, delimiter="\t")}
    assert {accession: (row["tier"], row["quant_tier"]) for accession, row in exported.items()} == {
        "PXD000001": ("Silver", "Partial"),
        "PXD075811": ("Platinum", "Partial"),
        "MSV000079514": ("Unverifiable", "Unverifiable"),
    }

    with closing(sqlite3.connect(database)) as connection:
        audit_rows = set(
            connection.execute("SELECT accession, tier, quant_tier, is_unverifiable FROM audit")
        )
        live_studies = list(
            connection.execute(
                "SELECT accession, fetched_at FROM study WHERE accession LIKE 'PXD%'"
            )
        )
        file_counts = dict(
            connection.execute("SELECT accession, COUNT(*) FROM study_files GROUP BY accession")
        )

    assert audit_rows == {
        ("PXD000001", "Silver", "Partial", 0),
        ("PXD075811", "Platinum", "Partial", 0),
        ("MSV000079514", "Unverifiable", "Unverifiable", 1),
    }
    assert {accession for accession, _ in live_studies} == {"PXD000001", "PXD075811"}
    retrieval_times = [datetime.fromisoformat(fetched_at) for _, fetched_at in live_studies]
    assert all(value.tzinfo is not None for value in retrieval_times)
    assert all(started_at <= value <= finished_at for value in retrieval_times)
    assert file_counts["PXD000001"] > 0
    assert file_counts["PXD075811"] > 0


def test_live_current_checksum_is_extracted_as_sha1() -> None:
    """Current PRIDE checksum fields survive extraction with a SHA-1 label."""
    files, _ = _fetch_live_audit("PXD004683")
    current_checksums = {
        value.strip()
        for file_data in files
        if isinstance(value := file_data.get("checksum"), str) and value.strip()
    }

    assert current_checksums
    rows = _extract_files_df("PXD004683", files)
    extracted_sha1 = set(
        rows.loc[rows["checksum_type"] == "SHA-1", "checksum"].dropna().astype(str)
    )
    assert current_checksums & extracted_sha1
