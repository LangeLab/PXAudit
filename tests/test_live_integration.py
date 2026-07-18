"""Live PRIDE API integration tests.

These tests make real network requests to the PRIDE Archive REST API (v3) and
verify that ``compute_audit()`` returns the expected ``tier`` and ``quant_tier``
for six well-known submissions. When ``PXAUDIT_LIVE_RECORD`` is set, the run
writes a JSON record containing its date, API version, inventory, completeness,
and changes.

Run with:
    uv run pytest -m integration -v --no-cov

Excluded from the default test suite (``-m 'not integration'`` in addopts) to
avoid network dependency during CI and offline development.

Accession inventory
-------------------
PXD057701   PARTIAL, 1 070 files (RAW + OTHER only)          → Raw / No Quant
PXD002244   PARTIAL, 18 files (SEARCH + RAW + PEAK)          → Bronze / No Quant
PXD000001   COMPLETE, 8 files (RESULT+PEAK, no SDRF)         → Silver / Partial
PXD004683   COMPLETE, 289 files (RESULT+PEAK+SDRF+refs)      → Diamond / Partial
PXD073444   COMPLETE, 30 files (RESULT+PEAK+SDRF, pubmed=0)  → Platinum / Partial
PXD075811   COMPLETE, 14 files (RESULT+PEAK+SDRF, no pub)    → Platinum / Partial

Quant-tier notes
----------------
All verified accessions produce either "No Quant" or "Partial" because none
carry a QUANT_MATRIX / ID_LIST file (proteinGroups.txt, etc.).  The
Quant-Ready and Quant-Complete values are covered by unit tests in
``test_tier_engine.py`` section 14 using synthetic payloads.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pxaudit.pride_client import fetch_files, fetch_project
from pxaudit.tier_engine import compute_audit

# ---------------------------------------------------------------------------
# Parametrized tier + quant_tier table
# ---------------------------------------------------------------------------

_LIVE_CASES = [
    # (accession, expected_tier, expected_quant_tier)
    ("PXD057701", "Raw", "No Quant"),
    ("PXD002244", "Bronze", "No Quant"),
    ("PXD000001", "Silver", "Partial"),
    ("PXD004683", "Diamond", "Partial"),
    ("PXD073444", "Platinum", "Partial"),
    ("PXD075811", "Platinum", "Partial"),
]

_LIVE_OBSERVATIONS: list[dict[str, str | bool]] = []


@pytest.fixture(scope="module", autouse=True)
def _write_live_verification_record() -> Iterator[None]:
    """Write machine-readable live evidence when the caller requests it."""
    _LIVE_OBSERVATIONS.clear()
    yield

    destination = os.environ.get("PXAUDIT_LIVE_RECORD")
    if not destination:
        return
    observations = sorted(_LIVE_OBSERVATIONS, key=lambda item: str(item["accession"]))
    observed_accessions = [str(item["accession"]) for item in observations]
    missing_accessions = [case[0] for case in _LIVE_CASES if case[0] not in observed_accessions]
    record = {
        "run_date": datetime.now(UTC).date().isoformat(),
        "api_version": "PRIDE Archive REST API v3",
        "api_base_url": "https://www.ebi.ac.uk/pride/ws/archive/v3",
        "accession_inventory": [case[0] for case in _LIVE_CASES],
        "observed_accessions": observed_accessions,
        "missing_accessions": missing_accessions,
        "complete": not missing_accessions,
        "observations": observations,
        "tier_changes": [item for item in observations if item["changed"]],
    }
    record_path = Path(destination)
    record_path.parent.mkdir(parents=True, exist_ok=True)
    record_path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")


@pytest.mark.integration
@pytest.mark.parametrize(
    "accession, expected_tier, expected_quant_tier",
    _LIVE_CASES,
    ids=[c[0] for c in _LIVE_CASES],
)
def test_live_tier_and_quant_tier(
    accession: str,
    expected_tier: str,
    expected_quant_tier: str,
) -> None:
    """Tier and quant_tier must match manually-verified values for live PRIDE data."""
    project = fetch_project(accession)
    files = fetch_files(accession)
    result = compute_audit(accession, project, files)
    _LIVE_OBSERVATIONS.append(
        {
            "accession": accession,
            "expected_tier": expected_tier,
            "observed_tier": result.tier,
            "expected_quant_tier": expected_quant_tier,
            "observed_quant_tier": result.quant_tier,
            "changed": result.tier != expected_tier or result.quant_tier != expected_quant_tier,
        }
    )

    assert result.tier == expected_tier, (
        f"{accession}: tier : got {result.tier!r}, expected {expected_tier!r}\n"
        f"  has_result_files={result.has_result_files}, has_psi_results={result.has_psi_results},\n"
        f"  has_sdrf={result.has_sdrf}, has_open_spectra={result.has_open_spectra},\n"
        f"  has_organism_part={result.has_organism_part}, has_publication={result.has_publication}"
    )
    assert result.quant_tier == expected_quant_tier, (
        f"{accession}: quant_tier : got {result.quant_tier!r}, expected {expected_quant_tier!r}\n"
        f"  has_psi_results={result.has_psi_results}, "
        f"has_tabular_quant={result.has_tabular_quant},\n"
        f"  has_quant_metadata={result.has_quant_metadata}"
    )


# ---------------------------------------------------------------------------
# Spot-check: specific flag values confirmed from the live API
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_live_pxd000001_silver_has_psi_no_sdrf() -> None:
    """PXD000001 must have PSI results (mzid) but no SDRF : the Silver criteria."""
    project = fetch_project("PXD000001")
    files = fetch_files("PXD000001")
    result = compute_audit("PXD000001", project, files)
    assert result.has_psi_results is True
    assert result.has_sdrf is False
    assert result.tier == "Silver"


@pytest.mark.integration
def test_live_pxd004683_diamond_all_fair_flags() -> None:
    """PXD004683 must satisfy every FAIR tier flag that Diamond requires."""
    project = fetch_project("PXD004683")
    files = fetch_files("PXD004683")
    result = compute_audit("PXD004683", project, files)
    assert result.has_result_files is True
    assert result.has_psi_results is True
    assert result.has_sdrf is True
    assert result.has_open_spectra is True
    assert result.has_organism_part is True
    assert result.has_publication is True
    assert result.tier == "Diamond"


@pytest.mark.integration
def test_live_pxd057701_raw_no_result_files() -> None:
    """PXD057701 is a PARTIAL submission with no result/search files → Raw."""
    project = fetch_project("PXD057701")
    files = fetch_files("PXD057701")
    result = compute_audit("PXD057701", project, files)
    assert result.has_result_files is False
    assert result.tier == "Raw"
    assert result.quant_tier == "No Quant"


# ---------------------------------------------------------------------------
# Bulk-audit integration test
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_live_bulk_audit(tmp_path: Path) -> None:
    """bulk-audit 3 real accessions : verify SQLite and TSV output."""
    from click.testing import CliRunner

    from pxaudit.cli import main

    acc_file = tmp_path / "accessions.txt"
    acc_file.write_text("PXD000001\nPXD004683\nMSV000079514\n")
    db_path = tmp_path / "results.db"
    tsv_path = tmp_path / "bulk_results.tsv"

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "bulk-audit",
            "--input",
            str(acc_file),
            "--db",
            str(db_path),
            "--format",
            "tsv",
            "--output",
            str(tsv_path),
            "--delay",
            "0",
        ],
    )
    assert result.exit_code == 0, f"bulk-audit failed: {result.output}"
    assert "Completed : 3" in result.output
    assert tsv_path.exists()

    tsv_content = tsv_path.read_text()
    assert "PXD000001" in tsv_content
    assert "PXD004683" in tsv_content
    assert "MSV000079514" in tsv_content
    assert "Silver" in tsv_content
    assert "Diamond" in tsv_content
    assert "Unverifiable" in tsv_content

    # Verify SQLite database has correct rows.
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute("SELECT COUNT(*) FROM audit")
        assert cursor.fetchone()[0] == 3

        cursor = conn.execute("SELECT tier FROM audit WHERE accession = ?", ("PXD000001",))
        assert cursor.fetchone()[0] == "Silver"

        cursor = conn.execute("SELECT tier FROM audit WHERE accession = ?", ("PXD004683",))
        assert cursor.fetchone()[0] == "Diamond"

        cursor = conn.execute(
            "SELECT tier, is_unverifiable FROM audit WHERE accession = ?",
            ("MSV000079514",),
        )
        row = cursor.fetchone()
        assert row[0] == "Unverifiable"
        assert row[1] == 1
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Live checksum and fetched_at verification
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_live_checksum_and_fetched_at_present() -> None:
    """Live PRIDE data must have checksums and fetched_at populated."""
    from datetime import UTC, datetime

    from pxaudit.cli import _extract_files_df, _extract_study

    project = fetch_project("PXD004683")
    files = fetch_files("PXD004683")

    # fetched_at should be set by the caller; we just verify extraction works.
    fetched_at = datetime.now(UTC).isoformat()
    study = _extract_study("PXD004683", project, fetched_at)
    assert study["fetched_at"] == fetched_at

    # At least one file should have a checksum from the live API.
    df = _extract_files_df("PXD004683", files)
    assert len(df) > 0
    checksums = df["checksum"].dropna()
    assert len(checksums) > 0, "Expected at least one file with a checksum from live PRIDE"
    assert df["checksum_type"].dropna().iloc[0] == "SHA-1"
