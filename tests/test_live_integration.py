"""Live PRIDE API integration tests.

These tests make real network requests to the PRIDE Archive REST API (v3) and
verify that ``compute_audit()`` returns the expected ``tier`` and ``quant_tier``
for six well-known submissions whose file contents and project metadata have
been manually verified against the live API on 2026-03-21.

Run with:
    uv run pytest -m integration -v

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

    assert result.tier == expected_tier, (
        f"{accession}: tier — got {result.tier!r}, expected {expected_tier!r}\n"
        f"  has_result_files={result.has_result_files}, has_psi_results={result.has_psi_results},\n"
        f"  has_sdrf={result.has_sdrf}, has_open_spectra={result.has_open_spectra},\n"
        f"  has_organism_part={result.has_organism_part}, has_publication={result.has_publication}"
    )
    assert result.quant_tier == expected_quant_tier, (
        f"{accession}: quant_tier — got {result.quant_tier!r}, expected {expected_quant_tier!r}\n"
        f"  has_psi_results={result.has_psi_results}, "
        f"has_tabular_quant={result.has_tabular_quant},\n"
        f"  has_quant_metadata={result.has_quant_metadata}"
    )


# ---------------------------------------------------------------------------
# Non-PRIDE routing (no API call needed — covered here for completeness)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_live_non_pxd_accession_is_unverifiable() -> None:
    """Non-PXD accession returns Unverifiable/Unverifiable without hitting the API."""
    result = compute_audit("MSV000079514", {}, [])
    assert result.tier == "Unverifiable"
    assert result.quant_tier == "Unverifiable"
    assert result.is_unverifiable is True
    assert result.has_psi_results is False
    assert result.has_tabular_quant is False


# ---------------------------------------------------------------------------
# Spot-check: specific flag values confirmed from the live API
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_live_pxd000001_silver_has_psi_no_sdrf() -> None:
    """PXD000001 must have PSI results (mzid) but no SDRF — the Silver criteria."""
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
