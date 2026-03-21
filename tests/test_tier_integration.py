"""Integration tests for compute_audit() against realistic PRIDE submission profiles.

Each test uses a hand-crafted mock that mirrors the actual /projects and /files
payloads for a known accession, allowing the full compute_audit() logic to be
exercised end-to-end without any network access.

Test organisation
-----------------
1.  7-tier ladder parametrized — one known accession per tier
    (Raw, Bronze, Silver×2, Platinum×2)
2.  Tier-ladder sequencing — confirms the ladder is strictly sequential
    (publication + organism_part do NOT skip past Silver if SDRF is absent)
3.  Submission-type gate — PARTIAL vs. COMPLETE result-file recognition

Note: the "edge cases" listed in the C08 spec (MSV000001, pubmedID=0, SDRF
compressed, mzTab basename, etc.) are already covered exhaustively in
test_tier_engine.py sections 2, 5, 11, 12, 13.  They are deliberately omitted
here to avoid duplicate coverage.
"""

from __future__ import annotations

import pytest

from pxaudit.tier_engine import compute_audit

# ---------------------------------------------------------------------------
# Local helpers — minimal PRIDE v3 CvParam shapes
# ---------------------------------------------------------------------------

# Redefine locally rather than importing from test_tier_engine to keep test
# modules independent (test files are not packages; cross-importing them is
# fragile and not considered best practice).


def _mk_project(
    *,
    title: str = "Integration test study",
    organism_name: str = "Homo sapiens",
    organism_id: str = "NEWT:9606",
    instrument: str = "Orbitrap Fusion",
    submission_type: str = "COMPLETE",
    organism_parts: list | None = None,
    references: list | None = None,
    quant_methods: list | None = None,
) -> dict:
    """Build a minimal /projects API response dict."""
    return {
        "title": title,
        "organisms": [
            {
                "@type": "CvParam",
                "cvLabel": "NEWT",
                "name": organism_name,
                "accession": organism_id,
            }
        ],
        "instruments": [{"@type": "CvParam", "name": instrument}],
        "submissionType": submission_type,
        "organismParts": organism_parts or [],
        "references": references or [],
        "quantificationMethods": quant_methods or [],
    }


def _mk_file(filename: str, category: str = "OTHER") -> dict:
    """Build a minimal /files API response dict (real PRIDE CvParam shape)."""
    return {
        "fileName": filename,
        "fileCategory": {
            "@type": "CvParam",
            "cvLabel": "PRIDE",
            "value": category,
            "name": "",
        },
        "fileSizeBytes": 1024,
        "publicFileLocations": [],
    }


# ---------------------------------------------------------------------------
# 1. Parametrized integration scenarios
# ---------------------------------------------------------------------------

# Each tuple: (label, accession, project_data, files_data, expected_tier)
_INTEGRATION_CASES = [
    # -------------------------------------------------------------------
    # PXD057701 — PARTIAL submission, only RAW + OTHER files.
    # No RESULT/SEARCH/QUANT in the PARTIAL result gate → Raw.
    # -------------------------------------------------------------------
    (
        "PXD057701-Raw",
        "PXD057701",
        _mk_project(submission_type="PARTIAL"),
        [
            _mk_file("run1.raw", "RAW"),
            _mk_file("README.txt", "OTHER"),
        ],
        "Raw",
    ),
    # -------------------------------------------------------------------
    # PXD002244 — PARTIAL submission, MGF spectra + Mascot SEARCH file.
    # SEARCH is in the PARTIAL result gate → has_result_files=True.
    # No PSI-standard RESULT file → has_psi_results=False → Bronze.
    # -------------------------------------------------------------------
    (
        "PXD002244-Bronze",
        "PXD002244",
        _mk_project(submission_type="PARTIAL"),
        [
            _mk_file("run1.raw", "RAW"),
            _mk_file("spectra.mgf", "PEAK"),
            _mk_file("mascot_results.dat", "SEARCH"),
        ],
        "Bronze",
    ),
    # -------------------------------------------------------------------
    # PXD000001 — COMPLETE, full result suite but no SDRF → Silver.
    # -------------------------------------------------------------------
    (
        "PXD000001-Silver",
        "PXD000001",
        _mk_project(submission_type="COMPLETE"),
        [
            _mk_file("run1.raw", "RAW"),
            _mk_file("run1.mzML", "PEAK"),
            _mk_file("results.mzid", "RESULT"),
            _mk_file("results.dat", "SEARCH"),
            _mk_file("README.txt", "OTHER"),
        ],
        "Silver",
    ),
    # -------------------------------------------------------------------
    # Synthetic Silver scenario: PSI results + publication + organism_part
    # annotated, BUT no SDRF.  Confirms the ladder is strictly sequential:
    # publication and organism_part do NOT elevate tier past Silver when the
    # SDRF requirement is unmet.
    # Note: PXD000001 is re-used as the routing accession (it is genuinely
    # Silver in real data too, though for different reasons).  The mock
    # payload is what drives the assertion — not the live API.
    # -------------------------------------------------------------------
    (
        "Silver-no-sdrf-despite-pub-and-orgpart",
        "PXD000001",
        _mk_project(
            submission_type="COMPLETE",
            organism_parts=[{"name": "lung"}],
            references=[{"pubmedID": 27794522}],
        ),
        [
            _mk_file("run1.raw", "RAW"),
            _mk_file("run1.mzML", "PEAK"),
            _mk_file("results.mzid", "RESULT"),
            _mk_file("results.dat", "SEARCH"),
        ],
        "Silver",
    ),
    # -------------------------------------------------------------------
    # PXD004683 — COMPLETE, real submission profile.  SDRF via authoritative
    # EXPERIMENTAL DESIGN category, open spectra (MGF), PSI results (mzid.gz
    # in a RESULT-category file), publication, organism_part → Diamond.
    # -------------------------------------------------------------------
    (
        "PXD004683-Diamond",
        "PXD004683",
        _mk_project(
            submission_type="COMPLETE",
            organism_parts=[{"name": "lung"}],
            references=[{"pubmedID": 27794522}],
        ),
        [
            _mk_file("run1.raw", "RAW"),
            _mk_file("run1.mgf", "PEAK"),
            _mk_file("results.mzid.gz", "RESULT"),
            _mk_file("sdrf.tsv", "EXPERIMENTAL DESIGN"),
        ],
        "Diamond",
    ),
    # -------------------------------------------------------------------
    # PXD073444 — COMPLETE, SDRF + open spectra + organism_part annotated,
    # but pubmedID=0 (PRIDE sentinel for unpublished) → Platinum.
    # -------------------------------------------------------------------
    (
        "PXD073444-Platinum-pubmedID-zero",
        "PXD073444",
        _mk_project(
            submission_type="COMPLETE",
            organism_parts=[{"name": "cell culture"}],
            references=[{"pubmedID": 0}],  # PRIDE sentinel for unpublished
        ),
        [
            _mk_file("run1.raw", "RAW"),
            _mk_file("run1.mzML", "PEAK"),
            _mk_file("results.mzid", "RESULT"),
            _mk_file("sdrf_samples.tsv", "OTHER"),  # SDRF via fallback regex
            _mk_file("README.txt", "OTHER"),
        ],
        "Platinum",
    ),
    # -------------------------------------------------------------------
    # PXD075811 — COMPLETE, SDRF + open spectra + organism_part, no publication
    # at all (empty references list) → Platinum.
    # Note: organism_part is required to clear the Gold gate
    # (not has_open_spectra OR not has_organism_part).  Without it the tier
    # would stop at Gold.
    # -------------------------------------------------------------------
    (
        "PXD075811-Platinum-no-pub",
        "PXD075811",
        _mk_project(
            submission_type="COMPLETE",
            organism_parts=[{"name": "brain"}],
            references=[],  # no publication linked
        ),
        [
            _mk_file("run1.raw", "RAW"),
            _mk_file("run1.mzML", "PEAK"),
            _mk_file("results.mzid", "RESULT"),
            _mk_file("sdrf_samples.tsv", "OTHER"),
        ],
        "Platinum",
    ),
]


@pytest.mark.parametrize(
    "accession, project, files, expected_tier",
    [case[1:] for case in _INTEGRATION_CASES],
    ids=[case[0] for case in _INTEGRATION_CASES],
)
def test_known_tier_example(
    accession: str,
    project: dict,
    files: list[dict],
    expected_tier: str,
) -> None:
    """compute_audit() produces the expected tier for each known submission profile."""
    result = compute_audit(accession, project, files)
    assert result.tier == expected_tier, (
        f"{accession}: expected tier={expected_tier!r}, got {result.tier!r}\n"
        f"  has_result_files={result.has_result_files}, has_psi_results={result.has_psi_results},\n"
        f"  has_sdrf={result.has_sdrf}, has_open_spectra={result.has_open_spectra},\n"
        f"  has_organism_part={result.has_organism_part}, has_publication={result.has_publication}"
    )


# ---------------------------------------------------------------------------
# 2. Tier-ladder sequencing — explicit single-flag drop tests
# ---------------------------------------------------------------------------


def test_tier_ladder_publication_does_not_skip_sdrf_requirement() -> None:
    """A submission with publication but no SDRF must not exceed Silver."""
    project = _mk_project(
        submission_type="COMPLETE",
        references=[{"pubmedID": 99999}],
    )
    files = [
        _mk_file("run1.mzML", "PEAK"),
        _mk_file("results.mzid", "RESULT"),
        # No SDRF file
    ]
    result = compute_audit("PXD000001", project, files)
    assert result.tier == "Silver"
    assert result.has_publication is True
    assert result.has_sdrf is False


def test_tier_ladder_organism_part_does_not_skip_sdrf_requirement() -> None:
    """A submission with organism_part but no SDRF must not exceed Silver."""
    project = _mk_project(
        submission_type="COMPLETE",
        organism_parts=[{"name": "kidney"}],
    )
    files = [
        _mk_file("run1.mzML", "PEAK"),
        _mk_file("results.mzid", "RESULT"),
        # No SDRF file
    ]
    result = compute_audit("PXD000001", project, files)
    assert result.tier == "Silver"
    assert result.has_organism_part is True
    assert result.has_sdrf is False


def test_tier_ladder_sdrf_without_open_spectra_stops_at_gold() -> None:
    """SDRF present, PSI results present, but no open spectra → Gold (not Platinum)."""
    project = _mk_project(
        submission_type="COMPLETE",
        organism_parts=[{"name": "liver"}],
        references=[{"pubmedID": 12345}],
    )
    files = [
        _mk_file("run1.raw", "RAW"),  # RAW only, no PEAK file
        _mk_file("results.mzid", "RESULT"),
        _mk_file("sdrf_samples.tsv", "OTHER"),
    ]
    result = compute_audit("PXD000001", project, files)
    assert result.tier == "Gold"
    assert result.has_sdrf is True
    assert result.has_open_spectra is False
