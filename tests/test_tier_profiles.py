"""Offline multi-file profiles spanning every qualitative tier.

These payloads are realistic synthetic PRIDE shapes, not recordings of the
accessions used as routing identifiers. Recorded and current PRIDE behavior
belongs to the L2 and L3 suites.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from pxaudit.tier_engine import AuditResult, compute_audit


@dataclass(frozen=True)
class _Profile:
    """One synthetic project-and-files profile with its exact audit result."""

    accession: str
    project: dict[str, object]
    files: list[dict[str, object]]
    expected: AuditResult


def _project(
    *,
    instrument: str | None = "Orbitrap Fusion",
    submission_type: str = "COMPLETE",
    organism_part: str | None = None,
    pubmed_id: int | None = None,
) -> dict[str, object]:
    """Build the PRIDE project fields consumed by tier scoring."""
    return {
        "title": "Synthetic profile study",
        "organisms": [
            {
                "@type": "CvParam",
                "cvLabel": "NEWT",
                "name": "Homo sapiens",
                "accession": "NEWT:9606",
            }
        ],
        "instruments": [] if instrument is None else [{"@type": "CvParam", "name": instrument}],
        "submissionType": submission_type,
        "organismParts": [] if organism_part is None else [{"name": organism_part}],
        "references": [] if pubmed_id is None else [{"pubmedID": pubmed_id}],
        "quantificationMethods": [],
    }


def _file(filename: str, category: str) -> dict[str, object]:
    """Build the PRIDE file fields consumed by classification."""
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


def _expected(
    accession: str,
    tier: str,
    quant_tier: str,
    *,
    has_instrument: bool = True,
    has_result_files: bool = False,
    has_psi_results: bool = False,
    has_open_spectra: bool = False,
    has_organism_part: bool = False,
    has_publication: bool = False,
    has_tabular_quant: bool = False,
    has_quant_metadata: bool = False,
    has_sdrf: bool = False,
    has_mztab: bool = False,
) -> AuditResult:
    """Build an exact expected result with complete mandatory metadata by default."""
    return AuditResult(
        accession=accession,
        tier=tier,
        has_title=True,
        has_organism=True,
        has_organism_id=True,
        has_instrument=has_instrument,
        has_result_files=has_result_files,
        has_psi_results=has_psi_results,
        has_open_spectra=has_open_spectra,
        has_organism_part=has_organism_part,
        has_publication=has_publication,
        has_tabular_quant=has_tabular_quant,
        has_quant_metadata=has_quant_metadata,
        has_sdrf=has_sdrf,
        has_mztab=has_mztab,
        quant_tier=quant_tier,
    )


_PROFILES = (
    _Profile(
        "PXD900001",
        _project(instrument=None, organism_part="brain", pubmed_id=1001),
        [
            _file("spectra.mzML", "PEAK"),
            _file("results.mzid", "RESULT"),
            _file("study.sdrf.tsv", "EXPERIMENTAL DESIGN"),
        ],
        _expected(
            "PXD900001",
            "None",
            "Partial",
            has_instrument=False,
            has_result_files=True,
            has_psi_results=True,
            has_open_spectra=True,
            has_organism_part=True,
            has_publication=True,
            has_sdrf=True,
        ),
    ),
    _Profile(
        "PXD900002",
        _project(submission_type="PARTIAL"),
        [_file("run.raw", "RAW"), _file("README.txt", "OTHER")],
        _expected("PXD900002", "Raw", "No Quant"),
    ),
    _Profile(
        "PXD900003",
        _project(submission_type="PARTIAL"),
        [_file("spectra.mgf", "PEAK"), _file("evidence.txt", "OTHER")],
        _expected(
            "PXD900003",
            "Bronze",
            "No Quant",
            has_result_files=True,
            has_open_spectra=True,
        ),
    ),
    _Profile(
        "PXD900004",
        _project(organism_part="lung", pubmed_id=1004),
        [_file("spectra.mzML", "PEAK"), _file("results.mzid", "RESULT")],
        _expected(
            "PXD900004",
            "Silver",
            "Partial",
            has_result_files=True,
            has_psi_results=True,
            has_open_spectra=True,
            has_organism_part=True,
            has_publication=True,
        ),
    ),
    _Profile(
        "PXD900005",
        _project(pubmed_id=1005),
        [
            _file("spectra.mzML", "PEAK"),
            _file("results.mzid", "RESULT"),
            _file("study.sdrf.tsv", "OTHER"),
        ],
        _expected(
            "PXD900005",
            "Gold",
            "Partial",
            has_result_files=True,
            has_psi_results=True,
            has_open_spectra=True,
            has_publication=True,
            has_sdrf=True,
        ),
    ),
    _Profile(
        "PXD900006",
        _project(organism_part="liver"),
        [
            _file("spectra.mgf", "PEAK"),
            _file("results.mzidentml.gz", "RESULT"),
            _file("study.sdrf.tsv", "EXPERIMENTAL DESIGN"),
        ],
        _expected(
            "PXD900006",
            "Platinum",
            "Partial",
            has_result_files=True,
            has_psi_results=True,
            has_open_spectra=True,
            has_organism_part=True,
            has_sdrf=True,
        ),
    ),
    _Profile(
        "PXD900007",
        _project(organism_part="kidney", pubmed_id=1007),
        [
            _file("spectra.mzML", "PEAK"),
            _file("results.mztab.zip", "RESULT"),
            _file("study.sdrf.tsv", "OTHER"),
        ],
        _expected(
            "PXD900007",
            "Diamond",
            "Partial",
            has_result_files=True,
            has_psi_results=True,
            has_open_spectra=True,
            has_organism_part=True,
            has_publication=True,
            has_sdrf=True,
            has_mztab=True,
        ),
    ),
)


@pytest.mark.parametrize("profile", _PROFILES, ids=lambda profile: profile.expected.tier)
def test_synthetic_profile_produces_exact_audit_result(profile: _Profile) -> None:
    """Each realistic multi-file profile produces its complete expected audit row."""
    result = compute_audit(profile.accession, profile.project, profile.files)

    assert result == profile.expected


@pytest.mark.parametrize("profile", _PROFILES, ids=lambda profile: profile.expected.tier)
def test_profile_scoring_ignores_file_order_and_duplicates(profile: _Profile) -> None:
    """File ordering and repeated records leave every profile outcome unchanged."""
    reordered = list(reversed(profile.files))
    repeated = [*profile.files, profile.files[0]]

    assert compute_audit(profile.accession, profile.project, reordered) == profile.expected
    assert compute_audit(profile.accession, profile.project, repeated) == profile.expected
