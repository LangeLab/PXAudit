"""L2 contracts against reviewed, sanitized PRIDE response fixtures."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pytest

from pxaudit.cli import _extract_files_df, _extract_study
from pxaudit.file_classifier import FileClass, FileTypeClassifier
from pxaudit.tier_engine import FlagOutcome, compute_audit

pytestmark = pytest.mark.recorded

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "pride"
_FIXTURE_PATHS = {
    "PXD000001": _FIXTURE_DIR / "PXD000001.json",
    "PXD004683": _FIXTURE_DIR / "PXD004683-file.json",
}


@pytest.fixture(scope="module")
def recorded_payloads() -> dict[str, dict[str, Any]]:
    """Load the reviewed PRIDE projections once for the module."""
    return {
        accession: json.loads(path.read_text(encoding="utf-8"))
        for accession, path in _FIXTURE_PATHS.items()
    }


@pytest.mark.parametrize(
    ("accession", "expected_urls"),
    [
        (
            "PXD000001",
            {
                "project_url": "https://www.ebi.ac.uk/pride/ws/archive/v3/projects/PXD000001",
                "files_url": "https://www.ebi.ac.uk/pride/ws/archive/v3/projects/PXD000001/files?page=0&pageSize=100&sortDirection=DESC&sortCondition=id",
            },
        ),
        (
            "PXD004683",
            {"files_url": "https://www.ebi.ac.uk/pride/ws/archive/v3/projects/PXD004683/files"},
        ),
    ],
)
def test_recorded_fixture_has_traceable_scope(
    recorded_payloads: dict[str, dict[str, Any]],
    accession: str,
    expected_urls: dict[str, str],
) -> None:
    """Every recording identifies its source, date, API, and projected scope."""
    metadata = recorded_payloads[accession]["fixture"]

    assert metadata["accession"] == accession
    assert metadata["api_version"] == "PRIDE Archive REST API v3"
    assert metadata["retrieved_at"] == "2026-07-17"
    assert metadata["scope"].startswith("Reviewed projection")
    assert {field: metadata[field] for field in expected_urls} == expected_urls


def test_recorded_project_maps_to_exact_study_row(
    recorded_payloads: dict[str, dict[str, Any]],
) -> None:
    """The reviewed project shape preserves every persisted study value."""
    project = recorded_payloads["PXD000001"]["project"]

    study = _extract_study("PXD000001", project, "2026-07-17T00:00:00+00:00")

    assert project["accession"] == "PXD000001"
    assert study == {
        "accession": "PXD000001",
        "title": "TMT spikes - Using R and Bioconductor for proteomics data analysis",
        "organism": "Erwinia carotovora",
        "organism_id": "NEWT:554",
        "instrument": "LTQ Orbitrap Velos",
        "submission_year": 2012,
        "submission_type": "COMPLETE",
        "keywords": "Spikes, Tmt, Eriwinia",
        "repository": "PRIDE",
        "fetched_at": "2026-07-17T00:00:00+00:00",
    }


def test_recorded_payload_preserves_exact_audit_result(
    recorded_payloads: dict[str, dict[str, Any]],
) -> None:
    """The reviewed project and files retain every scientific audit outcome."""
    payload = recorded_payloads["PXD000001"]

    result = compute_audit("PXD000001", payload["project"], payload["files"])

    assert asdict(result) == {
        "accession": "PXD000001",
        "tier": "Silver",
        "has_title": FlagOutcome.PASSED,
        "has_organism": FlagOutcome.PASSED,
        "has_organism_id": FlagOutcome.PASSED,
        "has_instrument": FlagOutcome.PASSED,
        "has_result_files": FlagOutcome.PASSED,
        "has_psi_results": FlagOutcome.PASSED,
        "has_open_spectra": FlagOutcome.PASSED,
        "has_organism_part": FlagOutcome.FAILED,
        "has_publication": FlagOutcome.PASSED,
        "has_tabular_quant": FlagOutcome.FAILED,
        "has_quant_metadata": FlagOutcome.FAILED,
        "has_sdrf": FlagOutcome.FAILED,
        "has_mztab": FlagOutcome.PASSED,
        "files_fetch_failed": False,
        "is_unverifiable": False,
        "ambiguity_count": 0,
        "tier_logic_version": "v3.0",
        "quant_tier": "Partial",
    }


def test_recorded_files_override_inconsistent_pride_categories(
    recorded_payloads: dict[str, dict[str, Any]],
) -> None:
    """Recorded filenames retain semantic classes when PRIDE categories are misleading."""
    files = recorded_payloads["PXD000001"]["files"]
    classifier = FileTypeClassifier()

    classes = [
        classifier.classify(file["fileName"], file["fileCategory"]["value"]) for file in files
    ]

    assert classes == [
        FileClass.RESULT,
        FileClass.PEAK,
        FileClass.PEAK,
        FileClass.FASTA,
        FileClass.RAW,
        FileClass.SEARCH,
        FileClass.SEARCH,
        FileClass.SEARCH,
    ]


def test_recorded_files_preserve_storage_projection(
    recorded_payloads: dict[str, dict[str, Any]],
) -> None:
    """Recorded file fields and absent optional metadata map without loss or invention."""
    files = recorded_payloads["PXD000001"]["files"]

    rows = _extract_files_df("PXD000001", files)

    assert list(rows.columns) == [
        "accession",
        "file_name",
        "file_category",
        "file_extension",
        "ftp_location",
        "file_size",
        "checksum",
        "checksum_type",
    ]
    assert rows["accession"].tolist() == ["PXD000001"] * len(files)
    assert rows["file_name"].tolist() == [file["fileName"] for file in files]
    assert rows["file_category"].tolist() == [file["fileCategory"]["value"] for file in files]
    assert rows["file_size"].tolist() == [file["fileSizeBytes"] for file in files]
    assert rows["file_extension"].tolist() == [
        ".gz",
        ".gz",
        ".mzXML",
        ".fasta",
        ".raw",
        ".txt",
        ".gz",
        ".dat",
    ]
    assert rows[["ftp_location", "checksum", "checksum_type"]].isna().all().all()


def test_recorded_current_checksum_preserves_complete_file_row(
    recorded_payloads: dict[str, dict[str, Any]],
) -> None:
    """A current checksum payload retains location, size, hash, and defensible algorithm."""
    file_data = recorded_payloads["PXD004683"]["file"]

    rows = _extract_files_df("PXD004683", [file_data])

    assert rows.to_dict(orient="records") == [
        {
            "accession": "PXD004683",
            "file_name": file_data["fileName"],
            "file_category": "PEAK",
            "file_extension": ".MGF",
            "ftp_location": file_data["publicFileLocations"][0]["value"],
            "file_size": 5907753,
            "checksum": "d7e9bc8469b477884f7c76c05d2ee87abca53393",
            "checksum_type": "SHA-1",
        }
    ]
