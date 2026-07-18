"""Shared test fixtures for the pxaudit test suite.

Fixtures defined here are auto-discovered by pytest and available to every
test module without explicit import.

Naming convention
-----------------
``pride_project_*``  : synthetic /projects API response dicts.
``pride_files_*``    : synthetic /files API response lists.

All payloads use the real PRIDE v3 JSON shape (CvParam dicts with ``value``
fields, ``publicFileLocations`` lists, etc.) so they exercise the same
extraction code paths as production data.
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# /projects payloads
# ---------------------------------------------------------------------------


@pytest.fixture()
def pride_project_complete_metadata() -> dict:
    """Project payload containing every required baseline metadata field."""
    return {
        "title": "Complete metadata study",
        "submissionDate": "2020-03-15",
        "keywords": ["proteomics", "phospho"],
        "organisms": [
            {
                "@type": "CvParam",
                "cvLabel": "NEWT",
                "name": "Homo sapiens",
                "accession": "NEWT:9606",
            }
        ],
        "instruments": [{"@type": "CvParam", "name": "Orbitrap Fusion"}],
    }


@pytest.fixture()
def pride_project_bronze() -> dict:
    """Organism present but no taxonomy accession → Bronze when result files exist."""
    return {
        "title": "Bronze study",
        "submissionDate": "2021-06-01",
        "keywords": ["proteomics"],
        "organisms": [{"@type": "CvParam", "name": "Homo sapiens"}],
        "instruments": [{"@type": "CvParam", "name": "Q Exactive"}],
    }


@pytest.fixture()
def pride_project_none_tier() -> dict:
    """Missing title → None tier regardless of files."""
    return {
        "organisms": [{"@type": "CvParam", "name": "Homo sapiens", "accession": "NEWT:9606"}],
        "instruments": [{"@type": "CvParam", "name": "Orbitrap"}],
    }


# ---------------------------------------------------------------------------
# /files payloads
# ---------------------------------------------------------------------------


@pytest.fixture()
def pride_files_psi_sdrf() -> list[dict]:
    """File payload containing mzIdentML, SDRF, and mzTab evidence."""
    return [
        {
            "fileName": "results.mzid",
            "fileCategory": {"@type": "CvParam", "value": "RESULT"},
            "fileSizeBytes": 1024,
            "publicFileLocations": [
                {"name": "FTP Protocol", "value": "ftp://ftp.ebi.ac.uk/results.mzid"}
            ],
            "fileChecksum": "abc123def456",
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
            "fileChecksum": "789ghi012jkl",
        },
    ]


@pytest.fixture()
def pride_files_silver() -> list[dict]:
    """Result file only, no SDRF → Silver tier when metadata is complete."""
    return [
        {
            "fileName": "results.mzid",
            "fileCategory": {"@type": "CvParam", "value": "RESULT"},
            "fileSizeBytes": 1024,
            "publicFileLocations": [],
        }
    ]


@pytest.fixture()
def pride_files_empty() -> list[dict]:
    """Empty file list → all file-level flags False."""
    return []
