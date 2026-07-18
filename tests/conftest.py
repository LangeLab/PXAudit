"""Shared test fixtures for the pxaudit test suite.

Fixtures defined here are auto-discovered by pytest and available to every
test module without explicit import.

Naming convention
-----------------
``pride_project_*``  : synthetic /projects API response dicts.
``pride_files_*``    : synthetic /files API response lists.

Payloads model the PRIDE v3 field structure used by PXAudit while keeping
their values synthetic and deterministic.
"""

from __future__ import annotations

import socket
from collections.abc import Generator
from pathlib import Path
from typing import NoReturn

import pytest


@pytest.fixture(autouse=True)
def _isolate_process_state(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[None, None, None]:
    """Isolate ambient paths, configuration, output mode, and network access."""
    root = tmp_path.parent / f"{tmp_path.name}-ambient"
    home = root / "home"
    work = root / "work"
    cache = root / "cache"
    home.mkdir(parents=True)
    work.mkdir()
    cache.mkdir()

    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    monkeypatch.setenv("XDG_CACHE_HOME", str(cache))
    monkeypatch.setenv("PXAUDIT_CONFIG", str(root / "missing.toml"))
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.chdir(work)

    from pxaudit import _output, config

    monkeypatch.setitem(config.DEFAULTS, "cache_dir", str(cache))
    _output.configure(quiet=False, verbose=False, no_color=False)

    if request.node.get_closest_marker("integration") is None:

        def block_network(*_args: object, **_kwargs: object) -> NoReturn:
            pytest.fail("offline tests must not open network connections")

        monkeypatch.setattr(socket.socket, "connect", block_network)
        monkeypatch.setattr(socket.socket, "connect_ex", block_network)
        monkeypatch.setattr(socket, "create_connection", block_network)

    yield
    _output.configure(quiet=False, verbose=False, no_color=False)


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
