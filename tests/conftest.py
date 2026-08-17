"""Isolated process state and synthetic PRIDE payloads shared by the test suite."""

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
    matplotlib = root / "matplotlib"
    home.mkdir(parents=True)
    work.mkdir()
    cache.mkdir()

    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    monkeypatch.setenv("XDG_CACHE_HOME", str(cache))
    monkeypatch.setenv("MPLCONFIGDIR", str(matplotlib))
    monkeypatch.setenv("PXAUDIT_CONFIG", str(root / "missing.toml"))
    for variable in ("COLUMNS", "LINES", "NO_COLOR"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.chdir(work)

    from pxaudit import _output, config

    monkeypatch.setitem(config.DEFAULTS, "cache_dir", str(cache))
    _output.configure(quiet=False, verbose=False, no_color=False)

    if request.node.get_closest_marker("integration") is None:

        def block_network(*_args: object, **_kwargs: object) -> NoReturn:
            pytest.fail("offline tests must not open network connections")

        for attribute in ("connect", "connect_ex", "sendto"):
            monkeypatch.setattr(socket.socket, attribute, block_network)
        for attribute in (
            "create_connection",
            "getaddrinfo",
            "gethostbyname",
            "gethostbyname_ex",
        ):
            monkeypatch.setattr(socket, attribute, block_network)

    yield
    _output.configure(quiet=False, verbose=False, no_color=False)


@pytest.fixture()
def pride_project_complete_metadata() -> dict[str, object]:
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
def pride_files_psi_sdrf() -> list[dict[str, object]]:
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
