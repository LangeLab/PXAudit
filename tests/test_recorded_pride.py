"""L2 contracts against reviewed, sanitized PRIDE response fixtures."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from pxaudit.cli import _extract_files_df, _extract_study
from pxaudit.tier_engine import compute_audit

pytestmark = pytest.mark.recorded

_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "pride" / "PXD000001.json"
_CHECKSUM_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "pride" / "PXD004683-file.json"


@pytest.fixture(scope="module")
def recorded_pxd000001() -> dict[str, Any]:
    """Load the reviewed PXD000001 API projection."""
    return json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))


def test_recorded_fixture_has_traceable_scope(recorded_pxd000001: dict[str, Any]) -> None:
    """Recorded evidence identifies its source, retrieval date, API, and projection scope."""
    metadata = recorded_pxd000001["fixture"]

    assert metadata["accession"] == "PXD000001"
    assert metadata["api_version"] == "PRIDE Archive REST API v3"
    assert metadata["retrieved_at"] == "2026-07-17"
    assert metadata["project_url"].startswith("https://www.ebi.ac.uk/pride/ws/archive/v3/")
    assert "projection" in metadata["scope"]


def test_recorded_payload_preserves_semantic_audit_contract(
    recorded_pxd000001: dict[str, Any],
) -> None:
    """The recorded payload retains extraction, file, and tier semantics without network use."""
    project = recorded_pxd000001["project"]
    files = recorded_pxd000001["files"]

    result = compute_audit("PXD000001", project, files)
    study = _extract_study("PXD000001", project, "2026-07-17T00:00:00+00:00")
    file_rows = _extract_files_df("PXD000001", files)

    assert result.tier == "Silver"
    assert result.quant_tier == "Partial"
    assert result.has_result_files is True
    assert result.has_psi_results is True
    assert result.has_sdrf is False
    assert result.has_open_spectra is True
    assert result.has_tabular_quant is False
    assert study["organism_id"] == "NEWT:554"
    assert study["instrument"] == "LTQ Orbitrap Velos"
    assert len(file_rows) == 8


def test_recorded_current_checksum_contract() -> None:
    """A current PRIDE checksum field is retained with its defensible SHA-1 label."""
    fixture = json.loads(_CHECKSUM_FIXTURE_PATH.read_text(encoding="utf-8"))

    assert fixture["fixture"]["retrieved_at"] == "2026-07-17"
    rows = _extract_files_df("PXD004683", [fixture["file"]])
    assert rows.loc[0, "checksum"] == "d7e9bc8469b477884f7c76c05d2ee87abca53393"
    assert rows.loc[0, "checksum_type"] == "SHA-1"
