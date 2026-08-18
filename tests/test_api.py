"""Public Python API contracts."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

import pxaudit.api as api
from pxaudit import (
    AuditResult,
    FileClass,
    FlagOutcome,
    audit_accessions,
    check_accession,
    compute_audit,
)


def test_package_exports_public_audit_contract() -> None:
    """The package exports the documented scoring and orchestration symbols."""
    assert AuditResult is api.AuditResult
    assert FileClass is api.FileClass
    assert FlagOutcome is api.FlagOutcome
    assert compute_audit is api.compute_audit
    assert check_accession is api.check_accession
    assert audit_accessions is api.audit_accessions


def test_check_accession_forwards_options_and_returns_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The single-accession helper delegates persistence and returns its audit result."""
    expected = MagicMock()
    audit = MagicMock(return_value=MagicMock(result=expected))
    monkeypatch.setattr("pxaudit.cli._audit_single", audit)

    result = check_accession(
        "PXD000001",
        db_path=tmp_path / "audits.db",
        no_cache=True,
        refresh=True,
        cache_dir=tmp_path / "cache",
        cache_ttl_seconds=12.5,
        request_delay=0,
    )

    assert result is expected
    audit.assert_called_once_with(
        "PXD000001",
        str(tmp_path / "audits.db"),
        no_cache=True,
        refresh=True,
        cache_dir=tmp_path / "cache",
        cache_ttl_seconds=12.5,
        request_delay=0,
    )


def test_audit_accessions_preserves_input_order(monkeypatch: pytest.MonkeyPatch) -> None:
    """The bulk helper returns sequential results in accession order."""
    results = {accession: MagicMock(name=accession) for accession in ("PXD000002", "PXD000001")}
    check = MagicMock(side_effect=lambda accession, **_: results[accession])
    monkeypatch.setattr(api, "check_accession", check)

    actual = audit_accessions(
        (accession for accession in ("PXD000002", "PXD000001")),
        db_path="audits.db",
        request_delay=0,
    )

    assert actual == [results["PXD000002"], results["PXD000001"]]
    assert [call.args[0] for call in check.call_args_list] == ["PXD000002", "PXD000001"]
