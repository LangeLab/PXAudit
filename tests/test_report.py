"""Tests for pxaudit.report : HTML report generation.

Coverage target: 100% branch coverage on report.py.
"""

from __future__ import annotations

import importlib.metadata as importlib_metadata
import sys
import threading
from collections.abc import Mapping
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from click.testing import CliRunner

from pxaudit.cli import main
from pxaudit.db import get_or_create_db
from pxaudit.report import (
    generate_report,
)


def _make_study_row(
    acc: str,
    title: str = "Test study",
    organism: str = "Homo sapiens",
    instrument: str = "Orbitrap",
    submission_year: int = 2023,
) -> dict[str, str | int]:
    """Build a minimal study row dict for direct DB insertion."""
    return {
        "accession": acc,
        "title": title,
        "organism": organism,
        "organism_id": "NEWT:9606",
        "instrument": instrument,
        "submission_year": submission_year,
        "submission_type": "COMPLETE",
        "keywords": "[]",
        "repository": "PRIDE",
        "fetched_at": "2024-01-01T00:00:00+00:00",
    }


def _make_audit_row(
    acc: str,
    tier: str = "Gold",
    quant_tier: str = "No Quant",
    flags: Mapping[str, int | None] | None = None,
) -> dict[str, str | int | None]:
    """Build a minimal audit row dict for direct DB insertion."""
    defaults: dict[str, int | None] = {
        "has_title": 1,
        "has_organism": 1,
        "has_organism_id": 1,
        "has_instrument": 1,
        "has_result_files": 1,
        "has_psi_results": 0,
        "has_sdrf": 0,
        "has_open_spectra": 0,
        "has_organism_part": 0,
        "has_publication": 0,
        "has_tabular_quant": 0,
        "has_quant_metadata": 0,
        "has_mztab": 0,
        "files_fetch_failed": 0,
        "is_unverifiable": 0,
    }
    if flags:
        defaults.update(flags)
    return {
        "accession": acc,
        "tier": tier,
        "quant_tier": quant_tier,
        "tier_logic_version": "v2.1",
        **defaults,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def realistic_db(tmp_path: Path) -> Path:
    """Database with diverse tiers, quant tiers, organisms, and instruments."""
    db_path = tmp_path / "realistic.db"
    conn = get_or_create_db(db_path)
    from pxaudit.db import _AUDIT_COLS, _STUDY_COLS

    # (accession, tier, quant_tier, organism, instrument, year, extra_flags)
    scenarios = [
        # Qualitative tier coverage
        (
            "PXD000001",
            "Diamond",
            "Quant-Complete",
            "Homo sapiens",
            "Orbitrap",
            2023,
            {
                "has_psi_results": 1,
                "has_sdrf": 1,
                "has_open_spectra": 1,
                "has_organism_part": 1,
                "has_publication": 1,
                "has_tabular_quant": 1,
                "has_quant_metadata": 1,
            },
        ),
        (
            "PXD000002",
            "Diamond",
            "Quant-Complete",
            "Homo sapiens",
            "Orbitrap",
            2023,
            {
                "has_psi_results": 1,
                "has_sdrf": 1,
                "has_open_spectra": 1,
                "has_organism_part": 1,
                "has_publication": 1,
                "has_tabular_quant": 1,
                "has_quant_metadata": 1,
            },
        ),
        (
            "PXD000003",
            "Platinum",
            "Quant-Ready",
            "Homo sapiens",
            "Q Exactive",
            2022,
            {
                "has_psi_results": 1,
                "has_sdrf": 1,
                "has_open_spectra": 1,
                "has_organism_part": 1,
                "has_publication": 0,
                "has_tabular_quant": 1,
                "has_quant_metadata": 0,
            },
        ),
        (
            "PXD000004",
            "Platinum",
            "Quant-Ready",
            "Mus musculus",
            "Q Exactive",
            2022,
            {
                "has_psi_results": 1,
                "has_sdrf": 1,
                "has_open_spectra": 1,
                "has_organism_part": 1,
                "has_publication": 0,
                "has_tabular_quant": 1,
                "has_quant_metadata": 0,
            },
        ),
        (
            "PXD000005",
            "Gold",
            "Partial",
            "Homo sapiens",
            "Orbitrap",
            2021,
            {
                "has_psi_results": 1,
                "has_sdrf": 1,
                "has_open_spectra": 0,
                "has_organism_part": 0,
                "has_tabular_quant": 0,
            },
        ),
        (
            "PXD000006",
            "Gold",
            "Partial",
            "Mus musculus",
            "timsTOF",
            2021,
            {
                "has_psi_results": 1,
                "has_sdrf": 1,
                "has_open_spectra": 0,
                "has_organism_part": 0,
                "has_tabular_quant": 0,
            },
        ),
        (
            "PXD000007",
            "Gold",
            "No Quant",
            "Drosophila melanogaster",
            "Orbitrap",
            2020,
            {
                "has_psi_results": 1,
                "has_sdrf": 1,
                "has_open_spectra": 0,
                "has_organism_part": 0,
                "has_tabular_quant": 0,
            },
        ),
        (
            "PXD000008",
            "Silver",
            "No Quant",
            "Homo sapiens",
            "Q Exactive",
            2020,
            {"has_psi_results": 1, "has_sdrf": 0, "has_tabular_quant": 0},
        ),
        (
            "PXD000009",
            "Silver",
            "No Quant",
            "Mus musculus",
            "Orbitrap",
            2019,
            {"has_psi_results": 1, "has_sdrf": 0, "has_tabular_quant": 0},
        ),
        (
            "PXD000010",
            "Bronze",
            "No Quant",
            "Homo sapiens",
            "timsTOF",
            2019,
            {"has_psi_results": 0, "has_tabular_quant": 0},
        ),
        (
            "PXD000011",
            "Bronze",
            "Partial",
            "Drosophila melanogaster",
            "Q Exactive",
            2018,
            {"has_psi_results": 0, "has_tabular_quant": 1},
        ),
        (
            "PXD000012",
            "Raw",
            "No Quant",
            "Homo sapiens",
            "Orbitrap",
            2018,
            {"has_result_files": 0, "has_psi_results": 0, "has_tabular_quant": 0},
        ),
        (
            "PXD000013",
            "Raw",
            "No Quant",
            "Mus musculus",
            "Q Exactive",
            2017,
            {"has_result_files": 0, "has_psi_results": 0, "has_tabular_quant": 0},
        ),
        (
            "PXD000014",
            "None",
            "No Quant",
            "Homo sapiens",
            "Orbitrap",
            2017,
            {"has_title": 0, "has_psi_results": 0, "has_tabular_quant": 0},
        ),
        (
            "PXD000015",
            "None",
            "No Quant",
            "Mus musculus",
            "timsTOF",
            2016,
            {"has_organism": 0, "has_psi_results": 0, "has_tabular_quant": 0},
        ),
    ]

    for acc, tier, quant_tier, organism, instrument, year, extra_flags in scenarios:
        study = _make_study_row(
            acc,
            title=f"Study {acc}",
            organism=organism,
            instrument=instrument,
            submission_year=year,
        )
        audit = _make_audit_row(acc, tier=tier, quant_tier=quant_tier, flags=extra_flags)

        ph_s = ", ".join("?" for _ in _STUDY_COLS)
        cols_s = ", ".join(_STUDY_COLS)
        conn.execute(
            f"INSERT OR REPLACE INTO study ({cols_s}) VALUES ({ph_s})",
            tuple(study[c] for c in _STUDY_COLS),
        )
        ph_a = ", ".join("?" for _ in _AUDIT_COLS)
        cols_a = ", ".join(_AUDIT_COLS)
        conn.execute(
            f"INSERT OR REPLACE INTO audit ({cols_a}) VALUES ({ph_a})",
            tuple(audit[c] for c in _AUDIT_COLS),
        )
    conn.close()
    return db_path


@pytest.fixture()
def empty_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "empty.db"
    get_or_create_db(db_path).close()
    return db_path


@pytest.fixture()
def all_unverifiable_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "unver.db"
    conn = get_or_create_db(db_path)
    for i in range(3):
        conn.execute(
            "INSERT OR REPLACE INTO audit (accession, tier, quant_tier, is_unverifiable, "
            "has_title, has_organism, has_instrument, has_result_files, "
            "has_psi_results, has_open_spectra, has_organism_part, has_publication, "
            "has_tabular_quant, has_quant_metadata, has_sdrf, has_mztab, "
            "has_organism_id, files_fetch_failed, tier_logic_version) "
            "VALUES (?, 'Unverifiable', 'Unverifiable', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'v2.1')",
            (f"PXD{i:06d}",),
        )
    conn.close()
    return db_path


@pytest.fixture()
def null_flag_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "nullflag.db"
    conn = get_or_create_db(db_path)
    from pxaudit.db import _AUDIT_COLS, _STUDY_COLS

    for i, hr in [(1, 1), (2, 0), (3, None)]:
        acc = f"PXD{i:06d}"
        study = _make_study_row(acc)
        audit = _make_audit_row(acc, flags={"has_result_files": hr})
        ph_s = ", ".join("?" for _ in _STUDY_COLS)
        cols_s = ", ".join(_STUDY_COLS)
        conn.execute(
            f"INSERT OR REPLACE INTO study ({cols_s}) VALUES ({ph_s})",
            tuple(study[c] for c in _STUDY_COLS),
        )
        ph_a = ", ".join("?" for _ in _AUDIT_COLS)
        cols_a = ", ".join(_AUDIT_COLS)
        conn.execute(
            f"INSERT OR REPLACE INTO audit ({cols_a}) VALUES ({ph_a})",
            tuple(audit[c] for c in _AUDIT_COLS),
        )
    conn.close()
    return db_path


@pytest.fixture()
def xss_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "xss.db"
    conn = get_or_create_db(db_path)
    study = _make_study_row("PXD000001")
    study["title"] = "<script>alert('xss')</script>"
    audit = _make_audit_row("PXD000001")
    from pxaudit.db import _AUDIT_COLS, _STUDY_COLS

    ph_s = ", ".join("?" for _ in _STUDY_COLS)
    cols_s = ", ".join(_STUDY_COLS)
    conn.execute(
        f"INSERT OR REPLACE INTO study ({cols_s}) VALUES ({ph_s})",
        tuple(study[c] for c in _STUDY_COLS),
    )
    ph_a = ", ".join("?" for _ in _AUDIT_COLS)
    cols_a = ", ".join(_AUDIT_COLS)
    conn.execute(
        f"INSERT OR REPLACE INTO audit ({cols_a}) VALUES ({ph_a})",
        tuple(audit[c] for c in _AUDIT_COLS),
    )
    conn.close()
    return db_path


@pytest.fixture()
def corrupted_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "corrupt.db"
    db_path.write_text("not a sqlite database", encoding="utf-8")
    return db_path


@pytest.fixture()
def output_dir(tmp_path: Path) -> Path:
    return tmp_path / "report_out"


@pytest.fixture()
def many_organisms_db(tmp_path: Path) -> Path:
    """DB with 12 unique organisms to test cohort chart truncation (>10 rows)."""
    db_path = tmp_path / "many_orgs.db"
    conn = get_or_create_db(db_path)
    from pxaudit.db import _AUDIT_COLS, _STUDY_COLS

    organisms = [f"Organism_{i}" for i in range(12)]
    for i, org in enumerate(organisms):
        acc = f"PXD{i:06d}"
        study = _make_study_row(acc, organism=org)
        audit = _make_audit_row(acc, tier="Gold")
        ph_s = ", ".join("?" for _ in _STUDY_COLS)
        cols_s = ", ".join(_STUDY_COLS)
        conn.execute(
            f"INSERT OR REPLACE INTO study ({cols_s}) VALUES ({ph_s})",
            tuple(study[c] for c in _STUDY_COLS),
        )
        ph_a = ", ".join("?" for _ in _AUDIT_COLS)
        cols_a = ", ".join(_AUDIT_COLS)
        conn.execute(
            f"INSERT OR REPLACE INTO audit ({cols_a}) VALUES ({ph_a})",
            tuple(audit[c] for c in _AUDIT_COLS),
        )
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Query function tests
# ---------------------------------------------------------------------------


class TestQueryFunctions:
    def test_tier_distribution_counts(self, realistic_db: Path) -> None:
        from pxaudit.report import _query_tier_distribution

        conn = get_or_create_db(realistic_db)
        df = _query_tier_distribution(conn)
        total_in_db = int(conn.execute("SELECT COUNT(*) FROM audit").fetchone()[0])
        conn.close()
        assert int(df["count"].sum()) == total_in_db
        assert abs(df["percentage"].sum() - 100.0) < 0.5

    def test_tier_distribution_covers_multiple_tiers(self, realistic_db: Path) -> None:
        from pxaudit.report import _query_tier_distribution

        conn = get_or_create_db(realistic_db)
        df = _query_tier_distribution(conn)
        conn.close()
        present_tiers = set(df[df["count"] > 0]["tier"])
        assert "Diamond" in present_tiers
        assert "Gold" in present_tiers
        assert "Silver" in present_tiers
        assert "Bronze" in present_tiers
        assert "Raw" in present_tiers
        assert "None" in present_tiers

    def test_quant_tier_distribution(self, realistic_db: Path) -> None:
        from pxaudit.report import _query_quant_tier_distribution

        conn = get_or_create_db(realistic_db)
        df = _query_quant_tier_distribution(conn)
        conn.close()
        assert not df.empty
        assert abs(df["percentage"].sum() - 100.0) < 0.5

    def test_quant_tier_distribution_covers_multiple_tiers(self, realistic_db: Path) -> None:
        from pxaudit.report import _query_quant_tier_distribution

        conn = get_or_create_db(realistic_db)
        df = _query_quant_tier_distribution(conn)
        conn.close()
        present_tiers = set(df[df["count"] > 0]["quant_tier"])
        assert "Quant-Complete" in present_tiers
        assert "Quant-Ready" in present_tiers
        assert "Partial" in present_tiers
        assert "No Quant" in present_tiers

    def test_cohort_organism_data(self, realistic_db: Path) -> None:
        from pxaudit.report import _query_cohort_organism

        conn = get_or_create_db(realistic_db)
        df = _query_cohort_organism(conn)
        conn.close()
        organisms = set(df["organism"])
        assert "Homo sapiens" in organisms
        assert "Mus musculus" in organisms

    def test_cohort_instrument_data(self, realistic_db: Path) -> None:
        from pxaudit.report import _query_cohort_instrument

        conn = get_or_create_db(realistic_db)
        df = _query_cohort_instrument(conn)
        conn.close()
        instruments = set(df["instrument"])
        assert "Orbitrap" in instruments
        assert "Q Exactive" in instruments

    def test_metadata_gaps_populated(self, realistic_db: Path) -> None:
        from pxaudit.report import _query_metadata_gaps

        conn = get_or_create_db(realistic_db)
        gaps = _query_metadata_gaps(conn)
        conn.close()
        assert len(gaps) > 0
        assert all("field" in g and "missing" in g and "severity" in g for g in gaps)

    def test_all_accessions_sorted(self, realistic_db: Path) -> None:
        from pxaudit.report import _TIER_ORDER, _query_all_accessions

        conn = get_or_create_db(realistic_db)
        rows = _query_all_accessions(conn)
        conn.close()
        assert len(rows) > 0
        prev_rank = -1
        for r in rows:
            rank = _TIER_ORDER.index(r["tier"]) if r["tier"] in _TIER_ORDER else 99
            assert rank >= prev_rank
            prev_rank = rank

    def test_all_accessions_have_quant_tier(self, realistic_db: Path) -> None:
        from pxaudit.report import _query_all_accessions

        conn = get_or_create_db(realistic_db)
        rows = _query_all_accessions(conn)
        conn.close()
        quant_tiers = {r["quant_tier"] for r in rows}
        assert "Quant-Complete" in quant_tiers
        assert "Quant-Ready" in quant_tiers
        assert "Partial" in quant_tiers
        assert "No Quant" in quant_tiers


class TestEmptyDataFrames:
    def test_empty_tier_dist(self, tmp_path: Path) -> None:
        from pxaudit.report import _query_tier_distribution

        db_path = tmp_path / "empty_audit.db"
        get_or_create_db(db_path).close()
        conn = get_or_create_db(db_path)
        df = _query_tier_distribution(conn)
        conn.close()
        # Returns full tier list with zero counts, not empty.
        assert not df.empty
        assert int(df["count"].sum()) == 0

    def test_empty_quant_dist(self, tmp_path: Path) -> None:
        from pxaudit.report import _query_quant_tier_distribution

        db_path = tmp_path / "noquant.db"
        conn = get_or_create_db(db_path)
        conn.execute(
            "INSERT INTO audit (accession, tier, quant_tier, is_unverifiable) VALUES ('PXD000001', 'Gold', NULL, 0)"
        )
        conn.close()
        conn = get_or_create_db(db_path)
        df = _query_quant_tier_distribution(conn)
        conn.close()
        # Returns full quant tier list with zero counts.
        assert not df.empty
        assert int(df["count"].sum()) == 0

    def test_empty_tier_chart(self) -> None:
        from pxaudit.report import _render_tier_chart

        out = _render_tier_chart(pd.DataFrame(columns=["tier", "count", "percentage"]))
        assert "No data" in out

    def test_empty_quant_chart(self) -> None:
        from pxaudit.report import _render_quant_tier_chart

        out = _render_quant_tier_chart(pd.DataFrame(columns=["quant_tier", "count", "percentage"]))
        assert "No data" in out


# ---------------------------------------------------------------------------
# Edge case tests
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_database_error(self, empty_db: Path, output_dir: Path) -> None:
        with pytest.raises(ValueError, match="no audited accessions"):
            generate_report(empty_db, output_dir, "X")

    def test_all_unverifiable(self, all_unverifiable_db: Path, output_dir: Path) -> None:
        out = generate_report(all_unverifiable_db, output_dir, "Unver")
        html = out.read_text(encoding="utf-8")
        assert "Unverifiable" in html

    def test_xss_escaped(self, xss_db: Path, output_dir: Path) -> None:
        out = generate_report(xss_db, output_dir, "X")
        html = out.read_text(encoding="utf-8")
        assert "<script>alert" not in html
        assert "&lt;script&gt;" in html

    def test_null_flags(self, null_flag_db: Path, output_dir: Path) -> None:
        out = generate_report(null_flag_db, output_dir, "Null")
        html = out.read_text(encoding="utf-8")
        assert "?" in html
        assert "+" in html
        assert "-" in html

    def test_direct_filenotfound(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="database not found"):
            generate_report(tmp_path / "nope.db", tmp_path, "X")

    @pytest.mark.skipif(sys.platform == "win32", reason="chmod has no effect on NTFS")
    def test_permission_error_mkdir(self, realistic_db: Path, tmp_path: Path) -> None:
        readonly = tmp_path / "ro"
        readonly.mkdir()
        readonly.chmod(0o555)
        try:
            with pytest.raises(PermissionError, match="cannot create"):
                generate_report(realistic_db, readonly / "sub", "X")
        finally:
            readonly.chmod(0o755)

    @pytest.mark.skipif(sys.platform == "win32", reason="chmod has no effect on NTFS")
    def test_permission_error_write(self, realistic_db: Path, tmp_path: Path) -> None:
        """PermissionError when report.html can't be written."""
        readonly = tmp_path / "ro2"
        readonly.mkdir()
        readonly.chmod(0o555)
        try:
            with pytest.raises(PermissionError):
                generate_report(realistic_db, readonly, "X")
        finally:
            readonly.chmod(0o755)

    def test_many_organisms_truncates_cohort_chart(
        self, many_organisms_db: Path, output_dir: Path
    ) -> None:
        from pxaudit.report import generate_report

        path = generate_report(many_organisms_db, output_dir, "X")
        assert path.exists()
        assert "Cohort Analysis" in path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI integration tests
# ---------------------------------------------------------------------------


class TestCliIntegration:
    def _runner(self) -> CliRunner:
        return CliRunner()

    def test_nonexistent_db(self, tmp_path: Path) -> None:
        r = self._runner().invoke(
            main, ["report", "--db", str(tmp_path / "nope.db"), "--output", str(tmp_path)]
        )
        assert r.exit_code == 2
        assert "database not found" in r.output
        assert list(tmp_path.iterdir()) == []

    def test_empty_db(self, empty_db: Path, tmp_path: Path) -> None:
        r = self._runner().invoke(
            main, ["report", "--db", str(empty_db), "--output", str(tmp_path / "out")]
        )
        assert r.exit_code == 1
        assert "no audited accessions" in r.output

    def test_output_dir_created(self, realistic_db: Path, tmp_path: Path) -> None:
        d = tmp_path / "new" / "nested"
        r = self._runner().invoke(main, ["report", "--db", str(realistic_db), "--output", str(d)])
        assert r.exit_code == 0
        assert (d / "report.html").exists()

    def test_overwrite_guard(self, realistic_db: Path, output_dir: Path) -> None:
        output_dir.mkdir()
        r = self._runner().invoke(
            main, ["report", "--db", str(realistic_db), "--output", str(output_dir)]
        )
        assert r.exit_code == 2
        assert "already exists" in r.output

    def test_overwrite_flag(self, realistic_db: Path, output_dir: Path) -> None:
        output_dir.mkdir()
        r = self._runner().invoke(
            main, ["report", "--db", str(realistic_db), "--output", str(output_dir), "--overwrite"]
        )
        assert r.exit_code == 0
        assert (output_dir / "report.html").exists()

    def test_corrupted_db(self, corrupted_db: Path, tmp_path: Path) -> None:
        r = self._runner().invoke(
            main, ["report", "--db", str(corrupted_db), "--output", str(tmp_path / "out")]
        )
        assert r.exit_code == 2
        assert "not a database" in r.output

    @pytest.mark.skipif(sys.platform == "win32", reason="chmod has no effect on NTFS")
    def test_permission_denied_dir(self, realistic_db: Path, tmp_path: Path) -> None:
        readonly = tmp_path / "ro_cli"
        readonly.mkdir()
        readonly.chmod(0o555)
        try:
            r = self._runner().invoke(
                main, ["report", "--db", str(realistic_db), "--output", str(readonly / "sub")]
            )
            assert r.exit_code == 2
            assert "Permission" in r.output
        finally:
            readonly.chmod(0o755)

    def test_missing_jinja2(self, realistic_db: Path, output_dir: Path) -> None:
        with (
            patch.dict(sys.modules, {"jinja2": None}),
            pytest.raises(ImportError, match="jinja2 is required"),
        ):
            generate_report(realistic_db, output_dir, "X")

    def test_missing_matplotlib(self, realistic_db: Path, output_dir: Path) -> None:
        with (
            patch.dict(sys.modules, {"matplotlib": None}),
            pytest.raises(ImportError, match="matplotlib is required"),
        ):
            generate_report(realistic_db, output_dir, "X")

    def test_cli_import_error(self, realistic_db: Path, tmp_path: Path) -> None:
        """CLI reports ImportError from generate_report."""
        with patch("pxaudit.report.generate_report", side_effect=ImportError("jinja2 is required")):
            r = CliRunner().invoke(
                main, ["report", "--db", str(realistic_db), "--output", str(tmp_path / "ie")]
            )
        assert r.exit_code == 1
        assert "jinja2 is required" in r.output

    def test_cli_filenotfound_error(self, realistic_db: Path, tmp_path: Path) -> None:
        """CLI reports FileNotFoundError from generate_report."""

        def fake(*a: object, **kw: object) -> None:
            raise FileNotFoundError("db vanished")

        with patch("pxaudit.report.generate_report", fake):
            r = CliRunner().invoke(
                main, ["report", "--db", str(realistic_db), "--output", str(tmp_path / "fnf")]
            )
        assert r.exit_code == 2
        assert "db vanished" in r.output


# ---------------------------------------------------------------------------
# HTML output tests
# ---------------------------------------------------------------------------


class TestHtmlOutput:
    def test_all_sections_present(self, realistic_db: Path, output_dir: Path) -> None:
        out = generate_report(realistic_db, output_dir, "Full")
        html = out.read_text(encoding="utf-8")
        assert "Quality Distribution" in html
        assert "Metadata Completeness" in html
        assert "Cohort Analysis" in html
        assert "Tier Reference" in html
        assert "All Accessions" in html

    def test_charts_embedded(self, realistic_db: Path, output_dir: Path) -> None:
        out = generate_report(realistic_db, output_dir, "C")
        html = out.read_text(encoding="utf-8")
        count = html.count("data:image/png;base64,")
        assert count >= 2

    def test_metadata_header(self, realistic_db: Path, output_dir: Path) -> None:
        out = generate_report(realistic_db, output_dir, "M")
        html = out.read_text(encoding="utf-8")
        assert "PXAudit version" in html
        assert str(realistic_db) in html

    def test_flag_badges(self, realistic_db: Path, output_dir: Path) -> None:
        out = generate_report(realistic_db, output_dir, "F")
        html = out.read_text(encoding="utf-8")
        assert "badge-ok" in html
        assert "badge-missing" in html
        assert "badge-unknown" in html

    def test_custom_title(self, realistic_db: Path, output_dir: Path) -> None:
        out = generate_report(realistic_db, output_dir, "Custom Title Here")
        html = out.read_text(encoding="utf-8")
        assert "<title>Custom Title Here</title>" in html

    def test_summary_cards(self, realistic_db: Path, output_dir: Path) -> None:
        out = generate_report(realistic_db, output_dir, "S")
        html = out.read_text(encoding="utf-8")
        assert "Total" in html
        assert "Verifiable" in html
        assert "Unverifiable" in html

    def test_no_external_assets(self, realistic_db: Path, output_dir: Path) -> None:
        out = generate_report(realistic_db, output_dir, "SA")
        html = out.read_text(encoding="utf-8")
        assert 'src="http' not in html
        assert 'href="http' not in html

    def test_quant_tier_styled(self, realistic_db: Path, output_dir: Path) -> None:
        out = generate_report(realistic_db, output_dir, "QT")
        html = out.read_text(encoding="utf-8")
        assert "tier-Partial" in html or "tier-Quant" in html

    def test_tier_classes_in_table(self, realistic_db: Path, output_dir: Path) -> None:
        out = generate_report(realistic_db, output_dir, "TC")
        html = out.read_text(encoding="utf-8")
        assert "tier-Diamond" in html
        assert "tier-Gold" in html
        assert "tier-Silver" in html
        assert "tier-Bronze" in html
        assert "tier-Raw" in html
        assert "tier-None" in html
        assert "tier-Quant-Complete" in html
        assert "tier-Quant-Ready" in html
        assert "tier-No-Quant" in html

    def test_cohort_charts_present(self, realistic_db: Path, output_dir: Path) -> None:
        out = generate_report(realistic_db, output_dir, "CC")
        html = out.read_text(encoding="utf-8")
        assert "By Organism" in html
        assert "By Instrument" in html

    def test_title_tooltip(self, realistic_db: Path, output_dir: Path) -> None:
        out = generate_report(realistic_db, output_dir, "TT")
        html = out.read_text(encoding="utf-8")
        assert 'title="' in html

    def test_tier_legend(self, realistic_db: Path, output_dir: Path) -> None:
        out = generate_report(realistic_db, output_dir, "TL")
        html = out.read_text(encoding="utf-8")
        assert "Tier Reference" in html
        assert "FAIR Ladder" in html

    def test_collapsible_table(self, realistic_db: Path, output_dir: Path) -> None:
        out = generate_report(realistic_db, output_dir, "CT")
        html = out.read_text(encoding="utf-8")
        assert "<details" in html
        assert "<summary" in html

    def test_concurrent_calls(self, realistic_db: Path, tmp_path: Path) -> None:
        errors: list[str] = []

        def run(out: str) -> None:
            try:
                generate_report(realistic_db, Path(out), "C")
            except Exception as e:
                errors.append(str(e))

        t1 = threading.Thread(target=run, args=(str(tmp_path / "c1"),))
        t2 = threading.Thread(target=run, args=(str(tmp_path / "c2"),))
        t1.start()
        t2.start()
        t1.join(timeout=30)
        t2.join(timeout=30)
        assert not errors
        assert (tmp_path / "c1" / "report.html").exists()
        assert (tmp_path / "c2" / "report.html").exists()


class TestVersionFallback:
    def test_version_unknown(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import pxaudit.report as report_mod

        def _raise(_name: str) -> str:
            raise importlib_metadata.PackageNotFoundError(_name)

        monkeypatch.setattr(report_mod.importlib_metadata, "version", _raise)
        assert report_mod._get_version() == "unknown"
