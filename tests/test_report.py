"""Database, rendering, filesystem, and CLI contracts for HTML reports."""

from __future__ import annotations

import importlib.metadata as importlib_metadata
import logging
import sqlite3
import sys
import threading
from collections.abc import Callable, Mapping
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from click.testing import CliRunner

from pxaudit import report as report_mod
from pxaudit.cli import main
from pxaudit.db import get_or_create_db, insert_audit, insert_study
from pxaudit.report import generate_report


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


def _insert_record(
    conn: sqlite3.Connection,
    study: dict[str, str | int],
    audit: dict[str, str | int | None],
) -> None:
    """Insert one study and its audit through the production persistence API."""
    insert_study(conn, study)
    insert_audit(conn, audit)


@pytest.fixture()
def realistic_db(tmp_path: Path) -> Path:
    """Database with diverse tiers, quant tiers, organisms, and instruments."""
    db_path = tmp_path / "realistic.db"
    conn = get_or_create_db(db_path)

    scenarios = [
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
            "Partial",
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
            "Partial",
            "Homo sapiens",
            "Q Exactive",
            2020,
            {"has_psi_results": 1, "has_sdrf": 0, "has_tabular_quant": 0},
        ),
        (
            "PXD000009",
            "Silver",
            "Partial",
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
        if extra_flags.get("has_title") == 0:
            study["title"] = ""
        if extra_flags.get("has_organism") == 0:
            study["organism"] = ""
        audit = _make_audit_row(acc, tier=tier, quant_tier=quant_tier, flags=extra_flags)
        _insert_record(conn, study, audit)
    conn.close()
    return db_path


@pytest.fixture()
def empty_db(tmp_path: Path) -> Path:
    """Return a schema-valid database without audit rows."""
    db_path = tmp_path / "empty.db"
    get_or_create_db(db_path).close()
    return db_path


@pytest.fixture()
def all_unverifiable_db(tmp_path: Path) -> Path:
    """Return a database containing only unverifiable audit rows."""
    db_path = tmp_path / "unver.db"
    conn = get_or_create_db(db_path)
    for accession in ("MSV000000001", "JPST000001", "IPX000001"):
        audit = _make_audit_row(
            accession,
            tier="Unverifiable",
            quant_tier="Unverifiable",
            flags={
                "has_title": 0,
                "has_organism": 0,
                "has_organism_id": 0,
                "has_instrument": 0,
                "has_result_files": 0,
                "is_unverifiable": 1,
            },
        )
        insert_audit(conn, audit)
    conn.close()
    return db_path


@pytest.fixture()
def null_flag_db(tmp_path: Path) -> Path:
    """Return a database with present, missing, and NULL result-file flags."""
    db_path = tmp_path / "nullflag.db"
    conn = get_or_create_db(db_path)

    for i, hr in [(1, 1), (2, 0), (3, None)]:
        acc = f"PXD{i:06d}"
        study = _make_study_row(acc)
        audit = _make_audit_row(acc, flags={"has_result_files": hr})
        _insert_record(conn, study, audit)
    conn.close()
    return db_path


@pytest.fixture()
def unknown_tier_db(tmp_path: Path) -> Path:
    """Database containing tier values outside the current report vocabulary."""
    db_path = tmp_path / "unknown-tier.db"
    conn = get_or_create_db(db_path)
    hostile_values: tuple[tuple[str | None, str | None], ...] = (
        ("Future Tier", "Future Quant"),
        (None, None),
        ("", ""),
        ("<script>", '" onmouseover="alert(1)'),
    )
    for index, (tier, quant_tier) in enumerate(hostile_values, start=1):
        audit = _make_audit_row(f"PXD{index:06d}")
        audit["tier"] = tier
        audit["quant_tier"] = quant_tier
        insert_audit(conn, audit)
    conn.close()
    return db_path


@pytest.fixture()
def all_gaps_db(tmp_path: Path) -> Path:
    """Database whose one verifiable row is missing every report flag."""
    db_path = tmp_path / "all-gaps.db"
    conn = get_or_create_db(db_path)
    insert_audit(
        conn,
        _make_audit_row(
            "PXD000001",
            tier="None",
            flags={column: 0 for _label, column in report_mod._FLAG_COLUMNS},
        ),
    )
    conn.close()
    return db_path


@pytest.fixture()
def xss_db(tmp_path: Path) -> Path:
    """Return a database containing markup in a persisted study title."""
    db_path = tmp_path / "xss.db"
    conn = get_or_create_db(db_path)
    study = _make_study_row("PXD000001")
    study["title"] = "<script>alert('xss')</script>"
    audit = _make_audit_row("PXD000001")
    _insert_record(conn, study, audit)
    conn.close()
    return db_path


@pytest.fixture()
def corrupted_db(tmp_path: Path) -> Path:
    """Return a regular file that is not a SQLite database."""
    db_path = tmp_path / "corrupt.db"
    db_path.write_text("not a sqlite database", encoding="utf-8")
    return db_path


@pytest.fixture()
def output_dir(tmp_path: Path) -> Path:
    """Return an absent report output path."""
    return tmp_path / "report_out"


@pytest.fixture()
def many_cohorts_db(tmp_path: Path) -> Path:
    """Database with 12 equal-sized organism and instrument cohorts."""
    db_path = tmp_path / "many_cohorts.db"
    conn = get_or_create_db(db_path)

    for i in range(12):
        acc = f"PXD{i:06d}"
        study = _make_study_row(
            acc,
            organism=f"Organism_{i}",
            instrument=f"Instrument_{i}",
        )
        audit = _make_audit_row(acc, tier="Gold")
        _insert_record(conn, study, audit)
    conn.close()
    return db_path


class TestQueryFunctions:
    """Contracts for SQL aggregation and row normalization."""

    @pytest.mark.parametrize(
        ("query", "column", "expected"),
        [
            (
                report_mod._query_tier_distribution,
                "tier",
                {
                    "Diamond": 2,
                    "Platinum": 2,
                    "Gold": 3,
                    "Silver": 2,
                    "Bronze": 2,
                    "Raw": 2,
                    "None": 2,
                    "Unverifiable": 0,
                    "Unknown": 0,
                },
            ),
            (
                report_mod._query_quant_tier_distribution,
                "quant_tier",
                {
                    "Quant-Complete": 2,
                    "Quant-Ready": 2,
                    "Partial": 6,
                    "No Quant": 5,
                    "Unverifiable": 0,
                    "Unknown": 0,
                },
            ),
        ],
        ids=("qualitative", "quantitative"),
    )
    def test_distributions_preserve_order_counts_and_percentages(
        self,
        realistic_db: Path,
        query: Callable[[sqlite3.Connection], pd.DataFrame],
        column: str,
        expected: dict[str, int],
    ) -> None:
        """Each distribution returns its complete vocabulary, exact counts, and total share."""
        conn = get_or_create_db(realistic_db)
        distribution = query(conn)
        conn.close()

        assert dict(zip(distribution[column], distribution["count"], strict=True)) == expected
        assert list(distribution[column]) == list(expected)
        assert int(distribution["count"].sum()) == 15
        assert distribution["percentage"].sum() == pytest.approx(100.0, abs=0.5)

    @pytest.mark.parametrize(
        ("query", "column", "expected"),
        [
            (report_mod._query_cohort_organism, "organism", {"Homo sapiens", "Mus musculus"}),
            (report_mod._query_cohort_instrument, "instrument", {"Orbitrap", "Q Exactive"}),
        ],
        ids=("organism", "instrument"),
    )
    def test_cohort_queries_preserve_known_groups(
        self,
        realistic_db: Path,
        query: Callable[[sqlite3.Connection], pd.DataFrame],
        column: str,
        expected: set[str],
    ) -> None:
        """Both cohort dimensions retain representative groups from stored studies."""
        conn = get_or_create_db(realistic_db)
        cohorts = query(conn)
        conn.close()

        assert expected <= set(cohorts[column])

    @pytest.mark.parametrize(
        ("query", "column", "prefix"),
        [
            (report_mod._query_cohort_organism, "organism", "Organism"),
            (report_mod._query_cohort_instrument, "instrument", "Instrument"),
        ],
        ids=("organism", "instrument"),
    )
    def test_cohort_queries_return_top_ten_with_deterministic_ties(
        self,
        many_cohorts_db: Path,
        query: Callable[[sqlite3.Connection], pd.DataFrame],
        column: str,
        prefix: str,
    ) -> None:
        """Both equal-sized cohort dimensions select the alphabetically first ten."""
        conn = get_or_create_db(many_cohorts_db)
        df = query(conn)
        conn.close()

        assert set(df[column]) == set(sorted(f"{prefix}_{i}" for i in range(12))[:10])

    def test_unknown_tiers_are_counted_and_normalized(self, unknown_tier_db: Path) -> None:
        """Future qualitative and quantitative values remain visible as unknown."""
        conn = get_or_create_db(unknown_tier_db)
        tier_dist = report_mod._query_tier_distribution(conn)
        quant_dist = report_mod._query_quant_tier_distribution(conn)
        cohort_dist = report_mod._query_cohort_organism(conn)
        rows = report_mod._query_all_accessions(conn)
        conn.close()

        assert int(tier_dist.loc[tier_dist["tier"] == "Unknown", "count"].iloc[0]) == 4
        assert int(quant_dist.loc[quant_dist["quant_tier"] == "Unknown", "count"].iloc[0]) == 4
        assert cohort_dist[["tier", "count"]].to_dict(orient="records") == [
            {"tier": "Unknown", "count": 4}
        ]
        assert [row["accession"] for row in rows] == [f"PXD{index:06d}" for index in range(1, 5)]
        assert all(row["tier"] == row["quant_tier"] == "Unknown" for row in rows)

    def test_all_accessions_preserve_sort_and_quant_tiers(self, realistic_db: Path) -> None:
        """Accession rows sort by quality then identity and retain all quant tiers."""
        conn = get_or_create_db(realistic_db)
        rows = report_mod._query_all_accessions(conn)
        conn.close()

        keys = [(report_mod._TIER_ORDER.index(row["tier"]), row["accession"]) for row in rows]
        assert keys == sorted(keys)
        assert {row["quant_tier"] for row in rows} == {
            "Quant-Complete",
            "Quant-Ready",
            "Partial",
            "No Quant",
        }


class TestEmptyDataFrames:
    """Contracts for empty and NULL distribution inputs."""

    @pytest.mark.parametrize(
        ("query", "column", "expected"),
        [
            (report_mod._query_tier_distribution, "tier", report_mod._TIER_ORDER),
            (report_mod._query_quant_tier_distribution, "quant_tier", report_mod._QUANT_TIER_ORDER),
        ],
        ids=("qualitative", "quantitative"),
    )
    def test_empty_distributions_retain_zero_filled_vocabulary(
        self,
        empty_db: Path,
        query: Callable[[sqlite3.Connection], pd.DataFrame],
        column: str,
        expected: list[str],
    ) -> None:
        """An empty audit table returns every tier with zero count and percentage."""
        conn = get_or_create_db(empty_db)
        distribution = query(conn)
        conn.close()

        assert distribution[column].tolist() == expected
        assert distribution["count"].tolist() == [0] * len(expected)
        assert distribution["percentage"].tolist() == [0.0] * len(expected)

    def test_empty_quant_dist(self, tmp_path: Path) -> None:
        """A stored NULL quant tier contributes to the unknown bucket."""
        db_path = tmp_path / "noquant.db"
        conn = get_or_create_db(db_path)
        conn.execute(
            "INSERT INTO audit (accession, tier, quant_tier, is_unverifiable) VALUES ('PXD000001', 'Gold', NULL, 0)"
        )
        conn.close()
        conn = get_or_create_db(db_path)
        df = report_mod._query_quant_tier_distribution(conn)
        conn.close()
        assert int(df["count"].sum()) == 1
        assert int(df.loc[df["quant_tier"] == "Unknown", "count"].iloc[0]) == 1

    @pytest.mark.parametrize(
        ("render", "column"),
        [
            (report_mod._render_tier_chart, "tier"),
            (report_mod._render_quant_tier_chart, "quant_tier"),
        ],
        ids=("qualitative", "quantitative"),
    )
    def test_empty_distribution_charts_render_placeholder(
        self,
        render: Callable[[pd.DataFrame], str],
        column: str,
    ) -> None:
        """Both empty distribution charts render an explicit no-data placeholder."""
        html = render(pd.DataFrame(columns=[column, "count", "percentage"]))

        assert html == '<p class="placeholder">No data available.</p>'


class TestEdgeCases:
    """Security, resource, and malformed-input report contracts."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (True, "passed"),
            (False, "failed"),
            (1, "passed"),
            (0, "failed"),
            ("1", "passed"),
            ("0", "failed"),
            ("unknown", "unknown"),
            ("other", "unknown"),
            (object(), "unknown"),
        ],
    )
    def test_normalize_flag_accepts_v2_and_v3_values(self, value: object, expected: str) -> None:
        """Read-only report normalization handles both schemas and invalid values."""
        assert report_mod._normalize_flag(value) == expected

    def test_collection_failure_closes_database(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A query failure closes the report's read-only database connection."""
        db_path = tmp_path / "input.db"
        db_path.touch()
        connection = Mock()
        failure = sqlite3.DatabaseError("query failed")
        monkeypatch.setattr(report_mod, "_open_db", Mock(return_value=connection))
        monkeypatch.setattr(report_mod, "_collect_report_data", Mock(side_effect=failure))

        with pytest.raises(sqlite3.DatabaseError, match="query failed") as caught:
            generate_report(db_path, tmp_path / "out", "X")

        assert caught.value is failure
        connection.close.assert_called_once_with()
        assert not (tmp_path / "out").exists()

    def test_empty_database_error(self, empty_db: Path, output_dir: Path) -> None:
        """A schema-valid database without audits cannot produce a misleading report."""
        with pytest.raises(ValueError, match="no audited accessions"):
            generate_report(empty_db, output_dir, "X")

    def test_all_unverifiable(self, all_unverifiable_db: Path, output_dir: Path) -> None:
        """An all-unverifiable cohort renders all three stored rows explicitly."""
        out = generate_report(all_unverifiable_db, output_dir, "Unver")
        html = out.read_text(encoding="utf-8")

        assert html.count('class="tier-Unverifiable"') == 6
        assert "fewer than 10 audited accessions" in html

    def test_unknown_tier_report_renders(self, unknown_tier_db: Path, output_dir: Path) -> None:
        """Unknown stored tiers remain renderable in every report section."""
        out = generate_report(unknown_tier_db, output_dir, "Unknown")

        html = out.read_text(encoding="utf-8")
        assert 'class="tier-Unknown">Unknown</span>' in html
        assert html.count('class="tier-Unknown"') == 8
        assert "<script>" not in html
        assert "onmouseover" not in html

    def test_xss_escaped(self, xss_db: Path, output_dir: Path) -> None:
        """Stored study titles are HTML-escaped in text and tooltip contexts."""
        out = generate_report(xss_db, output_dir, "X")
        html = out.read_text(encoding="utf-8")

        assert "<script>alert" not in html
        assert "&lt;script&gt;" in html
        assert "&#39;xss&#39;" in html

    def test_null_flags(self, null_flag_db: Path, output_dir: Path) -> None:
        """Present, missing, and unknown flags remain distinct in queries and HTML."""
        conn = get_or_create_db(null_flag_db)
        result_files = next(
            item
            for item in report_mod._query_metadata_gaps(conn)
            if item["field"] == "result_files"
        )
        conn.close()

        assert result_files == {
            "field": "result_files",
            "missing": 1,
            "unknown": 1,
            "present": 1,
            "severity": "critical",
            "pct_missing": 33.3,
        }
        out = generate_report(null_flag_db, output_dir, "Null")
        html = out.read_text(encoding="utf-8")
        assert '<span class="badge badge-unknown">?</span>' in html
        assert '<span class="badge badge-ok">+</span>' in html
        assert '<span class="badge badge-missing">-</span>' in html

    def test_mixed_v2_and_v3_flag_values_are_read_without_migration(
        self, tmp_path: Path, output_dir: Path
    ) -> None:
        """Read-only reports normalize legacy integers beside current text outcomes."""
        database = tmp_path / "mixed.db"
        columns = [column for _, column in report_mod._FLAG_COLUMNS]
        with closing(sqlite3.connect(database)) as conn:
            conn.execute(
                "CREATE TABLE study (accession TEXT PRIMARY KEY, title TEXT, organism TEXT, instrument TEXT)"
            )
            conn.execute(
                "CREATE TABLE audit (accession TEXT PRIMARY KEY, tier TEXT, quant_tier TEXT, "
                + ", ".join(f"{column} INTEGER" for column in columns)
                + ", files_fetch_failed INTEGER, is_unverifiable INTEGER, ambiguity_count INTEGER, tier_logic_version TEXT)"
            )
            conn.executemany(
                "INSERT INTO study VALUES (?, ?, ?, ?)",
                [
                    ("PXD000001", "Legacy", "Human", "Orbitrap"),
                    ("PXD000002", "Current", "Human", "Orbitrap"),
                ],
            )
            legacy_flags = [1 if column != "has_publication" else 0 for column in columns]
            current_flags = ["unknown" if column == "has_title" else "passed" for column in columns]
            prefix = "accession, tier, quant_tier, " + ", ".join(columns)
            suffix = ", files_fetch_failed, is_unverifiable, ambiguity_count, tier_logic_version"
            placeholders = ", ".join("?" for _ in range(3 + len(columns) + 4))
            conn.execute(
                f"INSERT INTO audit ({prefix}{suffix}) VALUES ({placeholders})",
                ("PXD000001", "Gold", "Partial", *legacy_flags, 0, 0, 0, "v2.1"),
            )
            conn.execute(
                f"INSERT INTO audit ({prefix}{suffix}) VALUES ({placeholders})",
                ("PXD000002", "Diamond", "Partial", *current_flags, 0, 0, 1, "v3.0"),
            )
            conn.commit()

        conn = report_mod._open_db(database)
        try:
            rows = report_mod._query_all_accessions(conn)
            gaps = report_mod._query_metadata_gaps(conn)
        finally:
            conn.close()
        title_gap = next(item for item in gaps if item["field"] == "title")
        assert title_gap["present"] == 1
        assert title_gap["unknown"] == 1
        assert title_gap["missing"] == 0
        publication_gap = next(item for item in gaps if item["field"] == "publication")
        assert publication_gap["present"] == 1
        assert publication_gap["missing"] == 1
        assert publication_gap["unknown"] == 0
        assert any(
            'badge badge-unknown">?</span>' in flag for flag in rows[0]["flags"] + rows[1]["flags"]
        )
        assert [row["accession"] for row in rows] == ["PXD000002", "PXD000001"]

        output = generate_report(database, output_dir, "Mixed")
        assert "Unknown" in output.read_text(encoding="utf-8")

    def test_every_missing_flag_reports_one_hundred_percent(self, all_gaps_db: Path) -> None:
        """Every confirmed missing flag reports 100 percent without unknowns or presents."""
        conn = get_or_create_db(all_gaps_db)
        gaps = report_mod._query_metadata_gaps(conn)
        conn.close()

        assert len(gaps) == len(report_mod._FLAG_COLUMNS)
        assert all(item["pct_missing"] == 100.0 for item in gaps)
        assert all(item["present"] == 0 and item["unknown"] == 0 for item in gaps)

    def test_direct_filenotfound(self, tmp_path: Path) -> None:
        """Direct generation rejects a missing database without creating it."""
        missing = tmp_path / "nope.db"

        with pytest.raises(FileNotFoundError, match="database not found"):
            generate_report(missing, tmp_path, "X")

        assert not missing.exists()

    def test_permission_error_mkdir(
        self, realistic_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A directory permission refusal retains its output context and cause."""
        refusal = PermissionError("denied")
        monkeypatch.setattr(Path, "mkdir", Mock(side_effect=refusal))

        with pytest.raises(PermissionError, match="cannot create output directory") as caught:
            generate_report(realistic_db, tmp_path / "out", "X")

        assert caught.value.__cause__ is refusal

    def test_output_directory_creation_os_error(
        self, realistic_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A non-permission directory failure retains output context."""
        monkeypatch.setattr(Path, "mkdir", Mock(side_effect=OSError("filesystem unavailable")))

        with pytest.raises(OSError, match="cannot create output directory") as caught:
            generate_report(realistic_db, tmp_path / "out", "X")

        assert isinstance(caught.value.__cause__, OSError)

    def test_permission_error_write(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """A temporary-file permission refusal becomes a contextual write error."""
        refusal = PermissionError("denied")
        monkeypatch.setattr(
            report_mod.tempfile,
            "NamedTemporaryFile",
            Mock(side_effect=refusal),
        )

        with pytest.raises(PermissionError, match="cannot write") as caught:
            report_mod._write_report(tmp_path / "report.html", "content")

        assert caught.value.__cause__ is refusal

    def test_atomic_write_failure_preserves_report_and_cleans_temporary(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Failed atomic replacement preserves the report and removes its temporary file."""
        report = tmp_path / "report.html"
        report.write_text("keep", encoding="utf-8")
        monkeypatch.setattr(report_mod.os, "replace", Mock(side_effect=OSError("disk full")))

        with pytest.raises(OSError, match="cannot write"):
            report_mod._write_report(report, "replacement")

        assert report.read_text(encoding="utf-8") == "keep"
        assert list(tmp_path.glob(".report.html.*.tmp")) == []

    def test_temporary_cleanup_failure_is_logged(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A secondary cleanup refusal is logged without hiding the primary write failure."""
        report = tmp_path / "report.html"
        original_unlink = Path.unlink

        def refuse_temporary(path: Path, missing_ok: bool = False) -> None:
            if path.name.startswith(".report.html."):
                raise OSError("cleanup failed")
            original_unlink(path, missing_ok=missing_ok)

        monkeypatch.setattr(report_mod.os, "replace", Mock(side_effect=OSError("disk full")))
        monkeypatch.setattr(Path, "unlink", refuse_temporary)

        with caplog.at_level(logging.WARNING), pytest.raises(OSError, match="cannot write"):
            report_mod._write_report(report, "replacement")

        assert "Could not remove report temporary file" in caplog.text


class TestReportComponent:
    """CLI-to-database-to-report component contracts."""

    pytestmark = pytest.mark.component

    def _runner(self) -> CliRunner:
        return CliRunner()

    def test_nonexistent_db(self, tmp_path: Path) -> None:
        """A missing CLI database is a usage error and leaves no artifact."""
        r = self._runner().invoke(
            main, ["report", "--db", str(tmp_path / "nope.db"), "--output", str(tmp_path)]
        )
        assert r.exit_code == 2
        assert "database not found" in r.output
        assert list(tmp_path.iterdir()) == []

    def test_empty_db(self, empty_db: Path, tmp_path: Path) -> None:
        """A schema-valid database without audits is an operational CLI failure."""
        r = self._runner().invoke(
            main, ["report", "--db", str(empty_db), "--output", str(tmp_path / "out")]
        )
        assert r.exit_code == 1
        assert "no audited accessions" in r.output

    def test_output_dir_created(self, realistic_db: Path, tmp_path: Path) -> None:
        """The CLI creates a missing nested output directory and report."""
        d = tmp_path / "new" / "nested"
        r = self._runner().invoke(main, ["report", "--db", str(realistic_db), "--output", str(d)])
        assert r.exit_code == 0
        assert (d / "report.html").exists()

    def test_overwrite_guard(self, realistic_db: Path, output_dir: Path) -> None:
        """Without overwrite, an existing report remains byte-for-byte unchanged."""
        output_dir.mkdir()
        report = output_dir / "report.html"
        report.write_text("keep", encoding="utf-8")
        r = self._runner().invoke(
            main, ["report", "--db", str(realistic_db), "--output", str(output_dir)]
        )
        assert r.exit_code == 2
        assert "already exists" in r.output
        assert report.read_text(encoding="utf-8") == "keep"

    def test_existing_output_directory_preserves_unrelated_files(
        self, realistic_db: Path, output_dir: Path
    ) -> None:
        """An existing output directory retains unrelated user files."""
        output_dir.mkdir()
        unrelated = output_dir / "notes.txt"
        unrelated.write_text("keep", encoding="utf-8")

        r = self._runner().invoke(
            main, ["report", "--db", str(realistic_db), "--output", str(output_dir)]
        )

        assert r.exit_code == 0
        assert (output_dir / "report.html").exists()
        assert unrelated.read_text(encoding="utf-8") == "keep"

    def test_default_output_writes_report_in_current_directory(
        self, realistic_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The default output writes report.html into the isolated current directory."""
        monkeypatch.chdir(tmp_path)

        r = self._runner().invoke(main, ["report", "--db", str(realistic_db)])

        assert r.exit_code == 0
        assert (tmp_path / "report.html").exists()

    def test_overwrite_flag(self, realistic_db: Path, output_dir: Path) -> None:
        """Overwrite replaces only report.html and preserves neighboring files."""
        output_dir.mkdir()
        report = output_dir / "report.html"
        report.write_text("old", encoding="utf-8")
        unrelated = output_dir / "notes.txt"
        unrelated.write_text("keep", encoding="utf-8")

        r = self._runner().invoke(
            main, ["report", "--db", str(realistic_db), "--output", str(output_dir), "--overwrite"]
        )

        assert r.exit_code == 0
        assert report.read_text(encoding="utf-8").startswith("<!DOCTYPE html>")
        assert unrelated.read_text(encoding="utf-8") == "keep"

    def test_output_path_must_be_directory(self, realistic_db: Path, tmp_path: Path) -> None:
        """A regular-file output path is rejected without replacing its contents."""
        output = tmp_path / "not-a-directory"
        output.write_text("keep", encoding="utf-8")

        r = self._runner().invoke(
            main, ["report", "--db", str(realistic_db), "--output", str(output)]
        )

        assert r.exit_code == 2
        assert "not a directory" in r.stderr
        assert output.read_text(encoding="utf-8") == "keep"

    def test_overwrite_refuses_report_target_directory(
        self, realistic_db: Path, output_dir: Path
    ) -> None:
        """Overwrite cannot replace a directory that PXAudit does not own as a report."""
        report_target = output_dir / "report.html"
        report_target.mkdir(parents=True)
        unrelated = report_target / "keep.txt"
        unrelated.write_text("keep", encoding="utf-8")

        result = self._runner().invoke(
            main,
            [
                "report",
                "--db",
                str(realistic_db),
                "--output",
                str(output_dir),
                "--overwrite",
            ],
        )

        assert result.exit_code == 2
        assert "report target" in result.stderr
        assert unrelated.read_text(encoding="utf-8") == "keep"

    def test_corrupted_db(self, corrupted_db: Path, tmp_path: Path) -> None:
        """A corrupt SQLite input becomes a stable operational CLI error."""
        r = self._runner().invoke(
            main, ["report", "--db", str(corrupted_db), "--output", str(tmp_path / "out")]
        )
        assert r.exit_code == 1
        assert "not a database" in r.output


class TestReportErrorContracts:
    """Optional dependency and CLI error translation contracts."""

    @pytest.mark.parametrize(
        ("module", "message"),
        [
            ("jinja2", "jinja2 is required"),
            ("matplotlib", "matplotlib is required"),
        ],
    )
    def test_missing_report_dependency_has_install_guidance(
        self,
        realistic_db: Path,
        output_dir: Path,
        module: str,
        message: str,
    ) -> None:
        """Each optional report dependency fails with targeted installation guidance."""
        with patch.dict(sys.modules, {module: None}), pytest.raises(ImportError, match=message):
            generate_report(realistic_db, output_dir, "X")

    @pytest.mark.parametrize(
        ("error", "message"),
        [
            (ImportError("jinja2 is required"), "jinja2 is required"),
            (FileNotFoundError("db vanished"), "db vanished"),
            (PermissionError("Permission denied"), "Permission denied"),
        ],
        ids=("dependency", "disappeared-input", "filesystem"),
    )
    def test_cli_translates_generation_failures(
        self,
        realistic_db: Path,
        tmp_path: Path,
        error: Exception,
        message: str,
    ) -> None:
        """Expected generation failures produce exit code one and their actionable message."""
        with patch("pxaudit.report.generate_report", side_effect=error):
            result = CliRunner().invoke(
                main, ["report", "--db", str(realistic_db), "--output", str(tmp_path / "out")]
            )

        assert result.exit_code == 1
        assert message in result.output


class TestHtmlOutput:
    """Self-contained HTML and concurrent rendering contracts."""

    def test_generated_report_preserves_complete_html_contract(
        self, realistic_db: Path, output_dir: Path
    ) -> None:
        """One generated artifact contains escaped, styled, embedded, and complete content."""
        out = generate_report(realistic_db, output_dir, "Cohort <Review>")

        html = out.read_text(encoding="utf-8")
        required_fragments = [
            "<title>Cohort &lt;Review&gt;</title>",
            "Quality Distribution",
            "Metadata Completeness",
            "Cohort Analysis",
            "Tier Reference",
            "FAIR Ladder",
            "All Accessions",
            "PXAudit version",
            str(realistic_db),
            "By Organism",
            "By Instrument",
            "<details open>",
            '<summary class="table-summary">',
            'title="Study PXD000001"',
        ]
        required_classes = [
            "tier-Diamond",
            "tier-Platinum",
            "tier-Gold",
            "tier-Silver",
            "tier-Bronze",
            "tier-Raw",
            "tier-None",
            "tier-Quant-Complete",
            "tier-Quant-Ready",
            "tier-Partial",
            "tier-No-Quant",
        ]

        assert html.startswith("<!DOCTYPE html>")
        assert html.endswith("</html>")
        assert all(fragment in html for fragment in required_fragments)
        assert all(f'class="{css_class}"' in html for css_class in required_classes)
        assert html.count("data:image/png;base64,") == 5
        assert "Cohort <Review>" not in html
        assert 'src="http' not in html
        assert 'href="http' not in html
        assert "fewer than 10 audited accessions" not in html

    def test_concurrent_calls(self, realistic_db: Path, tmp_path: Path) -> None:
        """Concurrent writers produce one complete report without leaked temporary files."""
        errors: list[str] = []

        def run(out: str) -> None:
            try:
                generate_report(realistic_db, Path(out), "C")
            except Exception as exc:
                errors.append(str(exc))

        output = tmp_path / "shared"
        t1 = threading.Thread(target=run, args=(str(output),))
        t2 = threading.Thread(target=run, args=(str(output),))
        t1.start()
        t2.start()
        t1.join(timeout=30)
        t2.join(timeout=30)
        assert not t1.is_alive() and not t2.is_alive()
        assert not errors
        html = (output / "report.html").read_text(encoding="utf-8")
        assert html.startswith("<!DOCTYPE html>")
        assert html.endswith("</html>")
        assert list(output.glob(".report.html.*.tmp")) == []


class TestVersionFallback:
    """Installed-version fallback contracts."""

    def test_version_unknown(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An uninstalled package reports an explicit unknown version."""

        def _raise(_name: str) -> str:
            raise importlib_metadata.PackageNotFoundError(_name)

        monkeypatch.setattr(report_mod.importlib_metadata, "version", _raise)
        assert report_mod._get_version() == "unknown"
