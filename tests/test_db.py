"""Tests for pxaudit.db — schema creation, upsert, batch inserts, constraints."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from pxaudit.db import (
    create_tables,
    get_or_create_db,
    insert_audit,
    insert_study,
    insert_study_files,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_STUDY_DATA: dict = {
    "accession": "PXD000001",
    "title": "A Test Study",
    "organism": "Homo sapiens",
    "organism_id": "9606",
    "instrument": "Orbitrap Fusion",
    "submission_year": 2023,
    "keywords": "proteomics, label-free",
    "repository": "PRIDE",
    "fetched_at": "2026-03-21T00:00:00Z",
}

_AUDIT_DATA: dict = {
    "accession": "PXD000001",
    "tier": "Gold",
    "has_title": 1,
    "has_organism": 1,
    "has_organism_id": 1,
    "has_instrument": 1,
    "has_result_files": 1,
    "has_sdrf": 1,
    "has_mztab": 0,
    "files_fetch_failed": 0,
    "is_unverifiable": 0,
    "tier_logic_version": "v0.1.0",
}


def _make_files_df(accession: str, n: int = 1) -> pd.DataFrame:
    """Return a well-formed study_files DataFrame with *n* rows."""
    return pd.DataFrame(
        {
            "accession": [accession] * n,
            "file_name": [f"file_{i}.raw" for i in range(n)],
            "file_category": ["RAW"] * n,
            "file_extension": [".raw"] * n,
            "ftp_location": [f"ftp://pride.ebi.ac.uk/file_{i}.raw" for i in range(n)],
            "file_size": [1024 * (i + 1) for i in range(n)],
        }
    )


@pytest.fixture()
def conn() -> sqlite3.Connection:
    """In-memory SQLite connection with schema already applied."""
    c = sqlite3.connect(":memory:", isolation_level=None)
    c.execute("PRAGMA foreign_keys = ON")
    create_tables(c)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------


def test_create_tables_creates_all_three_tables(conn: sqlite3.Connection) -> None:
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"study", "study_files", "audit"} == tables


def test_create_tables_is_idempotent(conn: sqlite3.Connection) -> None:
    # Calling a second time must not raise and must not change table count.
    create_tables(conn)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"study", "study_files", "audit"} == tables


def test_study_files_index_exists(conn: sqlite3.Connection) -> None:
    indexes = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert "idx_study_files_accession" in indexes


# ---------------------------------------------------------------------------
# insert_study
# ---------------------------------------------------------------------------


def test_insert_study_roundtrip(conn: sqlite3.Connection) -> None:
    insert_study(conn, _STUDY_DATA)
    row = conn.execute("SELECT * FROM study WHERE accession = 'PXD000001'").fetchone()
    assert row is not None
    (
        accession,
        title,
        organism,
        organism_id,
        instrument,
        submission_year,
        keywords,
        repository,
        fetched_at,
    ) = row
    assert accession == "PXD000001"
    assert title == "A Test Study"
    assert organism == "Homo sapiens"
    assert organism_id == "9606"
    assert instrument == "Orbitrap Fusion"
    assert submission_year == 2023
    assert keywords == "proteomics, label-free"
    assert repository == "PRIDE"
    assert fetched_at == "2026-03-21T00:00:00Z"


def test_insert_study_upsert_overwrites(conn: sqlite3.Connection) -> None:
    insert_study(conn, _STUDY_DATA)
    updated = {**_STUDY_DATA, "title": "Updated Title"}
    insert_study(conn, updated)
    rows = conn.execute("SELECT title FROM study").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "Updated Title"


def test_insert_study_nullable_fields_accepted(conn: sqlite3.Connection) -> None:
    # organism_id is nullable — must not raise.
    data = {**_STUDY_DATA, "organism_id": None}
    insert_study(conn, data)
    (organism_id,) = conn.execute(
        "SELECT organism_id FROM study WHERE accession = 'PXD000001'"
    ).fetchone()
    assert organism_id is None


def test_insert_study_missing_pk_raises(conn: sqlite3.Connection) -> None:
    bad = {**_STUDY_DATA, "accession": None}
    with pytest.raises(sqlite3.IntegrityError):
        insert_study(conn, bad)


# ---------------------------------------------------------------------------
# insert_study_files
# ---------------------------------------------------------------------------


def test_insert_study_files_zero_rows(conn: sqlite3.Connection) -> None:
    insert_study(conn, _STUDY_DATA)
    empty_df = pd.DataFrame(
        columns=[
            "accession",
            "file_name",
            "file_category",
            "file_extension",
            "ftp_location",
            "file_size",
        ]
    )
    insert_study_files(conn, "PXD000001", empty_df)
    (count,) = conn.execute("SELECT COUNT(*) FROM study_files").fetchone()
    assert count == 0


def test_insert_study_files_one_row(conn: sqlite3.Connection) -> None:
    insert_study(conn, _STUDY_DATA)
    insert_study_files(conn, "PXD000001", _make_files_df("PXD000001", 1))
    rows = conn.execute("SELECT * FROM study_files").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "PXD000001"
    assert rows[0][1] == "file_0.raw"


def test_insert_study_files_hundred_rows(conn: sqlite3.Connection) -> None:
    insert_study(conn, _STUDY_DATA)
    insert_study_files(conn, "PXD000001", _make_files_df("PXD000001", 100))
    (count,) = conn.execute("SELECT COUNT(*) FROM study_files").fetchone()
    assert count == 100


def test_insert_study_files_upsert_replaces(conn: sqlite3.Connection) -> None:
    # Insert 3 rows, then re-insert 5 rows — old 3 must be gone.
    insert_study(conn, _STUDY_DATA)
    insert_study_files(conn, "PXD000001", _make_files_df("PXD000001", 3))
    insert_study_files(conn, "PXD000001", _make_files_df("PXD000001", 5))
    (count,) = conn.execute("SELECT COUNT(*) FROM study_files").fetchone()
    assert count == 5


def test_insert_study_files_error_rolls_back(conn: sqlite3.Connection) -> None:
    # Seed 2 rows so we can assert they survive the failed re-insert.
    insert_study(conn, _STUDY_DATA)
    insert_study_files(conn, "PXD000001", _make_files_df("PXD000001", 2))

    # file_name has a NOT NULL constraint — None triggers IntegrityError mid-batch.
    bad_df = pd.DataFrame(
        [
            {
                "accession": "PXD000001",
                "file_name": None,
                "file_category": "RAW",
                "file_extension": ".raw",
                "ftp_location": None,
                "file_size": None,
            }
        ]
    )
    with pytest.raises(sqlite3.IntegrityError):
        insert_study_files(conn, "PXD000001", bad_df)

    # ROLLBACK means the DELETE was also undone — original 2 rows must still be there.
    (count,) = conn.execute("SELECT COUNT(*) FROM study_files").fetchone()
    assert count == 2


def test_insert_study_files_nullable_columns(conn: sqlite3.Connection) -> None:
    insert_study(conn, _STUDY_DATA)
    df = pd.DataFrame(
        [
            {
                "accession": "PXD000001",
                "file_name": "test.raw",
                "file_category": "RAW",
                "file_extension": ".raw",
                "ftp_location": None,
                "file_size": None,
            }
        ]
    )
    insert_study_files(conn, "PXD000001", df)
    ftp, size = conn.execute(
        "SELECT ftp_location, file_size FROM study_files WHERE accession = 'PXD000001'"
    ).fetchone()
    assert ftp is None
    assert size is None


# ---------------------------------------------------------------------------
# insert_audit
# ---------------------------------------------------------------------------


def test_insert_audit_roundtrip(conn: sqlite3.Connection) -> None:
    insert_study(conn, _STUDY_DATA)
    insert_audit(conn, _AUDIT_DATA)
    row = conn.execute("SELECT * FROM audit WHERE accession = 'PXD000001'").fetchone()
    assert row is not None
    (
        accession,
        tier,
        has_title,
        has_organism,
        has_organism_id,
        has_instrument,
        has_result_files,
        has_sdrf,
        has_mztab,
        files_fetch_failed,
        is_unverifiable,
        tier_logic_version,
    ) = row
    assert accession == "PXD000001"
    assert tier == "Gold"
    assert has_title == 1
    assert has_organism == 1
    assert has_organism_id == 1
    assert has_instrument == 1
    assert has_result_files == 1
    assert has_sdrf == 1
    assert has_mztab == 0
    assert files_fetch_failed == 0
    assert is_unverifiable == 0
    assert tier_logic_version == "v0.1.0"


def test_insert_audit_upsert_overwrites(conn: sqlite3.Connection) -> None:
    insert_study(conn, _STUDY_DATA)
    insert_audit(conn, _AUDIT_DATA)
    updated = {**_AUDIT_DATA, "tier": "Bronze"}
    insert_audit(conn, updated)
    rows = conn.execute("SELECT tier FROM audit").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "Bronze"


def test_insert_audit_missing_pk_raises(conn: sqlite3.Connection) -> None:
    bad = {**_AUDIT_DATA, "accession": None}
    with pytest.raises(sqlite3.IntegrityError):
        insert_audit(conn, bad)


# ---------------------------------------------------------------------------
# get_or_create_db
# ---------------------------------------------------------------------------


def test_get_or_create_db_creates_file(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    assert not db_path.exists()
    c = get_or_create_db(db_path)
    assert db_path.exists()
    tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"study", "study_files", "audit"} == tables
    c.close()


def test_get_or_create_db_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    c1 = get_or_create_db(db_path)
    c1.close()
    # Second open on the same file must not corrupt the schema.
    c2 = get_or_create_db(db_path)
    tables = {r[0] for r in c2.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"study", "study_files", "audit"} == tables
    c2.close()
