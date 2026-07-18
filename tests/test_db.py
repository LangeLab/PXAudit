"""Tests for pxaudit.db : schema creation, upsert, batch inserts, constraints."""

from __future__ import annotations

import sqlite3
from collections.abc import Generator
from pathlib import Path

import pandas as pd
import pytest

from pxaudit.db import (
    create_tables,
    get_or_create_db,
    insert_audit,
    insert_audit_record,
    insert_study,
    insert_study_files,
    migrate_audit_v2,
    migrate_study_files_v2,
    migrate_study_v2,
    open_existing_db,
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
    "submission_type": "COMPLETE",
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
    "has_psi_results": 1,
    "has_open_spectra": 1,
    "has_organism_part": 1,
    "has_publication": 0,
    "has_tabular_quant": 0,
    "has_quant_metadata": 0,
    "has_sdrf": 1,
    "has_mztab": 0,
    "files_fetch_failed": 0,
    "is_unverifiable": 0,
    "tier_logic_version": "v2.1",
    "quant_tier": "No Quant",
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
            "checksum": [None] * n,
            "checksum_type": [None] * n,
        }
    )


@pytest.fixture()
def conn() -> Generator[sqlite3.Connection, None, None]:
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
        submission_type,
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
    assert submission_type == "COMPLETE"
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
    # organism_id is nullable : must not raise.
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


def test_insert_study_files_fk_violation_raises(conn: sqlite3.Connection) -> None:
    # PRAGMA foreign_keys = ON is set by the fixture.
    # Inserting a study_files row whose accession is not in study must raise.
    df = _make_files_df("PXD_ORPHAN", 1)
    with pytest.raises(sqlite3.IntegrityError):
        insert_study_files(conn, "PXD_ORPHAN", df)


def test_insert_study_files_fk_enforced_via_get_or_create_db(tmp_path: Path) -> None:
    """FK constraints are enforced when using get_or_create_db().

    Regression test for ISS-001: connections from get_or_create_db() have
    PRAGMA foreign_keys = ON set once, which persists for the connection lifetime.
    """
    db_path = tmp_path / "fk_test.db"
    conn = get_or_create_db(db_path)
    insert_study(conn, _STUDY_DATA)
    orphan_df = _make_files_df("PXD_MISSING", 1)
    with pytest.raises(sqlite3.IntegrityError):
        insert_study_files(conn, "PXD_MISSING", orphan_df)
    conn.close()


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
            "checksum",
            "checksum_type",
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
    # Insert 3 rows, then re-insert 5 rows : old 3 must be gone.
    insert_study(conn, _STUDY_DATA)
    insert_study_files(conn, "PXD000001", _make_files_df("PXD000001", 3))
    insert_study_files(conn, "PXD000001", _make_files_df("PXD000001", 5))
    (count,) = conn.execute("SELECT COUNT(*) FROM study_files").fetchone()
    assert count == 5


def test_insert_study_files_error_rolls_back(conn: sqlite3.Connection) -> None:
    # Seed 2 rows so we can assert they survive the failed re-insert.
    insert_study(conn, _STUDY_DATA)
    insert_study_files(conn, "PXD000001", _make_files_df("PXD000001", 2))

    # file_name has a NOT NULL constraint : None triggers IntegrityError mid-batch.
    bad_df = pd.DataFrame(
        [
            {
                "accession": "PXD000001",
                "file_name": None,
                "file_category": "RAW",
                "file_extension": ".raw",
                "ftp_location": None,
                "file_size": None,
                "checksum": None,
                "checksum_type": None,
            }
        ]
    )
    with pytest.raises(sqlite3.IntegrityError):
        insert_study_files(conn, "PXD000001", bad_df)

    # ROLLBACK means the DELETE was also undone : original 2 rows must still be there.
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
                "checksum": None,
                "checksum_type": None,
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
        has_psi_results,
        has_open_spectra,
        has_organism_part,
        has_publication,
        has_tabular_quant,
        has_quant_metadata,
        has_sdrf,
        has_mztab,
        files_fetch_failed,
        is_unverifiable,
        tier_logic_version,
        quant_tier,
    ) = row
    assert accession == "PXD000001"
    assert tier == "Gold"
    assert has_title == 1
    assert has_organism == 1
    assert has_organism_id == 1
    assert has_instrument == 1
    assert has_result_files == 1
    assert has_psi_results == 1
    assert has_open_spectra == 1
    assert has_organism_part == 1
    assert has_publication == 0
    assert has_tabular_quant == 0
    assert has_quant_metadata == 0
    assert has_sdrf == 1
    assert has_mztab == 0
    assert files_fetch_failed == 0
    assert is_unverifiable == 0
    assert tier_logic_version == "v2.1"
    assert quant_tier == "No Quant"


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


def test_historical_files_fetch_failed_row_remains_readable(tmp_path: Path) -> None:
    """Existing incomplete-evidence rows remain readable through the read-only path."""
    database = tmp_path / "historical.db"
    writer = get_or_create_db(database)
    try:
        insert_study(writer, _STUDY_DATA)
        insert_audit(writer, {**_AUDIT_DATA, "files_fetch_failed": 1})
    finally:
        writer.close()

    reader = open_existing_db(database)
    try:
        stored = reader.execute(
            "SELECT files_fetch_failed FROM audit WHERE accession = ?", ("PXD000001",)
        ).fetchone()
    finally:
        reader.close()

    assert stored == (1,)


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


def test_get_or_create_db_upgrades_v1_to_v2(tmp_path: Path) -> None:
    """get_or_create_db must transparently upgrade a v1 database to v2."""
    path = tmp_path / "v1_upgrade.db"
    v1 = sqlite3.connect(str(path), isolation_level=None)
    _create_v1_schema(v1)
    v1.close()
    conn = get_or_create_db(path)
    audit_cols = {row[1] for row in conn.execute("PRAGMA table_info(audit)")}
    for expected in (
        "has_psi_results",
        "has_open_spectra",
        "has_organism_part",
        "has_publication",
        "has_tabular_quant",
        "has_quant_metadata",
        "quant_tier",
    ):
        assert expected in audit_cols, f"v2 column '{expected}' missing after get_or_create_db"
    study_cols = {row[1] for row in conn.execute("PRAGMA table_info(study)")}
    assert "submission_type" in study_cols
    conn.close()


def test_open_existing_db_refuses_missing_path_without_creating_it(tmp_path: Path) -> None:
    """Existing-only database access leaves a missing path untouched."""
    database = tmp_path / "missing.db"

    with pytest.raises(FileNotFoundError, match="database not found"):
        open_existing_db(database)

    assert not database.exists()
    assert list(tmp_path.iterdir()) == []


def test_open_existing_db_is_read_only_and_does_not_migrate(tmp_path: Path) -> None:
    """Existing-only access rejects writes and leaves a legacy schema unchanged."""
    database = tmp_path / "legacy.db"
    writer = sqlite3.connect(database)
    writer.execute("CREATE TABLE audit (accession TEXT PRIMARY KEY, tier TEXT)")
    writer.commit()
    writer.close()

    connection = open_existing_db(database)
    try:
        before = connection.execute("PRAGMA table_info(audit)").fetchall()
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("INSERT INTO audit VALUES ('PXD000001', 'Gold')")
        after = connection.execute("PRAGMA table_info(audit)").fetchall()
    finally:
        connection.close()

    assert before == after
    assert [row[1] for row in after] == ["accession", "tier"]


def test_audit_accession_has_no_v2_foreign_key(conn: sqlite3.Connection) -> None:
    """The v2 audit table intentionally remains independent of study."""
    assert conn.execute("PRAGMA foreign_key_list(audit)").fetchall() == []


# ---------------------------------------------------------------------------
# migrate_audit_v2
# ---------------------------------------------------------------------------


def _create_v1_schema(conn: sqlite3.Connection) -> None:
    """Build a v1-style DB in memory (no new columns, no submission_type)."""
    conn.execute("""
        CREATE TABLE study (
            accession TEXT NOT NULL PRIMARY KEY,
            title TEXT, organism TEXT, organism_id TEXT, instrument TEXT,
            submission_year INTEGER, keywords TEXT, repository TEXT, fetched_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE audit (
            accession TEXT NOT NULL PRIMARY KEY,
            tier TEXT,
            has_title INTEGER, has_organism INTEGER, has_organism_id INTEGER,
            has_instrument INTEGER, has_result_files INTEGER,
            has_sdrf INTEGER, has_mztab INTEGER,
            files_fetch_failed INTEGER, is_unverifiable INTEGER, tier_logic_version TEXT
        )
    """)


def test_migrate_audit_v2_adds_new_audit_columns(conn: sqlite3.Connection) -> None:
    """migrate_audit_v2 must add all 6 new flag columns to an existing v1 audit table."""
    # Replace the v2 schema with a v1-style one.
    v1_conn = sqlite3.connect(":memory:", isolation_level=None)
    _create_v1_schema(v1_conn)

    migrate_audit_v2(v1_conn)

    cols = {row[1] for row in v1_conn.execute("PRAGMA table_info(audit)")}
    for expected in (
        "has_psi_results",
        "has_open_spectra",
        "has_organism_part",
        "has_publication",
        "has_tabular_quant",
        "has_quant_metadata",
        "quant_tier",
    ):
        assert expected in cols, f"column '{expected}' not added by migrate_audit_v2"
    v1_conn.close()


def test_migrate_audit_v2_adds_submission_type_to_study(conn: sqlite3.Connection) -> None:
    """migrate_audit_v2 must add submission_type to a v1-style study table."""
    v1_conn = sqlite3.connect(":memory:", isolation_level=None)
    _create_v1_schema(v1_conn)

    migrate_audit_v2(v1_conn)

    study_cols = {row[1] for row in v1_conn.execute("PRAGMA table_info(study)")}
    assert "submission_type" in study_cols
    v1_conn.close()


def test_migrate_audit_v2_is_idempotent(conn: sqlite3.Connection) -> None:
    """Running migrate_audit_v2 twice on the same DB must not raise."""
    v1_conn = sqlite3.connect(":memory:", isolation_level=None)
    _create_v1_schema(v1_conn)
    migrate_audit_v2(v1_conn)
    migrate_audit_v2(v1_conn)  # second call : must be a no-op, not a failure
    v1_conn.close()


def test_migrate_audit_v2_no_op_on_v2_schema(conn: sqlite3.Connection) -> None:
    """Running migrate_audit_v2 on a freshly-created v2 DB must be a no-op."""
    # conn already has the v2 schema from create_tables() in the fixture.
    migrate_audit_v2(conn)  # must not raise
    cols = {row[1] for row in conn.execute("PRAGMA table_info(audit)")}
    assert "has_psi_results" in cols


# ---------------------------------------------------------------------------
# migrate_study_v2 tests
# ---------------------------------------------------------------------------


def test_migrate_study_v2_adds_fetched_at(conn: sqlite3.Connection) -> None:
    """migrate_study_v2 must add fetched_at to a study table that lacks it."""
    v0_conn = sqlite3.connect(":memory:", isolation_level=None)
    v0_conn.execute("""
        CREATE TABLE study (
            accession TEXT NOT NULL PRIMARY KEY,
            title TEXT, organism TEXT, instrument TEXT,
            submission_year INTEGER, keywords TEXT, repository TEXT
        )
    """)

    migrate_study_v2(v0_conn)

    cols = {row[1] for row in v0_conn.execute("PRAGMA table_info(study)")}
    assert "fetched_at" in cols
    v0_conn.close()


def test_migrate_study_v2_is_idempotent(conn: sqlite3.Connection) -> None:
    """Running migrate_study_v2 twice on the same DB must not raise."""
    v0_conn = sqlite3.connect(":memory:", isolation_level=None)
    v0_conn.execute("""
        CREATE TABLE study (
            accession TEXT NOT NULL PRIMARY KEY,
            title TEXT, organism TEXT, instrument TEXT,
            submission_year INTEGER, keywords TEXT, repository TEXT
        )
    """)
    migrate_study_v2(v0_conn)
    migrate_study_v2(v0_conn)  # second call: must be a no-op
    v0_conn.close()


def test_migrate_study_v2_no_op_on_current_schema(conn: sqlite3.Connection) -> None:
    """Running migrate_study_v2 on a freshly-created DB must be a no-op."""
    migrate_study_v2(conn)  # must not raise
    cols = {row[1] for row in conn.execute("PRAGMA table_info(study)")}
    assert "fetched_at" in cols


def test_get_or_create_db_calls_migrate_study_v2(tmp_path: Path) -> None:
    """get_or_create_db must call migrate_study_v2 for legacy databases."""
    from pxaudit.db import get_or_create_db

    # Create a v0 study table (no fetched_at) in a database file.
    db_path = tmp_path / "legacy.db"
    legacy = sqlite3.connect(str(db_path), isolation_level=None)
    legacy.execute("""
        CREATE TABLE study (
            accession TEXT NOT NULL PRIMARY KEY,
            title TEXT, organism TEXT, instrument TEXT,
            submission_year INTEGER, keywords TEXT, repository TEXT
        )
    """)
    legacy.execute("""
        CREATE TABLE audit (
            accession TEXT NOT NULL PRIMARY KEY, tier TEXT,
            has_title INTEGER, has_organism INTEGER, has_instrument INTEGER,
            has_result_files INTEGER, has_sdrf INTEGER, has_mztab INTEGER,
            files_fetch_failed INTEGER, is_unverifiable INTEGER, tier_logic_version TEXT
        )
    """)
    legacy.close()

    # Re-open via get_or_create_db: migration should add fetched_at.
    conn = get_or_create_db(db_path)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(study)")}
    assert "fetched_at" in cols
    conn.close()


# ---------------------------------------------------------------------------
# migrate_study_files_v2 tests
# ---------------------------------------------------------------------------


def test_migrate_study_files_v2_adds_checksum_columns(conn: sqlite3.Connection) -> None:
    """migrate_study_files_v2 must add checksum and checksum_type to study_files."""
    v0_conn = sqlite3.connect(":memory:", isolation_level=None)
    v0_conn.execute("""
        CREATE TABLE study_files (
            accession TEXT NOT NULL, file_name TEXT NOT NULL,
            file_category TEXT, file_extension TEXT,
            ftp_location TEXT, file_size INTEGER
        )
    """)

    migrate_study_files_v2(v0_conn)

    cols = {row[1] for row in v0_conn.execute("PRAGMA table_info(study_files)")}
    assert "checksum" in cols
    assert "checksum_type" in cols
    v0_conn.close()


def test_migrate_study_files_v2_is_idempotent(conn: sqlite3.Connection) -> None:
    """Running migrate_study_files_v2 twice must not raise."""
    v0_conn = sqlite3.connect(":memory:", isolation_level=None)
    v0_conn.execute("""
        CREATE TABLE study_files (
            accession TEXT NOT NULL, file_name TEXT NOT NULL,
            file_category TEXT, file_extension TEXT,
            ftp_location TEXT, file_size INTEGER
        )
    """)
    migrate_study_files_v2(v0_conn)
    migrate_study_files_v2(v0_conn)
    v0_conn.close()


def test_migrate_study_files_v2_no_op_on_current_schema(conn: sqlite3.Connection) -> None:
    """Running migrate_study_files_v2 on a freshly-created DB must be a no-op."""
    migrate_study_files_v2(conn)


# ---------------------------------------------------------------------------
# Checksum extraction tests
# ---------------------------------------------------------------------------


def test_study_files_checksum_stored_when_present(conn: sqlite3.Connection) -> None:
    """File with fileChecksum in PRIDE response must store checksum and MD5 type."""
    from pxaudit.cli import _extract_files_df

    files = [
        {
            "fileName": "results.mzid",
            "fileCategory": {"value": "RESULT"},
            "fileSizeBytes": 100,
            "publicFileLocations": [],
            "fileChecksum": "abc123",
        }
    ]
    df = _extract_files_df("PXD000001", files)
    assert df.loc[0, "checksum"] == "abc123"
    assert df.loc[0, "checksum_type"] == "MD5"


def test_study_files_checksum_null_when_absent(conn: sqlite3.Connection) -> None:
    """File without fileChecksum must store None for both checksum columns."""
    from pxaudit.cli import _extract_files_df

    files = [
        {
            "fileName": "results.mzid",
            "fileCategory": {"value": "RESULT"},
            "fileSizeBytes": 100,
            "publicFileLocations": [],
        }
    ]
    df = _extract_files_df("PXD000001", files)
    assert df.loc[0, "checksum"] is None or pd.isna(df.loc[0, "checksum"])
    assert df.loc[0, "checksum_type"] is None or pd.isna(df.loc[0, "checksum_type"])


def test_study_files_df_columns_include_checksum() -> None:
    """_extract_files_df must return checksum and checksum_type columns."""
    from pxaudit.cli import _extract_files_df

    df = _extract_files_df("PXD000001", [])
    assert "checksum" in df.columns
    assert "checksum_type" in df.columns


# ---------------------------------------------------------------------------
# insert_audit_record transaction tests
# ---------------------------------------------------------------------------


def test_insert_audit_record_success(tmp_path: Path) -> None:
    """All three inserts succeed: study, study_files, and audit must all be present."""
    conn = get_or_create_db(tmp_path / "test.db")
    try:
        files_df = _make_files_df("PXD000001", 2)
        insert_audit_record(conn, _STUDY_DATA, "PXD000001", files_df, _AUDIT_DATA)
        assert conn.execute("SELECT COUNT(*) FROM study").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM study_files").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM audit").fetchone()[0] == 1
    finally:
        conn.close()


def test_insert_audit_record_rollback_on_failure(tmp_path: Path) -> None:
    """If audit insert fails, study and study_files must also be rolled back."""
    conn = get_or_create_db(tmp_path / "test.db")
    try:
        bad_audit = {"accession": None, "tier": None}  # will fail NOT NULL
        files_df = _make_files_df("PXD000001", 1)
        with pytest.raises(sqlite3.IntegrityError):
            insert_audit_record(conn, _STUDY_DATA, "PXD000001", files_df, bad_audit)
        assert conn.execute("SELECT COUNT(*) FROM study").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM study_files").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM audit").fetchone()[0] == 0
    finally:
        conn.close()


def test_insert_audit_record_study_files_rollback(tmp_path: Path) -> None:
    """If study_files insert fails, study must also be rolled back."""
    conn = get_or_create_db(tmp_path / "test.db")
    try:
        # file_name has NOT NULL constraint; None triggers IntegrityError
        bad_df = pd.DataFrame(
            [
                {
                    "accession": "PXD000001",
                    "file_name": None,
                    "file_category": "RAW",
                    "file_extension": ".raw",
                    "ftp_location": None,
                    "file_size": None,
                    "checksum": None,
                    "checksum_type": None,
                }
            ]
        )
        with pytest.raises(sqlite3.IntegrityError):
            insert_audit_record(conn, _STUDY_DATA, "PXD000001", bad_df, _AUDIT_DATA)
        assert conn.execute("SELECT COUNT(*) FROM study").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM study_files").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM audit").fetchone()[0] == 0
    finally:
        conn.close()


@pytest.mark.parametrize("failure_stage", ["study", "files", "audit"])
def test_insert_audit_record_failure_preserves_prior_complete_record(
    tmp_path: Path, failure_stage: str
) -> None:
    """A failure at any insert stage rolls back to the prior complete record."""
    conn = get_or_create_db(tmp_path / "test.db")
    try:
        original_files = _make_files_df("PXD000001", 2)
        insert_audit_record(conn, _STUDY_DATA, "PXD000001", original_files, _AUDIT_DATA)
        before = (
            conn.execute("SELECT * FROM study ORDER BY accession").fetchall(),
            conn.execute("SELECT * FROM study_files ORDER BY file_name").fetchall(),
            conn.execute("SELECT * FROM audit ORDER BY accession").fetchall(),
        )

        study = {**_STUDY_DATA, "title": "Replacement"}
        files = _make_files_df("PXD000001", 1)
        audit = {**_AUDIT_DATA, "tier": "Diamond"}
        if failure_stage == "study":
            study["accession"] = None
        elif failure_stage == "files":
            files.loc[0, "file_name"] = None
        else:
            audit["accession"] = None

        with pytest.raises(sqlite3.IntegrityError):
            insert_audit_record(conn, study, "PXD000001", files, audit)

        after = (
            conn.execute("SELECT * FROM study ORDER BY accession").fetchall(),
            conn.execute("SELECT * FROM study_files ORDER BY file_name").fetchall(),
            conn.execute("SELECT * FROM audit ORDER BY accession").fetchall(),
        )
        assert after == before
    finally:
        conn.close()
