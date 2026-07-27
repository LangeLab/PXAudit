"""Tests for pxaudit.db : schema creation, upsert, batch inserts, constraints."""

from __future__ import annotations

import sqlite3
from collections.abc import Generator
from contextlib import closing
from pathlib import Path

import pandas as pd
import pytest

from pxaudit.db import (
    TransactionBatch,
    _configure_journal_mode,
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


def _database_snapshot(
    connection: sqlite3.Connection,
) -> tuple[list[tuple[object, ...]], ...]:
    """Return every persisted row in deterministic table order."""
    return tuple(
        connection.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()  # noqa: S608
        for table in ("study", "study_files", "audit")
    )


@pytest.fixture()
def conn() -> Generator[sqlite3.Connection, None, None]:
    """In-memory SQLite connection with schema already applied."""
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.execute("PRAGMA foreign_keys = ON")
    create_tables(connection)
    yield connection
    connection.close()


def test_create_tables_builds_complete_idempotent_schema(
    conn: sqlite3.Connection,
) -> None:
    """Schema creation produces every table and index and remains idempotent."""
    create_tables(conn)
    schema = conn.execute("SELECT type, name FROM sqlite_master").fetchall()
    tables = {name for kind, name in schema if kind == "table"}
    indexes = {name for kind, name in schema if kind == "index"}
    assert tables == {"study", "study_files", "audit"}
    assert "idx_study_files_accession" in indexes


def test_transaction_batch_commits_at_boundary_and_tracks_progress(
    conn: sqlite3.Connection,
) -> None:
    """A full transaction batch commits its rows and reports durable progress."""
    batch = TransactionBatch(conn, 2)
    conn.execute("CREATE TABLE values_table (value INTEGER)")

    batch.begin()
    conn.execute("INSERT INTO values_table VALUES (1)")
    batch.record()
    assert batch.pending_count == 1
    assert batch.committed_count == 0

    batch.begin()
    conn.execute("INSERT INTO values_table VALUES (2)")
    batch.record()

    assert batch.pending_count == 0
    assert batch.committed_count == 2
    assert conn.execute("SELECT value FROM values_table ORDER BY value").fetchall() == [(1,), (2,)]
    batch.commit()


def test_transaction_batch_rolls_back_pending_progress(conn: sqlite3.Connection) -> None:
    """Rolling back a partial batch removes pending rows but preserves committed progress."""
    batch = TransactionBatch(conn, 3)
    conn.execute("CREATE TABLE values_table (value INTEGER)")

    batch.begin()
    conn.execute("INSERT INTO values_table VALUES (1)")
    batch.record()
    assert batch.rollback() == 1
    assert batch.committed_count == 0
    assert conn.execute("SELECT * FROM values_table").fetchall() == []
    assert batch.rollback() == 0


def test_transaction_batch_rejects_non_positive_size(conn: sqlite3.Connection) -> None:
    """A transaction batch rejects a zero or negative commit limit."""
    with pytest.raises(ValueError, match="batch_size"):
        TransactionBatch(conn, 0)


def test_insert_audit_record_can_join_active_transaction(conn: sqlite3.Connection) -> None:
    """A caller-managed transaction persists an audit record without committing it."""
    conn.execute("BEGIN")
    insert_audit_record(
        conn,
        _STUDY_DATA,
        "PXD000001",
        _make_files_df("PXD000001"),
        _AUDIT_DATA,
        manage_transaction=False,
    )
    assert conn.in_transaction
    conn.commit()
    assert _database_snapshot(conn)[0]


def test_insert_audit_record_requires_transaction_when_not_managed(
    conn: sqlite3.Connection,
) -> None:
    """A caller-managed insert rejects an inactive connection."""
    with pytest.raises(sqlite3.ProgrammingError, match="active transaction"):
        insert_audit_record(
            conn,
            _STUDY_DATA,
            "PXD000001",
            _make_files_df("PXD000001"),
            _AUDIT_DATA,
            manage_transaction=False,
        )


def test_insert_audit_record_leaves_caller_transaction_open_on_failure(
    conn: sqlite3.Connection,
) -> None:
    """A caller-managed insert leaves rollback ownership with the batch controller."""
    conn.execute("BEGIN")
    bad_files = _make_files_df("PXD000001")
    bad_files.loc[0, "file_name"] = None

    with pytest.raises(sqlite3.IntegrityError):
        insert_audit_record(
            conn,
            _STUDY_DATA,
            "PXD000001",
            bad_files,
            _AUDIT_DATA,
            manage_transaction=False,
        )

    assert conn.in_transaction
    conn.rollback()


def test_insert_study_roundtrip(conn: sqlite3.Connection) -> None:
    """A complete study survives a database round trip without coercion."""
    insert_study(conn, _STUDY_DATA)
    row = conn.execute("SELECT * FROM study WHERE accession = 'PXD000001'").fetchone()
    assert row == tuple(_STUDY_DATA.values())


def test_insert_study_upsert_overwrites(conn: sqlite3.Connection) -> None:
    """A repeated study replaces the row instead of duplicating it."""
    insert_study(conn, _STUDY_DATA)
    updated = {**_STUDY_DATA, "title": "Updated Title"}
    insert_study(conn, updated)
    rows = conn.execute("SELECT title FROM study").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "Updated Title"


def test_insert_study_nullable_fields_accepted(conn: sqlite3.Connection) -> None:
    """Nullable study metadata is stored as SQL NULL."""
    data = {**_STUDY_DATA, "organism_id": None}
    insert_study(conn, data)
    (organism_id,) = conn.execute(
        "SELECT organism_id FROM study WHERE accession = 'PXD000001'"
    ).fetchone()
    assert organism_id is None


def test_insert_study_missing_pk_raises(conn: sqlite3.Connection) -> None:
    """A study without its required accession is rejected."""
    bad = {**_STUDY_DATA, "accession": None}
    with pytest.raises(sqlite3.IntegrityError):
        insert_study(conn, bad)


def test_insert_study_files_fk_violation_raises(conn: sqlite3.Connection) -> None:
    """File rows cannot reference a study that does not exist."""
    df = _make_files_df("PXD_ORPHAN", 1)
    with pytest.raises(sqlite3.IntegrityError):
        insert_study_files(conn, "PXD_ORPHAN", df)


def test_insert_study_files_rejects_cross_accession_rows(
    conn: sqlite3.Connection,
) -> None:
    """A replacement cannot delete one accession and insert rows for another."""
    other_study = {**_STUDY_DATA, "accession": "PXD000002"}
    insert_study(conn, _STUDY_DATA)
    insert_study(conn, other_study)
    insert_study_files(conn, "PXD000001", _make_files_df("PXD000001", 1))
    insert_study_files(conn, "PXD000002", _make_files_df("PXD000002", 1))
    before = conn.execute(
        "SELECT accession, file_name FROM study_files ORDER BY accession"
    ).fetchall()

    mixed_files = _make_files_df("PXD000001", 2)
    mixed_files.loc[1, "accession"] = "PXD000002"
    with pytest.raises(ValueError, match="accession"):
        insert_study_files(conn, "PXD000001", mixed_files)

    after = conn.execute(
        "SELECT accession, file_name FROM study_files ORDER BY accession"
    ).fetchall()
    assert after == before


@pytest.mark.parametrize("missing_column", list(_make_files_df("PXD000001").columns))
def test_insert_study_files_rejects_incomplete_frames_without_mutation(
    conn: sqlite3.Connection, missing_column: str
) -> None:
    """Every required frame column is validated before an existing snapshot changes."""
    insert_study(conn, _STUDY_DATA)
    original = _make_files_df("PXD000001", 2)
    insert_study_files(conn, "PXD000001", original)

    with pytest.raises(KeyError, match=missing_column):
        insert_study_files(
            conn,
            "PXD000001",
            _make_files_df("PXD000001").drop(columns=missing_column),
        )

    stored = conn.execute("SELECT file_name FROM study_files ORDER BY rowid").fetchall()
    assert stored == [("file_0.raw",), ("file_1.raw",)]


def test_insert_study_files_fk_enforced_via_get_or_create_db(tmp_path: Path) -> None:
    """File foreign keys remain enabled on production database connections."""
    with closing(get_or_create_db(tmp_path / "fk_test.db")) as connection:
        insert_study(connection, _STUDY_DATA)
        with pytest.raises(sqlite3.IntegrityError):
            insert_study_files(connection, "PXD_MISSING", _make_files_df("PXD_MISSING"))


@pytest.mark.parametrize("row_count", [0, 1, 100])
def test_insert_study_files_handles_batch_boundaries(
    conn: sqlite3.Connection, row_count: int
) -> None:
    """Empty, singleton, and larger batches preserve every expected file name."""
    insert_study(conn, _STUDY_DATA)
    insert_study_files(conn, "PXD000001", _make_files_df("PXD000001", row_count))
    rows = conn.execute("SELECT file_name FROM study_files ORDER BY rowid").fetchall()
    assert rows == [(f"file_{index}.raw",) for index in range(row_count)]


def test_insert_study_files_upsert_replaces(conn: sqlite3.Connection) -> None:
    """Replacing a file set removes all rows from the previous snapshot."""
    insert_study(conn, _STUDY_DATA)
    insert_study_files(conn, "PXD000001", _make_files_df("PXD000001", 3))
    insert_study_files(conn, "PXD000001", _make_files_df("PXD000001", 5))
    (count,) = conn.execute("SELECT COUNT(*) FROM study_files").fetchone()
    assert count == 5


def test_insert_study_files_error_rolls_back(conn: sqlite3.Connection) -> None:
    """A failed replacement restores the complete prior file set."""
    insert_study(conn, _STUDY_DATA)
    insert_study_files(conn, "PXD000001", _make_files_df("PXD000001", 2))

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

    (count,) = conn.execute("SELECT COUNT(*) FROM study_files").fetchone()
    assert count == 2


def test_insert_study_files_nullable_columns(conn: sqlite3.Connection) -> None:
    """Optional file metadata is stored as SQL NULL."""
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


def test_insert_audit_roundtrip(conn: sqlite3.Connection) -> None:
    """A complete audit survives a database round trip without coercion."""
    insert_study(conn, _STUDY_DATA)
    insert_audit(conn, _AUDIT_DATA)
    row = conn.execute("SELECT * FROM audit WHERE accession = 'PXD000001'").fetchone()
    assert row == tuple(_AUDIT_DATA.values())


def test_insert_audit_upsert_overwrites(conn: sqlite3.Connection) -> None:
    """A repeated audit replaces the row instead of duplicating it."""
    insert_study(conn, _STUDY_DATA)
    insert_audit(conn, _AUDIT_DATA)
    updated = {**_AUDIT_DATA, "tier": "Bronze"}
    insert_audit(conn, updated)
    rows = conn.execute("SELECT tier FROM audit").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "Bronze"


def test_insert_audit_missing_pk_raises(conn: sqlite3.Connection) -> None:
    """An audit without its required accession is rejected."""
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


def test_get_or_create_db_creates_file(tmp_path: Path) -> None:
    """Opening a new path creates a database with the complete schema."""
    db_path = tmp_path / "test.db"
    assert not db_path.exists()
    with closing(get_or_create_db(db_path)) as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert db_path.exists()
    assert {"study", "study_files", "audit"} == tables
    with closing(get_or_create_db(db_path)) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)


def test_wal_setup_falls_back_to_default_journal_mode_with_warning() -> None:
    """Unavailable WAL setup selects DELETE mode and emits a warning."""

    class FakeResult:
        def __init__(self, row: tuple[str]) -> None:
            self.row = row

        def fetchone(self) -> tuple[str]:
            return self.row

    class FakeConnection:
        def __init__(self, fail_wal: bool = True) -> None:
            self.statements: list[str] = []
            self.fail_wal = fail_wal

        def execute(self, statement: str) -> FakeResult:
            self.statements.append(statement)
            if "WAL" in statement and self.fail_wal:
                raise sqlite3.OperationalError("unsupported journal mode")
            return FakeResult(("delete",))

    connection = FakeConnection()
    with pytest.warns(RuntimeWarning, match="WAL mode could not be enabled"):
        mode = _configure_journal_mode(connection)  # type: ignore[arg-type]

    assert mode == "delete"
    assert connection.statements == [
        "PRAGMA journal_mode = WAL",
        "PRAGMA journal_mode = DELETE",
    ]

    reported_connection = FakeConnection(fail_wal=False)
    with pytest.warns(RuntimeWarning, match="WAL mode is unavailable"):
        reported_mode = _configure_journal_mode(reported_connection)  # type: ignore[arg-type]

    assert reported_mode == "delete"
    assert reported_connection.statements == [
        "PRAGMA journal_mode = WAL",
        "PRAGMA journal_mode = DELETE",
    ]


def test_wal_fallback_failure_is_reported() -> None:
    """A database that rejects both journal modes raises a typed SQLite error."""

    class FailingConnection:
        def execute(self, _statement: str) -> None:
            raise sqlite3.OperationalError("journal mode denied")

    with pytest.raises(sqlite3.OperationalError, match="Default journal mode could not be enabled"):
        _configure_journal_mode(FailingConnection())  # type: ignore[arg-type]


def test_get_or_create_db_idempotent(tmp_path: Path) -> None:
    """Reopening a database leaves its schema intact."""
    db_path = tmp_path / "test.db"
    with closing(get_or_create_db(db_path)):
        pass
    with closing(get_or_create_db(db_path)) as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert {"study", "study_files", "audit"} == tables


def test_get_or_create_db_upgrades_v1_to_v2(tmp_path: Path) -> None:
    """get_or_create_db must transparently upgrade a v1 database to v2."""
    path = tmp_path / "v1_upgrade.db"
    with closing(sqlite3.connect(str(path), isolation_level=None)) as legacy:
        _create_v1_schema(legacy)
    with closing(get_or_create_db(path)) as connection:
        audit_cols = {row[1] for row in connection.execute("PRAGMA table_info(audit)")}
        study_cols = {row[1] for row in connection.execute("PRAGMA table_info(study)")}

    expected_audit = {
        "has_psi_results",
        "has_open_spectra",
        "has_organism_part",
        "has_publication",
        "has_tabular_quant",
        "has_quant_metadata",
        "quant_tier",
    }
    assert expected_audit <= audit_cols
    assert "submission_type" in study_cols


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
    with closing(sqlite3.connect(database)) as writer:
        writer.execute("CREATE TABLE audit (accession TEXT PRIMARY KEY, tier TEXT)")
        writer.commit()

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


def _create_v1_schema(conn: sqlite3.Connection) -> None:
    """Build the legacy tables needed by migration tests."""
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


def test_migrate_audit_v2_is_complete_and_idempotent() -> None:
    """The audit migration adds its full schema and tolerates repeated use."""
    expected_audit = {
        "has_psi_results",
        "has_open_spectra",
        "has_organism_part",
        "has_publication",
        "has_tabular_quant",
        "has_quant_metadata",
        "quant_tier",
    }
    with closing(sqlite3.connect(":memory:", isolation_level=None)) as legacy:
        _create_v1_schema(legacy)
        legacy.execute("INSERT INTO study (accession, title) VALUES ('PXD000001', 'Legacy')")
        legacy.execute("INSERT INTO audit (accession, tier) VALUES ('PXD000001', 'Gold')")
        migrate_audit_v2(legacy)
        migrate_audit_v2(legacy)
        audit_columns = {row[1] for row in legacy.execute("PRAGMA table_info(audit)")}
        study_columns = {row[1] for row in legacy.execute("PRAGMA table_info(study)")}
        stored = legacy.execute(
            "SELECT study.accession, title, tier FROM study JOIN audit USING (accession)"
        ).fetchone()

    assert expected_audit <= audit_columns
    assert "submission_type" in study_columns
    assert stored == ("PXD000001", "Legacy", "Gold")


def test_migrate_study_v2_is_complete_and_idempotent() -> None:
    """The study migration adds fetched_at and tolerates repeated use."""
    with closing(sqlite3.connect(":memory:", isolation_level=None)) as legacy:
        legacy.execute("""
            CREATE TABLE study (
                accession TEXT NOT NULL PRIMARY KEY,
                title TEXT, organism TEXT, instrument TEXT,
                submission_year INTEGER, keywords TEXT, repository TEXT
            )
        """)
        legacy.execute("INSERT INTO study (accession, title) VALUES ('PXD000001', 'Legacy')")
        migrate_study_v2(legacy)
        migrate_study_v2(legacy)
        columns = {row[1] for row in legacy.execute("PRAGMA table_info(study)")}
        stored = legacy.execute("SELECT accession, title FROM study").fetchone()

    assert "fetched_at" in columns
    assert stored == ("PXD000001", "Legacy")


def test_get_or_create_db_calls_migrate_study_v2(tmp_path: Path) -> None:
    """Opening a legacy database applies the study migration."""
    db_path = tmp_path / "legacy.db"
    with closing(sqlite3.connect(str(db_path), isolation_level=None)) as legacy:
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

    with closing(get_or_create_db(db_path)) as connection:
        cols = {row[1] for row in connection.execute("PRAGMA table_info(study)")}
    assert "fetched_at" in cols


def test_migrate_study_files_v2_is_complete_and_idempotent() -> None:
    """The file migration adds checksum fields and tolerates repeated use."""
    with closing(sqlite3.connect(":memory:", isolation_level=None)) as legacy:
        legacy.execute("""
            CREATE TABLE study_files (
                accession TEXT NOT NULL, file_name TEXT NOT NULL,
                file_category TEXT, file_extension TEXT,
                ftp_location TEXT, file_size INTEGER
            )
        """)
        legacy.execute(
            "INSERT INTO study_files (accession, file_name, file_size) "
            "VALUES ('PXD000001', 'legacy.raw', 1024)"
        )
        migrate_study_files_v2(legacy)
        migrate_study_files_v2(legacy)
        columns = {row[1] for row in legacy.execute("PRAGMA table_info(study_files)")}
        stored = legacy.execute(
            "SELECT accession, file_name, file_size FROM study_files"
        ).fetchone()

    assert {"checksum", "checksum_type"} <= columns
    assert stored == ("PXD000001", "legacy.raw", 1024)


def test_insert_study_files_preserves_checksum_metadata(
    conn: sqlite3.Connection,
) -> None:
    """Checksum values, algorithms, and nulls survive persistence unchanged."""
    insert_study(conn, _STUDY_DATA)
    files = _make_files_df("PXD000001", 3)
    files["checksum"] = ["a" * 32, "b" * 40, None]
    files["checksum_type"] = ["MD5", "SHA-1", None]
    insert_study_files(conn, "PXD000001", files)

    stored = conn.execute(
        "SELECT checksum, checksum_type FROM study_files ORDER BY rowid"
    ).fetchall()
    assert stored == [("a" * 32, "MD5"), ("b" * 40, "SHA-1"), (None, None)]


def test_insert_audit_record_success(tmp_path: Path) -> None:
    """A complete atomic record persists all three components."""
    with closing(get_or_create_db(tmp_path / "test.db")) as conn:
        files_df = _make_files_df("PXD000001", 2)
        insert_audit_record(conn, _STUDY_DATA, "PXD000001", files_df, _AUDIT_DATA)
        assert conn.execute("SELECT COUNT(*) FROM study").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM study_files").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM audit").fetchone()[0] == 1


@pytest.mark.parametrize("component", ["study", "files", "audit"])
def test_insert_audit_record_rejects_mixed_accessions(
    conn: sqlite3.Connection, component: str
) -> None:
    """An atomic record cannot combine identities from different accessions."""
    study = dict(_STUDY_DATA)
    files = _make_files_df("PXD000001")
    audit = dict(_AUDIT_DATA)
    if component == "study":
        study["accession"] = "PXD000002"
    elif component == "files":
        files.loc[0, "accession"] = "PXD000002"
    else:
        audit["accession"] = "PXD000002"

    with pytest.raises(ValueError, match="accession"):
        insert_audit_record(conn, study, "PXD000001", files, audit)

    assert conn.execute("SELECT COUNT(*) FROM study").fetchone() == (0,)
    assert conn.execute("SELECT COUNT(*) FROM study_files").fetchone() == (0,)
    assert conn.execute("SELECT COUNT(*) FROM audit").fetchone() == (0,)


@pytest.mark.parametrize("has_prior_record", [False, True], ids=["new", "replacement"])
@pytest.mark.parametrize("failure_table", ["study", "study_files", "audit"])
def test_insert_audit_record_rolls_back_failure_at_every_stage(
    conn: sqlite3.Connection, failure_table: str, has_prior_record: bool
) -> None:
    """Every SQL failure leaves either an empty database or the prior snapshot."""
    if has_prior_record:
        insert_audit_record(
            conn,
            _STUDY_DATA,
            "PXD000001",
            _make_files_df("PXD000001", 2),
            _AUDIT_DATA,
        )
    before = _database_snapshot(conn)
    conn.execute(
        f"CREATE TRIGGER force_failure BEFORE INSERT ON {failure_table} "  # noqa: S608
        "BEGIN SELECT RAISE(ABORT, 'forced failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced failure"):
        insert_audit_record(
            conn,
            {**_STUDY_DATA, "title": "Replacement"},
            "PXD000001",
            _make_files_df("PXD000001"),
            {**_AUDIT_DATA, "tier": "Diamond"},
        )

    after = _database_snapshot(conn)
    assert after == before


def test_insert_audit_record_interruption_rolls_back_active_transaction(
    conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An interruption after partial replacement restores the prior snapshot."""
    insert_audit_record(
        conn,
        _STUDY_DATA,
        "PXD000001",
        _make_files_df("PXD000001", 2),
        _AUDIT_DATA,
    )
    before = _database_snapshot(conn)

    def interrupt_audit_insert(*_args: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr("pxaudit.db._insert_audit_row", interrupt_audit_insert)
    with pytest.raises(KeyboardInterrupt):
        insert_audit_record(
            conn,
            {**_STUDY_DATA, "title": "Replacement"},
            "PXD000001",
            _make_files_df("PXD000001"),
            {**_AUDIT_DATA, "tier": "Diamond"},
        )

    after = _database_snapshot(conn)
    assert conn.in_transaction is False
    assert after == before
