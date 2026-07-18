"""SQLite database schema, insert functions, and schema migrations.

Tables
------
study        One row per accession: title, organism, instrument, submission metadata.
study_files  One row per file: name, category, extension, FTP URL, size, checksum.
audit        One row per accession: computed tier, quant tier, and Boolean flags.

Each insert function manages its own transaction (BEGIN/COMMIT/ROLLBACK).
Migration functions (``migrate_audit_v2``, ``migrate_study_v2``,
``migrate_study_files_v2``) are idempotent and safe to run on already-
migrated databases.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

# Column order matches CREATE TABLE statement order.

_STUDY_COLS = (
    "accession",
    "title",
    "organism",
    "organism_id",
    "instrument",
    "submission_year",
    "submission_type",  # "COMPLETE" or "PARTIAL" from PRIDE API
    "keywords",
    "repository",
    "fetched_at",
)

_STUDY_FILES_COLS = (
    "accession",
    "file_name",
    "file_category",
    "file_extension",
    "ftp_location",
    "file_size",
    "checksum",
    "checksum_type",
)

_AUDIT_COLS = (
    "accession",
    "tier",
    "has_title",
    "has_organism",
    "has_organism_id",
    "has_instrument",
    "has_result_files",
    "has_psi_results",  # FileClass.RESULT found (mzIdentML / mzTab)
    "has_open_spectra",  # FileClass.PEAK found
    "has_organism_part",  # len(project["organismParts"]) > 0
    "has_publication",  # pubmedID present, non-null, and != 0
    "has_tabular_quant",  # FileClass.QUANT_MATRIX or ID_LIST found
    "has_quant_metadata",  # quantificationMethods[] non-empty
    "has_sdrf",
    "has_mztab",
    "files_fetch_failed",
    "is_unverifiable",
    "tier_logic_version",
    "quant_tier",
)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_CREATE_STUDY = """
CREATE TABLE IF NOT EXISTS study (
    accession        TEXT NOT NULL PRIMARY KEY,
    title            TEXT,
    organism         TEXT,
    organism_id      TEXT,
    instrument       TEXT,
    submission_year  INTEGER,
    submission_type  TEXT,
    keywords         TEXT,
    repository       TEXT,
    fetched_at       TEXT
);
"""

_CREATE_STUDY_FILES = """
CREATE TABLE IF NOT EXISTS study_files (
    accession       TEXT NOT NULL REFERENCES study(accession),
    file_name       TEXT NOT NULL,
    file_category   TEXT,
    file_extension  TEXT,
    ftp_location    TEXT,
    file_size       INTEGER,
    checksum        TEXT,
    checksum_type   TEXT
);
"""

_CREATE_STUDY_FILES_INDEX = """
CREATE INDEX IF NOT EXISTS idx_study_files_accession
ON study_files (accession);
"""

_CREATE_AUDIT = """
CREATE TABLE IF NOT EXISTS audit (
    accession           TEXT NOT NULL PRIMARY KEY,
    tier                TEXT,
    has_title           INTEGER,
    has_organism        INTEGER,
    has_organism_id     INTEGER,  -- tracked for analysis; not used in tier gating
    has_instrument      INTEGER,
    has_result_files    INTEGER,
    has_psi_results     INTEGER,
    has_open_spectra    INTEGER,
    has_organism_part   INTEGER,
    has_publication     INTEGER,
    has_tabular_quant   INTEGER,
    has_quant_metadata  INTEGER,
    has_sdrf            INTEGER,
    has_mztab           INTEGER,
    files_fetch_failed  INTEGER,
    is_unverifiable     INTEGER,
    tier_logic_version  TEXT,
    quant_tier          TEXT
);
"""

# ---------------------------------------------------------------------------
# DML
# ---------------------------------------------------------------------------

_INSERT_STUDY = (
    "INSERT OR REPLACE INTO study "
    "(accession, title, organism, organism_id, instrument, "
    "submission_year, submission_type, keywords, repository, fetched_at) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

_INSERT_STUDY_FILES = (
    "INSERT INTO study_files "
    "(accession, file_name, file_category, file_extension, ftp_location, file_size, "
    "checksum, checksum_type) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)

_INSERT_AUDIT = (
    "INSERT OR REPLACE INTO audit "
    "(accession, tier, has_title, has_organism, has_organism_id, has_instrument, "
    "has_result_files, has_psi_results, has_open_spectra, has_organism_part, "
    "has_publication, has_tabular_quant, has_quant_metadata, "
    "has_sdrf, has_mztab, files_fetch_failed, is_unverifiable, tier_logic_version, quant_tier) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_tables(conn: sqlite3.Connection) -> None:
    """Create all three tables and the study_files index if they do not yet exist."""
    conn.execute(_CREATE_STUDY)
    conn.execute(_CREATE_STUDY_FILES)
    conn.execute(_CREATE_STUDY_FILES_INDEX)
    conn.execute(_CREATE_AUDIT)


def get_or_create_db(path: str | Path) -> sqlite3.Connection:
    """Open (or create) a SQLite database file, apply the schema, and return the connection.

    The connection is opened with ``isolation_level=None`` (autocommit) so that
    every insert function manages its own ``BEGIN`` / ``COMMIT`` explicitly.

    Migrations are called after schema creation so that databases from an
    earlier schema version are transparently upgraded on first use.
    """
    conn = sqlite3.connect(str(Path(path)), isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    create_tables(conn)
    migrate_audit_v2(conn)
    migrate_study_v2(conn)
    migrate_study_files_v2(conn)
    return conn


def open_existing_db(path: str | Path) -> sqlite3.Connection:
    """Open an existing SQLite database without creating or migrating it.

    The returned connection is read-only and has SQLite query-only enforcement enabled.

    Parameters
    ----------
    path:
        Existing SQLite database file.

    Returns
    -------
    sqlite3.Connection
        Read-only connection to the existing database.

    Raises
    ------
    FileNotFoundError
        If ``path`` does not identify an existing regular file.
    sqlite3.Error
        If SQLite cannot open the file as a database.
    """
    database_path = Path(path)
    if not database_path.is_file():
        raise FileNotFoundError(f"database not found: {database_path}")

    uri = f"{database_path.absolute().as_uri()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only = ON")
    return conn


def insert_study(conn: sqlite3.Connection, data: dict) -> None:
    """Upsert one row into the ``study`` table.

    Missing keys in *data* are treated as NULL.
    """
    row = tuple(data.get(c) for c in _STUDY_COLS)
    conn.execute("BEGIN")
    try:
        conn.execute(_INSERT_STUDY, row)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def insert_study_files(conn: sqlite3.Connection, accession: str, files_df: pd.DataFrame) -> None:
    """Replace all file rows for *accession* with a single DELETE + ``executemany`` INSERT.

    The replacement is atomic: both operations share one explicit transaction.
    *files_df* must contain exactly the columns in ``_STUDY_FILES_COLS``
    (extra columns are ignored).  ``file_extension`` must already be derived
    by the caller; this function does not compute it.

    Any pandas NA / float NaN in the DataFrame is written as SQL NULL.
    """
    df_sub = files_df[list(_STUDY_FILES_COLS)]
    # Convert to object dtype so numpy can hold Python None instead of float NaN,
    # which sqlite3 would interpret as REAL rather than NULL.
    rows = df_sub.astype(object).where(df_sub.notna(), other=None).values.tolist()

    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM study_files WHERE accession = ?", (accession,))
        conn.executemany(_INSERT_STUDY_FILES, rows)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def insert_audit(conn: sqlite3.Connection, data: dict) -> None:
    """Upsert one row into the ``audit`` table.

    Missing keys in *data* are treated as NULL.
    """
    row = tuple(data.get(c) for c in _AUDIT_COLS)
    conn.execute("BEGIN")
    try:
        conn.execute(_INSERT_AUDIT, row)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def insert_audit_record(
    conn: sqlite3.Connection,
    study: dict,
    accession: str,
    files_df: pd.DataFrame,
    audit_data: dict,
) -> None:
    """Insert study, study_files, and audit in a single transaction.

    Either all three succeed or all three roll back.  This prevents partial
    failures from leaving orphaned rows across tables.

    Parameters
    ----------
    conn:
        SQLite connection with ``isolation_level=None`` (autocommit).
    study:
        Study row dict matching ``_STUDY_COLS``.
    accession:
        PRIDE accession string, e.g. ``"PXD000001"``.
    files_df:
        DataFrame with columns matching ``_STUDY_FILES_COLS``.
    audit_data:
        Audit row dict matching ``_AUDIT_COLS``.
    """
    conn.execute("BEGIN")
    try:
        _insert_study_row(conn, study)
        _insert_study_files_rows(conn, accession, files_df)
        _insert_audit_row(conn, audit_data)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _insert_study_row(conn: sqlite3.Connection, data: dict) -> None:
    """Insert a single study row without managing a transaction."""
    row = tuple(data.get(c) for c in _STUDY_COLS)
    conn.execute(_INSERT_STUDY, row)


def _insert_study_files_rows(
    conn: sqlite3.Connection, accession: str, files_df: pd.DataFrame
) -> None:
    """Replace all file rows for *accession* without managing a transaction."""
    df_sub = files_df[list(_STUDY_FILES_COLS)]
    rows = df_sub.astype(object).where(df_sub.notna(), other=None).values.tolist()
    conn.execute("DELETE FROM study_files WHERE accession = ?", (accession,))
    conn.executemany(_INSERT_STUDY_FILES, rows)


def _insert_audit_row(conn: sqlite3.Connection, data: dict) -> None:
    """Insert a single audit row without managing a transaction."""
    row = tuple(data.get(c) for c in _AUDIT_COLS)
    conn.execute(_INSERT_AUDIT, row)


def migrate_audit_v2(conn: sqlite3.Connection) -> None:
    """Upgrade a database from schema v1 to v2 in-place.

    Adds boolean flag columns to the ``audit`` table and ``submission_type``
    to the ``study`` table if they are not already present.  Idempotent:
    uses ``PRAGMA table_info`` to guard each ``ALTER TABLE ADD COLUMN``.
    """
    existing_audit = {row[1] for row in conn.execute("PRAGMA table_info(audit)")}
    for col in (
        "has_psi_results",
        "has_open_spectra",
        "has_organism_part",
        "has_publication",
        "has_tabular_quant",
        "has_quant_metadata",
    ):
        if col not in existing_audit:
            conn.execute(f"ALTER TABLE audit ADD COLUMN {col} INTEGER")  # noqa: S608

    if "quant_tier" not in existing_audit:
        conn.execute("ALTER TABLE audit ADD COLUMN quant_tier TEXT")  # noqa: S608

    existing_study = {row[1] for row in conn.execute("PRAGMA table_info(study)")}
    if "submission_type" not in existing_study:
        conn.execute("ALTER TABLE study ADD COLUMN submission_type TEXT")


def migrate_study_v2(conn: sqlite3.Connection) -> None:
    """Upgrade a database from schema v1 to v2 for the ``study`` table.

    Adds the ``fetched_at`` column if it is not already present.
    Idempotent: uses ``PRAGMA table_info`` to guard the column addition.
    """
    existing = {row[1] for row in conn.execute("PRAGMA table_info(study)")}
    if "fetched_at" not in existing:
        conn.execute("ALTER TABLE study ADD COLUMN fetched_at TEXT")


def migrate_study_files_v2(conn: sqlite3.Connection) -> None:
    """Upgrade a database from schema v1 to v2 for the ``study_files`` table.

    Adds ``checksum`` and ``checksum_type`` columns if they are not already present.
    Idempotent: uses ``PRAGMA table_info`` to guard each column addition.
    """
    existing = {row[1] for row in conn.execute("PRAGMA table_info(study_files)")}
    for col in ("checksum", "checksum_type"):
        if col not in existing:
            conn.execute(f"ALTER TABLE study_files ADD COLUMN {col} TEXT")  # noqa: S608


__all__ = [
    "create_tables",
    "get_or_create_db",
    "open_existing_db",
    "insert_audit",
    "insert_audit_record",
    "insert_study",
    "insert_study_files",
    "migrate_audit_v2",
    "migrate_study_files_v2",
    "migrate_study_v2",
]
