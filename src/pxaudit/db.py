"""SQLite database schema creation and insert functions."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Column ordering — canonical, matches CREATE TABLE statement order
# ---------------------------------------------------------------------------

_STUDY_COLS = (
    "accession",
    "title",
    "organism",
    "organism_id",
    "instrument",
    "submission_year",
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
)

_AUDIT_COLS = (
    "accession",
    "tier",
    "has_title",
    "has_organism",
    "has_organism_id",
    "has_instrument",
    "has_result_files",
    "has_sdrf",
    "has_mztab",
    "files_fetch_failed",
    "is_unverifiable",
    "tier_logic_version",
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
    file_size       INTEGER
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
    has_organism_id     INTEGER,
    has_instrument      INTEGER,
    has_result_files    INTEGER,
    has_sdrf            INTEGER,
    has_mztab           INTEGER,
    files_fetch_failed  INTEGER,
    is_unverifiable     INTEGER,
    tier_logic_version  TEXT
);
"""

# ---------------------------------------------------------------------------
# DML
# ---------------------------------------------------------------------------

_INSERT_STUDY = (
    "INSERT OR REPLACE INTO study "
    "(accession, title, organism, organism_id, instrument, "
    "submission_year, keywords, repository, fetched_at) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

_INSERT_STUDY_FILES = (
    "INSERT INTO study_files "
    "(accession, file_name, file_category, file_extension, ftp_location, file_size) "
    "VALUES (?, ?, ?, ?, ?, ?)"
)

_INSERT_AUDIT = (
    "INSERT OR REPLACE INTO audit "
    "(accession, tier, has_title, has_organism, has_organism_id, "
    "has_instrument, has_result_files, has_sdrf, has_mztab, "
    "files_fetch_failed, is_unverifiable, tier_logic_version) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
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
    """
    conn = sqlite3.connect(str(Path(path)), isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    create_tables(conn)
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
