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
    "submission_type",  # «new» "COMPLETE" or "PARTIAL" from PRIDE API
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
    # ── New v2 flags ──────────────────────────────────────────────────────────────────────
    "has_psi_results",  # FileClass.RESULT found (mzIdentML / mzTab)
    "has_open_spectra",  # FileClass.PEAK found
    "has_organism_part",  # len(project["organismParts"]) > 0
    "has_publication",  # pubmedID present, non-null, and != 0
    "has_tabular_quant",  # FileClass.QUANT_MATRIX or ID_LIST found
    "has_quant_metadata",  # quantificationMethods[] non-empty
    # ─────────────────────────────────────────────────────────────────────────────────────
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
    "(accession, file_name, file_category, file_extension, ftp_location, file_size) "
    "VALUES (?, ?, ?, ?, ?, ?)"
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


def migrate_audit_v2(conn: sqlite3.Connection) -> None:
    """Upgrade an existing database to the v2 schema in-place.

    Adds the six new boolean flag columns to the ``audit`` table and the
    ``submission_type`` column to the ``study`` table if they are not already
    present.  Safe to run multiple times (idempotent: uses ``PRAGMA table_info``
    to guard each ``ALTER TABLE ADD COLUMN``).
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
