<!-- markdownlint-disable MD024 -->

# Changelog

All notable changes to PXAudit are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project follows [Semantic Versioning](https://semver.org/).

---

## [0.4.0] - Unreleased

### Added

- `insert_audit_record()` in `db.py`: transaction-wrapped batch insert for study, study_files, and audit. Prevents partial failures from leaving orphaned rows.
- `_COMPOUND_EXTS` in `file_classifier.py`: derived dynamically from `_EXTENSION_TO_CLASS` keys. No more manual sync required.
- `test_compound_exts_derived_from_registry` and `test_compound_exts_all_have_dot_after_first_char` in `test_file_classifier.py`.

### Fixed

- `has_mztab` now detects compressed mzTab files (`.mztab.gz`, `.mztab.zip`, `.mztab.bz2`, etc.) by applying `strip_compression` before the extension check.
- `--no-cache` now skips both cache reads AND writes. `--refresh` skips reads but still writes to cache. Previously both flags behaved identically.
- DB inserts wrapped in single transaction: study, study_files, and audit either all succeed or all roll back.

---

## [0.3.0] - 2026-05-25 - [Tagged]

Schema provenance, file manifest, cache versioning, stale-cache fallback, public API exports, code-quality refinements, and cross-platform CI.

### Added

- Stale cache fallback: `read_cache_stale()` added; on network failure, stale cached data is served with a warning. Hard error only when no cache exists.
- `migrate_study_v2()` and `migrate_study_files_v2()`: idempotent migrations for `fetched_at`, `checksum`, and `checksum_type` columns on existing databases.
- Cache version header (`cache_version: 1`): written on every cache write, validated on read. Legacy format (pre-v0.3.0) still readable. Unknown version triggers re-fetch.
- Per-file checksum tracking: `fileChecksum` from PRIDE API stored as `checksum` (TEXT) with `checksum_type="MD5"` in `study_files` table.
- `pxaudit manifest PXD000001` command: lists files for an accession from the audit database with `--format tsv|json`. Errors if accession not yet audited.
- `AuditData` NamedTuple: typed return value for `_audit_single()`, replacing the positional 5-tuple.
- `__all__` exports defined in every public module.
- Expanded module docstrings in `cli.py` and `db.py`.
- `STYLE.md` in `plan/`: code-writing guide, docstring conventions, comment standards, and emoji policy.
- CI matrix expanded to Ubuntu, macOS, and Windows across Python 3.12-3.14. `astral-sh/setup-uv` bumped to v6.
- Integration test for live checksum and `fetched_at` verification against the PRIDE API.
- 455 unit tests (+27 from v0.2.0), 100% branch coverage.

### Fixed

- HTTP 429 rate-limit now retries with exponential backoff instead of failing immediately.
- Mixed PRIDE + non-PRIDE input in bulk-audit produces correct Unverifiable rows.
- `_unwrap_cache()` helper handles versioned, legacy, and corrupt cache formats robustly.
- Removed all `# Audit fix (Issue N)` and `# Note:` justification comments from source code.
- Replaced Unicode em-dashes and box-drawing characters with ASCII equivalents across all files.

---

## [0.2.0] - 2026-05-25 - [Tagged]

CI/CD pipeline, type checking, bulk auditing, TSV/JSON/CSV export, and rate-limit backoff.

### Added

- `pxaudit bulk-audit --input accessions.txt` command: batch audit from a file or stdin (`-`).
    - `--format tsv|json|csv` for flat-file export alongside SQLite.
    - `--delay` configurable wait between API calls (default 1s).
    - `--continue-on-error` to skip failed accessions and continue the batch.
    - `--overwrite` to overwrite existing export files.
    - Progress bar via `tqdm` with completed/failed/total summary and tier distribution.
    - Deduplicates input accessions with a warning on duplicates.
    - Graceful `KeyboardInterrupt`: writes partial results to the database.
- `tqdm>=4.67.0` added to runtime dependencies.
- HTTP 429 rate-limit handling: retries with exponential backoff instead of failing immediately.
- GitHub Actions CI workflow: runs ruff lint + format check, mypy type check, and pytest with 100% coverage enforcement on push/PR. Matrix includes 3.12, 3.13, 3.14.
- `_audit_single()` core pipeline extracted from `check`; shared by both `check` and `bulk-audit`.
- mypy type checking with strict options (`disallow_untyped_defs`, `disallow_incomplete_defs`, `check_untyped_defs`).
- mypy pre-commit hook (runs on `src/` and `tests/`).
- `fail_under = 100` in `[tool.coverage.report]` to enforce full coverage in CI.
- `mypy>=1.15.0` to dev dependencies.
- 428 unit tests (+25 from v0.1.1), 100% branch coverage.
- Bulk-audit integration test (live API): 3 real accessions verified against SQLite and TSV output.
- Mixed PRIDE + non-PRIDE bulk-audit test: confirms Unverifiable rows for MassIVE accessions.

### Fixed

- Added missing type annotations in `tests/test_pride_client.py` (`_setup_session`).
- Fixed generator return type annotation in `tests/test_db.py` (`conn` fixture).
- Fixed `tmp_path` type annotation in `tests/test_cli.py` (`Path`, not `TempPathFactory`).
- Fixed stale `# type: ignore[arg-type]` comment in `tier_engine.py` (now `call-overload`).
- Added `# type: ignore[assignment]` in `cli.py` for `read_cache` calls that return a union.

---

## [0.1.1] - 2026-05-10

Cache hardening, bug fixes, and doc improvements.

### Added

- Cache TTL: `read_cache()` compares `st_mtime` against configurable `max_age` (default 7 days). Stale entries are deleted and trigger re-fetch (#8).
- `--refresh` flag on `check`: force re-fetch even from fresh cache, still writes result.
- `--version` flag: `pxaudit --version` prints installed version (#7).
- KeyboardInterrupt handler: Ctrl+C prints clean `"Interrupted."` and exits 130 (#13).
- TTL boundary tests (at / ±1s), `max_age=0` bypass at cache layer, v1-to-v2 upgrade test.

### Fixed

- `write_cache` now atomic: writes to `.tmp` then `os.replace()`; no corrupt files on crash (#3).
- `PRAGMA foreign_keys = ON` enforced inside every write function; works on raw connections (#1).
- `migrate_audit_v2(conn)` now called in `get_or_create_db()`; v1 databases are transparently upgraded (#10).
- Cache docstring now matches actual `~/.pxaudit_cache/` default (#12).
- `_PRIDE_PREFIX` deduplicated into `pxaudit/__init__.py` (#11).

### Changed

- `None` tier documented as reserved for non-PRIDE repositories in `tier_engine.py` docstring (#5).
- `has_organism_id` column annotated in SQL and `database_schema.md` as tracked but not tier-gating (#9).

---

## [0.1.0] - 2026-03-21 - [Tagged]

First tagged release. Single-study auditing with a 7-tier FAIR ladder and quantification readiness axis.

### Added

- `pxaudit check` command: audit a single PXD accession. Outputs tier, quant tier, and Boolean flags to terminal and SQLite.
- PRIDE API client with retry/backoff, typed exceptions, Session reuse, and paginated file listing.
- Local JSON cache under `~/.pxaudit_cache/` with corruption recovery.
- `FileClass` StrEnum (9 values) with 3-stage classifier (PRIDE category, extension, basename). Compression stripping.
- 7-tier FAIR ladder: None, Raw, Bronze, Silver, Gold, Platinum, Diamond.
- Quant tier secondary axis: Unverifiable, No Quant, Partial, Quant-Ready, Quant-Complete.
- SQLite schema: `study`, `study_files`, `audit` (19 columns). Upsert on re-audit. `migrate_audit_v2()` for v1 upgrades.
- Two-stage SDRF detection: PRIDE `EXPERIMENTAL DESIGN` category + word-boundary filename regex fallback.
- Non-PRIDE accessions (`MSV`, `JPST`, `IPX`) accepted and flagged Unverifiable.
- 384 unit tests, 100% branch coverage. 10 integration tests against live PRIDE API.
- `CITATION.cff`.

### Fixed

- Cache dir resolved relative to CWD; now uses absolute `~/.pxaudit_cache/` (#2).
- `fetch_files` fetched only the first 100 files; added pagination loop (#4).

[0.3.0]: https://github.com/LangeLab/PXAudit/releases/tag/v0.3.0
[0.2.0]: https://github.com/LangeLab/PXAudit/releases/tag/v0.2.0
[0.1.0]: https://github.com/LangeLab/PXAudit/releases/tag/v0.1.0
