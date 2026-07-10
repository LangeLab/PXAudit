<!-- markdownlint-disable MD024 -->

# Changelog

All notable changes to PXAudit are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project follows [Semantic Versioning](https://semver.org/).

---

## [0.5.0] - 2026-07-10 - [Unreleased]

CLI polish, user configuration, and cache management.

### Added

- Group flags: `-q`/`--quiet`, `-v`/`--verbose`, `--no-color`, and `--cache-dir`.
- Structured terminal helpers for status, warnings, detail, and errors (optional ANSI; respects `NO_COLOR` and non-TTY).
- User config file `~/.pxaudit.toml` (override path with `PXAUDIT_CONFIG`) with keys: `cache_dir`, `cache_ttl_seconds`, `db_path`, `request_delay`, `bulk_delay`, `export_format`.
- `pxaudit config show`: effective settings with source tags (`default` / `config` / `flag`).
- `pxaudit cache info` and `pxaudit cache clear` (`--yes` skips confirmation).
- Quiet `check` / `bulk-audit` compact one-line summaries; verbose detail lines for cache/fetch steps.

### Changed

- `--no-cache` skips cache reads **and** writes; `--refresh` skips reads only and still writes (help text matches behavior).
- `bulk-audit --delay` is the inter-accession `bulk_delay`; `request_delay` remains the per-request politeness delay (config-primary).
- Inter-accession delay is skipped when an accession needs no network fetch (full fresh cache hit).
- `_audit_single` no longer prints to the terminal; warnings are returned for the CLI to emit.

### Fixed

- Cache and DB defaults can be set in the user config and overridden by CLI flags (flag > config > default).
- Config rejects boolean and negative `request_delay` / `bulk_delay` / `cache_ttl_seconds` values.
- `bulk-audit` applies inter-accession delay after failed API attempts that fall back to stale cache.
- `bulk-audit` exits 130 on KeyboardInterrupt (same as `check`).
- Nested TOML tables in the config file warn that only flat keys are supported.
- Connection/proxy failures from `requests` are wrapped as `PrideAPIError` so the CLI exits cleanly.

## [0.4.0] - 2026-06-22 - [Tagged]

### Added

- `pxaudit report --db results.db` command: self-contained HTML report generation from SQLite database.
- Quality Distribution: donut charts with full legends for qualitative (7-tier FAIR ladder) and quantitative tiers.
- Metadata Completeness: horizontal bar chart showing missing fields ranked by frequency, color-coded by severity.
- Cohort Analysis: stacked bar charts showing quality distribution by organism and instrument type.
- Tier Reference: two-column grid with accurate descriptions for all tiers.
- Dataset Explorer: sortable table with colored tier badges, flag indicators, and title tooltips.
- Publication-quality matplotlib plots: 150 DPI, proper typography, clean layouts.
- Local demo database generator for report screenshots (150+ synthetic datasets).
- `test_report.py`: 38 tests covering all query functions, chart rendering, and HTML output.

### Changed

- Color palette redesigned: distinct colors matching tier meanings (blue=Diamond, gold=Gold, etc.).
- Tier descriptions updated with precise definitions from source code.
- Report structure reorganized around scientist's questions (quality, completeness, cohorts).

### Fixed

- Quantitative tier detection: now correctly identifies Quant-Ready and Quant-Complete datasets.
- Donut chart legends show all tiers even when count=0.
- Metadata completeness chart annotations instead of redundant table.

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
- Project style guide: docstring conventions, comment standards, and emoji policy.
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
- `has_organism_id` column annotated in SQL as tracked but not tier-gating (#9).

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

[0.4.0]: https://github.com/LangeLab/PXAudit/releases/tag/v0.4.0
[0.3.0]: https://github.com/LangeLab/PXAudit/releases/tag/v0.3.0
[0.2.0]: https://github.com/LangeLab/PXAudit/releases/tag/v0.2.0
[0.1.0]: https://github.com/LangeLab/PXAudit/releases/tag/v0.1.0
