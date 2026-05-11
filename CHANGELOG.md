<!-- markdownlint-disable MD024 -->

# Changelog

All notable changes to PXAudit are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project follows [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

### Planned

- Bulk audit via `pxaudit bulk-audit --input accessions.txt` with rate-limited batch processing and TSV/JSON export.

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

- `write_cache` now atomic: writes to `.tmp` then `os.replace()` — no corrupt files on crash (#3).
- `PRAGMA foreign_keys = ON` enforced inside every write function — works on raw connections (#1).
- `migrate_audit_v2(conn)` now called in `get_or_create_db()` — v1 databases are transparently upgraded (#10).
- Cache docstring now matches actual `~/.pxaudit_cache/` default (#12).
- `_PRIDE_PREFIX` deduplicated into `pxaudit/__init__.py` (#11).

### Changed

- `None` tier documented as reserved for non-PRIDE repositories in `tier_engine.py` docstring (#5).
- `has_organism_id` column annotated in SQL and `database_schema.md` as tracked but not tier-gating (#9).

---

## [0.1.0] - 2026-03-21

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
