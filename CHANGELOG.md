# Changelog

All notable changes to PXAudit are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project follows [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

### Planned

- Bulk audit via `pxaudit bulk-audit --input accessions.txt` with rate-limited batch processing and TSV/JSON export.
- Cache hardening: atomic writes (#3), TTL with `--refresh` (#7).
- `--version` flag (#6).

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

### Known Issues

- #1: FK constraints unenforced on raw `sqlite3` connections.
- #3: `write_cache` not atomic; interrupted write leaves corrupt file.
- #5: `None` tier unreachable for live PXD accessions (PRIDE enforces mandatory fields).
