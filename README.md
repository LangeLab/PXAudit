# PXAudit

**Audit Proteomics Exchange (PRIDE) study metadata from the command line.**

PXAudit fetches a [PRIDE](https://www.ebi.ac.uk/pride/) dataset's project metadata and file list, classifies every file with a deterministic `FileTypeClassifier`, then assigns two orthogonal quality scores:

- **Tier** — a 7-level [FAIR](https://doi.org/10.1038/sdata.2016.18) ladder from _None_ through _Diamond_, based on metadata completeness and open-data practices.
- **Quant Tier** — a secondary axis for quantification readiness, from _No Quant_ through _Quant-Complete_.

Results are written to a local SQLite database so you can query, compare, and track scores over time.

---

## Installation

Requires Python ≥ 3.12. [uv](https://docs.astral.sh/uv/) is the recommended runner.

```bash
# Clone and install in editable mode
git clone https://github.com/LangeLab/PXAudit.git
cd PXAudit
uv sync
```

The `pxaudit` console script is installed automatically. Verify with:

```bash
uv run pxaudit --help
```

---

## Quick Start

```bash
uv run pxaudit check PXD000001
```

On first run, PXAudit fetches project metadata and file lists from the PRIDE REST API and caches both responses under `~/.pxaudit_cache/`. Subsequent runs for the same accession are instant (cache hits skip the network entirely). Audit results are written to `pxaudit_results.db` in the current directory.

---

## Usage

### `pxaudit check`

```bash
Usage: pxaudit check [OPTIONS] ACCESSION

  Audit a single Proteomics Exchange accession.

Options:
  --no-cache        Skip cache reads and always fetch fresh data from PRIDE.
  --db PATH         SQLite output path.  [default: pxaudit_results.db]
  --help            Show this message and exit.
```

#### Examples

```bash
# Audit a single dataset
uv run pxaudit check PXD004683

# Force a fresh fetch (bypasses local cache)
uv run pxaudit check PXD004683 --no-cache

# Write results to a specific database file
uv run pxaudit check PXD004683 --db ~/audits/lab.db
```

Non-PRIDE accessions (e.g. `MSV`, `JPST`, `IPX`) are accepted without error and assigned the _Unverifiable_ tier — PXAudit only has access to the PRIDE REST API.

---

## Example Output

```
Accession : PXD000001
Tier      : Silver
Quant Tier: Partial
------------------------------------------------
Metadata
  ✔ Title         TMT proteomics of human cell lines
  ✔ Organism      Homo sapiens (9606)
  ✔ Instrument    LTQ Orbitrap Velos
  ✘ Organism part annotated
  ✔ Publication   linked
  ✘ Quant metadata (CV methods)
------------------------------------------------
Files (142 total)
  ✔ Result/Search files present
  ✔ PSI-standard results (mzIdentML / mzTab-ID)
  ✔ Open spectra (mzML / MGF)
  ✘ SDRF file present
  ✔ mzTab summary present
  ✘ Tabular quant table (proteinGroups / evidence)
------------------------------------------------
```

---

## Tier System

PXAudit scores each dataset on a **7-tier FAIR ladder**. Every tier adds one FAIR requirement to the previous; a dataset must satisfy all criteria up to and including the tier it claims.

| Tier         | Requirements                                                                              |
| ------------ | ----------------------------------------------------------------------------------------- |
| **None**     | Missing a mandatory metadata field (title, organism, or instrument).                      |
| **Raw**      | Mandatory metadata present; no processed result files found.                              |
| **Bronze**   | Result/search files present, but none are PSI-standard (mzIdentML / mzTab).               |
| **Silver**   | PSI-standard results present; no SDRF experimental-design file.                           |
| **Gold**     | SDRF present; open spectra (mzML / MGF) **or** organism-part annotation missing.          |
| **Platinum** | Open spectra + organism-part annotation present; no linked PubMed publication.            |
| **Diamond**  | All FAIR criteria met: PSI results, SDRF, open spectra, organism part, and a publication. |

> Tier logic is version-stamped (`tier_logic_version = "v2.0"`) and stored in the database so that re-scoring after a logic update can be detected.

### Quant Tier (secondary axis)

The quant tier is independent of the FAIR tier and indicates quantification readiness.

| Quant Tier         | Meaning                                                                         |
| ------------------ | ------------------------------------------------------------------------------- |
| **Unverifiable**   | Non-PRIDE accession; cannot be evaluated.                                       |
| **No Quant**       | No PSI-standard results and no tabular quant files.                             |
| **Partial**        | Either PSI-standard IDs **or** a quant table — but not both.                    |
| **Quant-Ready**    | PSI IDs + tabular quant table present; CV-term quantification metadata missing. |
| **Quant-Complete** | PSI IDs + tabular quant table + CV-term method metadata — fully described.      |

### Validated Results

The following scores were produced by running `pxaudit check` against the live PRIDE REST API on 2026-03-21 and are included in the integration test suite.

| Accession | Tier     | Quant Tier |
| --------- | -------- | ---------- |
| PXD057701 | Raw      | No Quant   |
| PXD002244 | Bronze   | No Quant   |
| PXD000001 | Silver   | Partial    |
| PXD073444 | Platinum | Partial    |
| PXD075811 | Platinum | Partial    |
| PXD004683 | Diamond  | Partial    |

---

## Output Database

Every `check` run upserts three tables in the SQLite database:

| Table         | Description                                                                                                                                     |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `study`       | One row per accession — title, organism, instrument, submission year and type, keywords.                                                        |
| `study_files` | One row per file — name, PRIDE category, extension, FTP URL, size in bytes.                                                                     |
| `audit`       | One row per accession — computed tier, quant tier, 13 `has_*` quality flags, `files_fetch_failed`, `is_unverifiable`, and `tier_logic_version`. |

**Example queries**

```sql
-- Tier distribution across all audited datasets
SELECT tier, COUNT(*) AS n FROM audit GROUP BY tier ORDER BY n DESC;

-- All Diamond datasets
SELECT accession, quant_tier FROM audit WHERE tier = 'Diamond';

-- Datasets ready for re-scoring after a logic update
SELECT accession FROM audit WHERE tier_logic_version != 'v2.0';

-- File-type breakdown for a single accession
SELECT file_category, COUNT(*) AS n
FROM study_files
WHERE accession = 'PXD004683'
GROUP BY file_category;
```

---

## Development Setup

```bash
uv sync
uv run pre-commit install
```

Pre-commit runs `ruff` (lint + format, line-length 100) on every commit.

### Project Layout

```
src/pxaudit/
├── cli.py              # click entry points (pxaudit check)
├── tier_engine.py      # 7-tier FAIR ladder + quant tier logic
├── file_classifier.py  # deterministic FileClass assignment for every file type
├── pride_client.py     # PRIDE REST API v3 client with pagination + retry/backoff
├── db.py               # SQLite schema + upsert helpers
└── cache.py            # local JSON response cache (~/.pxaudit_cache/)
```

---

## Testing

```bash
# Unit tests (default — no network required)
uv run pytest

# With coverage report
uv run pytest --cov=pxaudit --cov-report=term-missing

# Live integration tests against the real PRIDE API (requires network)
uv run pytest -m integration -v --no-cov
```

The default run excludes integration tests (`-m 'not integration'` is set in `pyproject.toml`). The test suite has **384 unit tests** with **100% branch coverage** across all modules, plus **10 live integration tests** covering six real PRIDE accessions.

---

## Non-PRIDE Accessions

PXAudit only has access to the PRIDE REST API. Accessions from other repositories (MassIVE `MSV`, jPOST `JPST`, iProX `IPX`) are accepted as valid input but receive:

- `tier = "Unverifiable"`
- `quant_tier = "Unverifiable"`
- All Boolean flags set to `False`

This is by design — no assertion of quality is made for data that cannot be inspected.

---

## Roadmap

v0.1.0 covers single-study auditing. Planned work beyond this release:

- **Bulk audit** — `pxaudit bulk-audit --input accessions.txt` with rate-limited batch processing and TSV/JSON export alongside the SQLite database.
- **File manifest** — extend `study_files` with richer provenance (download URLs, file sizes already captured; planned: per-file hash, last-seen timestamps).
- **Reporting** — `pxaudit report --db results.db` generating tier distributions, SDRF adoption trends, metadata completeness over time, and an exemplar shortlist as a Quarto-rendered HTML report.
- **Multi-repository** — plugin adapters for MassIVE, jPOST, and iProX so non-PRIDE accessions are audited rather than marked Unverifiable.

Contributions and issue reports are welcome.

---

## Citation

If you use PXAudit in your research, please cite it as:

```bibtex
@software{ergin_pxaudit_2026,
  author   = {Ergin, Enes Kemal},
  title    = {{PXAudit}: A command-line tool for auditing {Proteomics Exchange} study metadata},
  year     = {2026},
  version  = {0.1.0},
  url      = {https://github.com/LangeLab/PXAudit},
  license  = {MIT},
}
```

A `CITATION.cff` file is included in the repository root for tools that parse it automatically (e.g. GitHub's _Cite this repository_ button, Zenodo).

---

## License

MIT
