<!-- markdownlint-disable MD010 MD033 MD036 MD041 -->
<p align="center">
  <img src="https://raw.githubusercontent.com/LangeLab/PXAudit/main/assets/banner.svg" alt="PXAudit" width="180">
</p>

<p align="center">
  Audit Proteomics Exchange (PRIDE) study metadata from the command line.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.12--3.14-2D7D46?style=flat-square&logo=python&logoColor=white" alt="Python 3.12-3.14">
  <img src="https://img.shields.io/badge/version-0.4.0-8B5CF6?style=flat-square" alt="v0.4.0">
  <img src="https://img.shields.io/badge/status-beta-C17D10?style=flat-square" alt="Beta">
  <img src="https://img.shields.io/github/actions/workflow/status/LangeLab/PXAudit/ci.yml?branch=main&style=flat-square&logo=github&label=CI" alt="CI">
  <img src="https://img.shields.io/badge/coverage-100%25-22C55E?style=flat-square" alt="100% branch coverage">
  <img src="https://img.shields.io/badge/license-MIT-4B9D6E?style=flat-square" alt="MIT">
</p>

<p align="center">
  <a href="CHANGELOG.md"><img src="https://img.shields.io/badge/changelog-CHANGELOG-E05D44?style=flat-square" alt="Changelog"></a>
  <a href="CITATION.cff"><img src="https://img.shields.io/badge/cite-CITATION.cff-0066CC?style=flat-square" alt="Citation"></a>
  <a href="https://github.com/LangeLab/PXAudit/wiki"><img src="https://img.shields.io/badge/docs-Wiki-0F766E?style=flat-square" alt="Docs"></a>
</p>

PXAudit fetches a [PRIDE](https://www.ebi.ac.uk/pride/) dataset's project metadata and file list, classifies every file with a deterministic `FileTypeClassifier`, then assigns a 7-tier [FAIR](https://doi.org/10.1038/sdata.2016.18) ladder and a quantification-readiness tier. Results are written to a local SQLite database.

---

## Installation

Requires Python >= 3.12. [uv](https://docs.astral.sh/uv/) is the recommended runner.

```bash
git clone https://github.com/LangeLab/PXAudit.git
cd PXAudit
uv sync
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

Audit a single Proteomics Exchange accession.

```bash
uv run pxaudit check PXD004683
uv run pxaudit check PXD004683 --no-cache   # bypass local cache
uv run pxaudit check PXD004683 --db ~/audits/lab.db
```

Options: `--refresh` (re-fetch, update cache), `--no-cache` (skip cache reads), `--db PATH` (SQLite output path, default `pxaudit_results.db`).

Non-PRIDE accessions (`MSV`, `JPST`, `IPX`) are accepted without error and assigned the _Unverifiable_ tier; PXAudit only has access to the PRIDE API.

### `pxaudit bulk-audit`

Audit multiple accessions in batch.

```bash
uv run pxaudit bulk-audit --input accessions.txt
uv run pxaudit bulk-audit --input accessions.txt --format tsv --output results.tsv
cat accessions.txt | uv run pxaudit bulk-audit --input -
```

Options: `--format tsv|json|csv`, `--output PATH`, `--delay SECONDS`, `--continue-on-error`, `--overwrite`.

### `pxaudit manifest`

List files for an accession from the audit database.

```bash
uv run pxaudit manifest PXD004683
uv run pxaudit manifest PXD004683 --format json
```

Options: `--db PATH` (default `pxaudit_results.db`), `--format tsv|json`.

### `pxaudit report`

Generate a self-contained HTML report from a populated database.

```bash
uv run pxaudit report --db pxaudit_results.db
uv run pxaudit report --db pxaudit_results.db --output reports/ --title "Lab Audit Report"
```

Options: `--output DIR` (default: current directory), `--title TEXT` (default: "PXAudit Report"), `--overwrite` (overwrite existing output directory).

Requires optional dependencies: `pip install pxaudit[report]` or `uv sync --extra report`.

---

## Example Output

```text
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
| **Partial**        | Either PSI-standard IDs **or** a quant table, but not both.                     |
| **Quant-Ready**    | PSI IDs + tabular quant table present; CV-term quantification metadata missing. |
| **Quant-Complete** | PSI IDs + tabular quant table + CV-term method metadata are fully described.    |

### Validated Results

The following scores were last verified against the live PRIDE REST API on 2026-03-21 and are included in the integration test suite.

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

| Table         | Description                                                                                                                                    |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `study`       | One row per accession: title, organism, instrument, submission year and type, keywords.                                                        |
| `study_files` | One row per file: name, PRIDE category, extension, FTP URL, size in bytes.                                                                     |
| `audit`       | One row per accession: computed tier, quant tier, 13 `has_*` quality flags, `files_fetch_failed`, `is_unverifiable`, and `tier_logic_version`. |

> **Example queries**

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

Pre-commit runs `ruff` (lint + format, line-length 100) and `mypy` (strict options) on every commit. See the [wiki](https://github.com/LangeLab/PXAudit/wiki) for detailed reference documentation.

### Project Layout

```text
src/pxaudit/
├── cli.py              # click entry points (check, bulk-audit, manifest, report)
├── tier_engine.py      # 7-tier FAIR ladder + quant tier logic
├── file_classifier.py  # deterministic FileClass assignment for every file type
├── pride_client.py     # PRIDE REST API v3 client with pagination + retry/backoff
├── db.py               # SQLite schema + upsert helpers + migrations
├── cache.py            # local JSON response cache (~/.pxaudit_cache/)
└── report.py           # HTML report generation with Jinja2 + matplotlib
```

---

## Testing

```bash
# Unit tests (default, no network required)
uv run pytest

# With coverage report
uv run pytest --cov=pxaudit --cov-report=term-missing

# Live integration tests against the real PRIDE API (requires network)
uv run pytest -m integration -v --no-cov
```

The default run excludes integration tests (`-m 'not integration'` is set in `pyproject.toml`). The test suite has **506 unit tests** with **100% branch coverage** across all modules, plus **12 live integration tests** covering six real PRIDE accessions.

---

## Roadmap

- **Reporting**: `pxaudit report --db results.db` generating tier distributions, metadata completeness, cohort analysis, and dataset explorer as self-contained HTML reports.
- **Multi-repository**: plugin adapters for MassIVE, jPOST, and iProX so non-PRIDE accessions are audited rather than marked Unverifiable.

Contributions and issue reports are welcome.

---

## Citation

If you use PXAudit in your research, please cite it as:

```bibtex
@software{ergin_pxaudit_2026,
  author   = {Ergin, Enes Kemal},
  title    = {{PXAudit}: A command-line tool for auditing {Proteomics Exchange} study metadata},
  year     = {2026},
  version  = {0.4.0},
  url      = {https://github.com/LangeLab/PXAudit},
  license  = {MIT},
}
```

A `CITATION.cff` file is included in the repository root for tools that parse it automatically (e.g. GitHub's _Cite this repository_ button, Zenodo).

---

## License

MIT License. See [LICENSE](LICENSE) for details.
