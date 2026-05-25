# PXAudit

Audit Proteomics Exchange (PRIDE) study metadata from the command line.

## Quick start

```bash
git clone https://github.com/LangeLab/PXAudit.git
cd PXAudit
uv sync
uv run pxaudit check PXD000001
```

That one command fetches the dataset's metadata and file list, classifies every file, scores it on a FAIR ladder, and writes everything to a local SQLite database.

## What you can do with it

- **Check a single dataset**: `pxaudit check PXD000001` scores one accession and prints a summary.
- **Audit a whole list**: `pxaudit bulk-audit --input ids.txt` runs through dozens or hundreds of accessions with a progress bar, then exports the results.
- **Inspect file inventories**: `pxaudit manifest PXD000001` lists every file in a dataset with its category, size, and checksum.
- **Track over time**: every audit writes to the same SQLite database, so you can query tier distributions, spot trends, and flag datasets that need re-scoring after a logic update.
- **Work offline**: API responses are cached locally. If the network goes down, PXAudit falls back to the cached data with a warning.

## How it works

```bash
PRIDE API  -->  local cache  -->  file classifier  -->  tier engine  -->  SQLite DB
```

PXAudit hits two PRIDE REST endpoints per accession (`/projects` and `/files`), caches the raw JSON, classifies every filename into one of nine `FileClass` types, then runs a deterministic Boolean checklist to assign two scores:

- **Tier**: a 7-level FAIR ladder from _None_ through _Diamond_
- **Quant Tier**: a secondary axis from _No Quant_ through _Quant-Complete_

Results are upserted into three SQLite tables (`study`, `study_files`, `audit`) so nothing gets lost when you re-audit an accession.

## Project status

Current release: **v0.3.0** (beta). Active development at [github.com/LangeLab/PXAudit](https://github.com/LangeLab/PXAudit).
