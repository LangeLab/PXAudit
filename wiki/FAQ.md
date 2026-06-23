# FAQ

## What accessions can PXAudit audit?

Any PRIDE accession (prefix `PXD`). Accessions from MassIVE (`MSV`), jPOST (`JPST`), and iProX (`IPX`) are accepted but score `Unverifiable`. PXAudit only has access to the PRIDE REST API.

## Does PXAudit download my data files?

No. It only queries metadata endpoints. No raw data, result files, or anything else gets downloaded.

## How are files classified?

Multi-step deterministic pipeline:

1. **Extension registry**: maps known extensions like `.mzML`, `.raw`, `.mzid` to a FileClass. Checks the original filename first (for compound formats like `.sky.zip`), then the de-compressed filename.
2. **Exact-stem map**: catches MaxQuant fixed filenames like `proteinGroups.txt`
3. **SDRF check**: matches filenames containing "sdrf" with a tabular extension
4. **PSI basename patterns**: catches mzTab and PRIDE XML variants
5. **Quant-matrix patterns**: matches tool-specific quant output like `report.tsv`, `proteinGroups.txt`
6. **ID-list patterns**: matches PSM and scan-level lists like `psm.tsv`
7. **PRIDE fileCategory fallback**: used only when none of the above match

Compression suffixes (`.gz`, `.zip`, `.bz2`) are stripped before classification so `results.mzid.gz` classifies the same as `results.mzid`.

## Why is my dataset scoring lower than expected?

A few common reasons:

- **Missing metadata**: title, organism, or instrument absent. Result: **None**.
- **No result files**: the submission has raw data but nothing processed. Result: **Raw**.
- **Non-standard results**: only proprietary search output, no mzIdentML or mzTab. Result: **Bronze**.
- **No SDRF**: no experimental-design file. Result: **Silver**.
- **Partial submission**: PARTIAL submissions have relaxed requirements but may still lack files.

## How can I re-score datasets after a logic update?

Check which accessions used an older version of the tier logic:

```sql
SELECT accession FROM audit WHERE tier_logic_version != 'v2.0';
```

Then re-run `pxaudit check` on those accessions to update their scores.

## Does PXAudit work offline?

Partially. Once an accession has been audited, its raw API responses are cached under `~/.pxaudit_cache/`. Subsequent runs reuse the cache. If the network is down and stale cached data exists, PXAudit falls back to it with a warning.

## What is the cache TTL?

Default is 7 days. Use `--refresh` to force a re-fetch regardless of cache age.

## How do I cite PXAudit?

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

## How do I generate a report?

After auditing datasets, run:

```bash
pxaudit report --db pxaudit_results.db --output report/
```

This produces a self-contained `report.html` file with donut charts, metadata completeness bars, cohort analysis, and a full accession table. Requires `jinja2` and `matplotlib` (install with `pip install pxaudit[report]`).

Use `--title` to customise the header and `--overwrite` to replace a previous report in the same directory.
