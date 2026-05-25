# FAQ

## What accessions can PXAudit audit?

Any PRIDE accession (prefix `PXD`). Accessions from MassIVE (`MSV`), jPOST (`JPST`), and iProX (`IPX`) are accepted but score `Unverifiable`. PXAudit only has access to the PRIDE REST API.

## Does PXAudit download my data files?

No. It only queries metadata endpoints. No raw data, result files, or anything else gets downloaded.

## How are files classified?

Three-stage deterministic pipeline:

1. **Extension registry**: maps known extensions like `.mzML`, `.raw`, `.mzid` to a FileClass
2. **Exact-stem map**: catches MaxQuant fixed filenames like `proteinGroups.txt`
3. **Regex patterns**: matches tool-specific output names like `report.tsv`, `psm.tsv`

The PRIDE `fileCategory` tag is used only as a fallback when none of the above match. Compression suffixes (`.gz`, `.zip`, `.bz2`) are stripped before classification so `results.mzid.gz` classifies the same as `results.mzid`.

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
  version  = {0.3.0},
  url      = {https://github.com/LangeLab/PXAudit},
  license  = {MIT},
}
```
