# FAQ

## What does PXAudit actually inspect?

PXAudit requests two PRIDE Archive API resources for each PXD accession:

- project metadata; and
- the complete paginated file listing.

It classifies filenames and PRIDE file categories, computes the evidence flags, and stores the result in SQLite.

## Does it download deposited data files?

No. I did not want a metadata audit to quietly turn into a multi-gigabyte download. PXAudit records names, categories, locations, sizes, and checksums returned by PRIDE, but it does not fetch raw spectra, processed results, or archive contents.

## Which accessions are supported?

Canonical PRIDE identifiers must be `PXD` followed by at least six digits. Input is trimmed and converted to uppercase.

```bash
pxaudit check pxd000001  # audited as PXD000001
```

Safe 3 to 64 character identifiers from other repositories are accepted, including MassIVE (`MSV`), jPOST (`JPST`), and iProX (`IPX`) identifiers. They score `Unverifiable` because PXAudit 0.5.1 only queries PRIDE.

Embedded whitespace, path separators, query or fragment markers, control characters, `..`, unsafe endpoints, and malformed PXD-like values are rejected.

## Why support non-PRIDE identifiers if they cannot be audited?

Mixed ProteomeXchange lists are common, and rejecting every partner identifier makes batch bookkeeping harder. PXAudit keeps those identifiers visible without inventing PRIDE evidence for them. Their repository is inferred when the prefix is recognized, and both tiers remain `Unverifiable`.

I would rather make that limitation obvious than pretend several repositories share one API contract. Native adapters can be added when their metadata and file semantics are handled properly.

## Why did my dataset score lower than expected?

The FAIR ladder stops at the first failed gate. Check the evidence in order:

1. Is the title non-empty?
2. Does the first organism entry have a name?
3. Does the first instrument entry have a name?
4. Is there recognized processed result evidence?
5. Is there a supported PSI identification result (`.mzid`, `.mzidentml`, or proteomics `.mztab`)?
6. Is there an SDRF?
7. Are open spectra and organism-part annotation both present?
8. Is a non-zero PubMed ID linked?

A publication cannot raise a dataset past Silver when SDRF is missing. Likewise, an SDRF cannot raise a dataset past Gold when open spectra or organism part is missing.

Run verbose mode to see cache and fetch details, then inspect [[Tier System]] for the exact scoring gates:

```bash
pxaudit -v check PXD000001
```

## PRIDE labels my file as RESULT. Why is `has_psi_results` false?

`RESULT` is broad processed-result evidence. `has_psi_results` is deliberately narrower and requires a filename ending in `.mzid`, `.mzidentml`, or proteomics `.mztab` after compression removal.

Examples:

| File | Processed result | PSI result |
| --- | --- | --- |
| `results.csv` with PRIDE category `RESULT` | Yes | No |
| `results.idXML` | Yes | No |
| `results.mzid.gz` | Yes | Yes |
| `results.mztab` | Yes | Yes |
| `quality.mzQC` | No | No |
| `metabolomics.mztab-m` | No | No |

The separation prevents a generic category from being treated as proof of a specific scientific standard.

## Why does `evidence.txt` not count as a quantification table?

An identification or PSM list can contain useful numbers without being an abundance summary or matrix. PXAudit reserves `has_tabular_quant` for recognized protein, peptide, precursor, or ion abundance outputs such as MaxQuant `proteinGroups.txt` or DIA-NN `report.tsv`.

This distinction matters because Quant-Ready and Quant-Complete claim that both PSI identification evidence and a reusable quantitative table are present.

## How are compressed files handled?

Known compression suffixes are removed layer by layer before format and basename rules run. These classify by their underlying content name:

```text
results.mzid.gz       -> RESULT and PSI evidence
study.sdrf.tsv.zip    -> SDRF
report.tsv.gz         -> QUANT_MATRIX
```

Compound formats are checked before generic compression removal, so a Skyline `.sky.zip` archive remains a recognized SEARCH file.

## Why is an SDRF-looking filename not recognized?

PXAudit requires an `sdrf` token and either a tabular extension (`.tsv`, `.txt`, or `.csv`) or the PRIDE experimental-design category. This accepts names such as `study.sdrf.tsv` while rejecting unrelated or ambiguous names such as `asdrf.tsv`, `sdrfile.tsv`, and `sdrf_instructions.pdf` without the matching PRIDE category.

## Does PXAudit work offline?

It can reuse cached responses, but a first audit needs PRIDE unless the cache was populated elsewhere.

The default cache lifetime is seven days. A fresh two-endpoint cache hit performs no network request. When a live request fails, default and `--refresh` modes may fall back to a compatible stale entry and report its age.

```bash
# Reuse fresh cache when available
pxaudit check PXD000001

# Try live first, then allow stale fallback
pxaudit check PXD000001 --refresh

# Use no cache at all
pxaudit check PXD000001 --no-cache
```

`--no-cache` performs no fresh read, stale read, or write. It is not an offline mode.

## What is a mixed-snapshot warning?

Project metadata and the file list are separate PRIDE responses. Version-2 cache entries store a snapshot identifier for responses retrieved during the same audit. PXAudit warns when the identifiers differ or when a compatible older entry has no identifier.

The warning means the responses are individually usable but cannot be proven to describe the same moment. Use `--refresh` to request both endpoints again. A partial live failure can still produce a mixed snapshot when one endpoint falls back to stale data.

## What happens when PRIDE is unavailable?

Default and refresh modes first try the allowed cache path described above. Without usable project data, the command exits with code 1. Without a usable file response, the audit is incomplete and also exits with code 1.

> [!IMPORTANT]
> PXAudit does not replace missing file evidence with an empty list and score it as Raw. It writes no new database rows for that attempt, and a previous completed record remains unchanged.

I care about this behavior because an API outage and a dataset with no processed files are not the same scientific observation.

## Can cache cleanup delete unrelated files?

`pxaudit cache clear` deletes only regular version-2 entries that prove PXAudit ownership and whose embedded accession, endpoint, payload type, and filename agree.

> [!CAUTION]
> `--yes` skips the confirmation prompt. It does not weaken cache-path or ownership validation.

It ignores unrelated files, directories, symbolic links, temporary files, corrupt JSON, legacy payloads, and identity mismatches. It also refuses broad cache locations such as the home directory, current directory, filesystem root, or system temporary directory.

Inspect the exact owned and ignored counts first:

```bash
pxaudit cache info
pxaudit cache clear
```

## Where are results stored?

The default database is `pxaudit_results.db` in the current directory. Override it per command or in `~/.pxaudit.toml`:

```bash
pxaudit check PXD000001 --db ~/audits/pride.db
pxaudit manifest PXD000001 --db ~/audits/pride.db
```

See [[Database Schema]] for tables, NULL semantics, migrations, and SQL examples.

## Why does `manifest` say no files were found?

The accession must have a completed audit in the selected database. Confirm both the accession and database path:

```bash
pxaudit check PXD004683 --db cohort.db
pxaudit manifest PXD004683 --db cohort.db
```

An incomplete audit does not create an empty manifest. If the audit failed because file evidence was unavailable, resolve the network or cache problem and run it again.

## Why will the report not overwrite `report.html`?

PXAudit protects an existing report unless `--overwrite` is explicit:

```bash
pxaudit report --db cohort.db --output report/ --overwrite
```

The output directory itself may already exist. Only the target `report.html` conflicts. Symbolic links and non-file targets are refused.

From a source checkout, install the optional dependencies first:

```bash
uv sync --extra report
```

## How do I re-score after a logic update?

Find rows written by an older scoring contract:

```sql
SELECT accession
FROM audit
WHERE tier_logic_version IS NULL
   OR tier_logic_version != 'v2.1';
```

Re-run each accession. Add `--refresh` when current live responses are required:

```bash
pxaudit check PXD000001 --refresh
```

## How do I cite PXAudit?

The repository includes `CITATION.cff`, which GitHub and reference managers can parse. The equivalent BibTeX entry is:

```bibtex
@software{ergin_pxaudit_2026,
  author   = {Ergin, Enes Kemal},
  title    = {{PXAudit}: A command-line tool for auditing {Proteomics Exchange} study metadata},
  year     = {2026},
  version  = {0.5.1},
  url      = {https://github.com/LangeLab/PXAudit},
  license  = {MIT},
}
```

## Where should I report a problem?

Open an issue at [github.com/LangeLab/PXAudit/issues](https://github.com/LangeLab/PXAudit/issues). Include the PXAudit version, command, accession when it is safe to share, exit code, and the complete error message. Do not attach private cache contents or databases unless you have reviewed them first.
