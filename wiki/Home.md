# PXAudit

I made PXAudit because I was checking PRIDE submissions by hand and got tired of repeating the same steps. I also wanted the result to still be there when I came back to a dataset, instead of piecing the same check together again.

PXAudit reads project metadata and file listings from the [PRIDE Archive](https://www.ebi.ac.uk/pride/). It does not download raw data or result files. Each completed audit produces:

- a seven-level FAIR tier;
- a separate quantification-readiness tier;
- the evidence flags behind both scores;
- a file inventory; and
- a local SQLite record that can be queried, exported, or rendered as an HTML report.

## Start with one dataset

PXAudit supports Python 3.12 through 3.14. From a source checkout:

```bash
git clone https://github.com/LangeLab/PXAudit.git
cd PXAudit
uv sync
uv run pxaudit check PXD000001
```

> [!NOTE]
> The first audit queries PRIDE, caches the project and file-list responses under `~/.pxaudit_cache/`, and creates `pxaudit_results.db` in the current directory. A completed audit writes the study, file inventory, and score together in one transaction.

Run the same accession again to reuse fresh cached responses:

```bash
uv run pxaudit check PXD000001
```

Use `--refresh` to try PRIDE before any stale fallback, or `--no-cache` when the run must perform no cache reads or writes:

```bash
uv run pxaudit check PXD000001 --refresh
uv run pxaudit check PXD000001 --no-cache
```

## Read the result

The qualitative tier describes how far the submission progresses through the FAIR evidence ladder, from missing mandatory metadata to a complete Diamond profile. The quant tier answers a different question: whether the deposited identification, abundance-table, and method metadata support quantitative reuse.

The score is not inferred from a label alone. PXAudit stores the underlying flags, including PSI-standard results, open spectra, SDRF, organism part, publication, quantification tables, and method metadata. See [[Tier System]] for the exact gates and worked examples.

## Audit a collection

Create a text file with one accession per line:

```text
# pilot cohort
PXD000001
PXD004683
PXD073444
```

Then audit and export it:

```bash
uv run pxaudit bulk-audit \
  --input accessions.txt \
  --format tsv \
  --output audit.tsv
```

Blank lines and lines beginning with `#` are ignored. Input is canonicalized to uppercase and duplicates are skipped. Use `--continue-on-error` when one invalid or unavailable accession should not stop the rest of the batch.

## Inspect files and generate a report

The manifest command reads a stored file inventory without contacting PRIDE:

```bash
uv run pxaudit manifest PXD004683
uv run pxaudit manifest PXD004683 --format json
uv run pxaudit summary --db pxaudit_results.db
```

Install the optional report dependencies and generate a self-contained HTML report:

```bash
uv sync --extra report
uv run pxaudit report --db pxaudit_results.db --output report/
```

The report contains qualitative and quantitative distributions, metadata gaps, the ten largest organism and instrument cohorts, tier definitions, and a quality-sorted accession table.

![Demo report screenshot](https://raw.githubusercontent.com/LangeLab/PXAudit/main/assets/demo-report-capture.png)

## When evidence is unavailable

> [!IMPORTANT]
> I would rather return no score than make a failed request look like negative scientific evidence. If the project or file response cannot be fetched and no compatible stale cache entry exists, PXAudit exits with an error and does not write a new audit. An older completed database record remains untouched.

Default and `--refresh` runs may use compatible stale cache entries after a live failure and will say when they do. `--no-cache` disables that fallback. See [[CLI Reference]] for the complete cache and failure contracts.

## Current scope

PXAudit 0.5.4 queries PRIDE for canonical `PXD` accessions. Safe identifiers from MassIVE, jPOST, iProX, and other repositories are accepted for storage but score `Unverifiable`; PXAudit does not pretend that PRIDE supplied evidence for them.

## Find the detail you need

- [[CLI Reference]] covers every command, option, configuration value, cache mode, and exit code.
- [[Tier System]] explains file evidence, FAIR gates, quantification readiness, and re-scoring.
- [[Database Schema]] documents tables, types, relationships, migrations, and SQL examples.
- [[FAQ]] answers common setup, scoring, cache, and report questions.
- [[Development]] covers architecture, tests, style, CI, and documentation changes.
