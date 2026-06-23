# CLI Reference

## Global flags

`--help` and `--version` work on every command.

---

## `pxaudit check`

Audit a single accession.

```bash
pxaudit check [OPTIONS] ACCESSION
```

- **Options**
    - `--refresh`: force re-fetch from PRIDE, update the local cache
    - `--no-cache`: skip cache entirely, always fetch fresh
    - `--db PATH`: SQLite output path (default: `pxaudit_results.db`)

**Examples**

```bash
# Quick check
pxaudit check PXD004683

# Re-download even if cached
pxaudit check PXD004683 --refresh

# Store results somewhere else
pxaudit check PXD004683 --db ~/projects/lab-audits.db
```

On success, prints a summary to the terminal and upserts the study, files, and audit rows into the SQLite database. If the PRIDE project endpoint is unreachable and no cached data exists, exits with code 1.

---

## `pxaudit bulk-audit`

Audit many accessions in one run.

```bash
pxaudit bulk-audit [OPTIONS]
```

**Options**

| Flag                  | Default              | Description                                            |
| --------------------- | -------------------- | ------------------------------------------------------ |
| `--input PATH`        | required             | Path to accession list (one per line) or `-` for stdin |
| `--db PATH`           | `pxaudit_results.db` | SQLite output path                                     |
| `--format FMT`        | none                 | Export results: `tsv`, `json`, or `csv`                |
| `--output PATH`       | auto-generated       | Export file path                                       |
| `--delay SEC`         | `1.0`                | Seconds to wait between API calls                      |
| `--continue-on-error` | off                  | Skip accessions that fail and keep going               |
| `--overwrite`         | off                  | Overwrite an existing export file                      |

**Examples**

```bash
# Basic batch
pxaudit bulk-audit --input accessions.txt

# Export to a TSV file for Excel
pxaudit bulk-audit --input ids.txt --format tsv --output results.tsv

# Pipe from another command, keep going on errors
cat accessions.txt | pxaudit bulk-audit --input - --continue-on-error

# Rate-limit yourself to one request every 2 seconds
pxaudit bulk-audit --input ids.txt --delay 2
```

Shows a progress bar via `tqdm` while processing. When finished, prints a summary with total, completed, failed counts and a tier distribution breakdown.

**Handy one-liner for exploring a cohort:**

```bash
# Audit every PXD from a search result, then query the DB for Silver+ datasets
grep -o 'PXD[0-9]*' search_results.txt | sort -u > cohort.txt
pxaudit bulk-audit --input cohort.txt
sqlite3 pxaudit_results.db "SELECT accession, tier FROM audit WHERE tier IN ('Silver','Gold','Platinum','Diamond') ORDER BY tier;"
```

---

## `pxaudit manifest`

List files for an accession from the database.

```bash
pxaudit manifest [OPTIONS] ACCESSION
```

- **Options**
    - `--db PATH`: SQLite database path (default: `pxaudit_results.db`)
    - `--format FMT`: output format: `tsv` (default) or `json`

**Examples**

```bash
# Default TSV output
pxaudit manifest PXD004683

# JSON for programmatic use
pxaudit manifest PXD004683 --format json
```

The accession must have been audited first (via `check` or `bulk-audit`). Output includes: file_name, file_category, file_extension, ftp_location, file_size, checksum, checksum_type. PRIDE provides checksums for most recent submissions; older datasets may have none.

---

## `pxaudit report`

Generate a self-contained HTML report from a populated database.

```bash
pxaudit report [OPTIONS]
```

**Options**

| Flag           | Default          | Description                              |
| -------------- | ---------------- | ---------------------------------------- |
| `--db PATH`    | required         | SQLite database path                     |
| `--output DIR` | `.`              | Output directory for the HTML report     |
| `--title TEXT` | `PXAudit Report` | Report title shown in the page header    |
| `--overwrite`  | off              | Overwrite an existing output directory   |

**Examples**

```bash
# Basic report in current directory
pxaudit report --db pxaudit_results.db

# Custom title and output directory
pxaudit report --db results.db --output ~/reports/ --title "My Cohort Audit"

# Overwrite a previous report
pxaudit report --db results.db --output report/ --overwrite
```

The report is a single `report.html` file (no external dependencies) containing:

- **Summary header**: total accessions, verifiable/unverifiable counts, database path, version
- **Quality Distribution**: donut charts for both qualitative (FAIR ladder) and quantitative tiers with full legends
- **Metadata Completeness**: horizontal bar chart showing % missing for each metadata field, colour-coded by severity (red = critical, amber = moderate, green = acceptable)
- **Cohort Analysis**: stacked bar charts breaking down quality by organism and instrument type
- **Tier Reference**: two-column grid explaining every qualitative and quantitative tier
- **All Accessions**: collapsible table with accession, title, tier, quant tier, and every metadata flag as a coloured badge (present / absent / unknown)

Charts are rendered with matplotlib (150 DPI) and embedded as base64 PNG. The template uses Jinja2. Both are optional dependencies installed via `pip install pxaudit[report]`.

Requires `jinja2` and `matplotlib`. Install with:

```bash
pip install pxaudit[report]
```

---

## Workflow example: from search to report

```bash
# 1. Collect accessions from a keyword search (or wherever)
echo -e "PXD000001\nPXD004683\nPXD073444" > targets.txt

# 2. Audit them all
pxaudit bulk-audit --input targets.txt --format tsv --output audit.tsv

# 3. Check what you got
sqlite3 pxaudit_results.db "
  SELECT tier, COUNT(*) AS n
  FROM audit
  GROUP BY tier
  ORDER BY n DESC;
"

# 4. Inspect a specific dataset's files
pxaudit manifest PXD004683 | head -20

# 5. Generate an HTML report
pxaudit report --db pxaudit_results.db --output report/ --title "Target Audit"

# 6. Re-audit a single accession after a logic update
pxaudit check PXD004683 --refresh
```
