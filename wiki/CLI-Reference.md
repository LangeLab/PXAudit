# CLI Reference

This page documents the PXAudit 0.5.4 command line. Examples use `pxaudit` for readability. From a source checkout, run the same commands with `uv run`, for example `uv run pxaudit check PXD000001`.

## Command map

| Command | Purpose | Reads PRIDE | Writes the audit database |
| --- | --- | --- | --- |
| `check ACCESSION` | Audit one accession | For `PXD` accessions when cache does not satisfy the request | Yes, after a complete audit |
| `bulk-audit --input PATH` | Audit a list and optionally export it | Uses default cache and live-fetch behavior per accession | Yes, one completed accession at a time |
| `summary --db PATH` | Print aggregate audit counts and metadata gaps | No | May migrate a legacy database |
| `manifest ACCESSION` | Print a stored file inventory | No | No |
| `report --db PATH` | Build `report.html` from stored audits | No | No |
| `config show` | Print resolved configuration and its source | No | No |
| `cache info` | Inspect validated cache entries | No | No |
| `cache clear` | Remove validated PXAudit cache entries | No | No |

## Global options

Global options must appear before the subcommand because PXAudit uses Click's command-group parsing:

```bash
pxaudit -q check PXD004683
pxaudit check -q PXD004683  # error: -q is in the wrong position
```

> [!IMPORTANT]
> Put `-q`, `-v`, `--no-color`, and `--cache-dir` before the subcommand. Click does not move group options across the command name.

- `-q`, `--quiet` uses compact status output where the command supports it.
- `-v`, `--verbose` includes cache, fetch, skipped-accession, or report details.
- `--no-color` disables ANSI color. `NO_COLOR`, quiet mode, and non-TTY output are also respected.
- `--cache-dir PATH` overrides the configured API cache directory.
- `--version` prints the installed PXAudit version.
- `--help` prints command help.

`--quiet` and `--verbose` are mutually exclusive. Using both exits with code 2.

```bash
pxaudit -q check PXD000001          # compact summary
pxaudit -v check PXD000001          # cache and fetch details
pxaudit --no-color check PXD000001  # plain output
```

## Configuration

PXAudit reads `~/.pxaudit.toml` by default. Set `PXAUDIT_CONFIG` to use another file.

```toml
cache_dir = "~/.cache/pxaudit"
cache_ttl_seconds = 604800
db_path = "pxaudit_results.db"
request_delay = 0.5
bulk_delay = 1.0
export_format = "tsv"
# Optional: true or false. Unset follows TTY detection.
color = true
```

The file is flat TOML. Nested tables are ignored with a warning. Unknown keys and invalid values are ignored individually, so one bad setting does not discard the valid settings beside it.

| Key | Built-in default | Contract |
| --- | --- | --- |
| `cache_dir` | `~/.pxaudit_cache` | Dedicated directory for project and file response envelopes |
| `cache_ttl_seconds` | `604800` | Non-negative, finite fresh-cache lifetime in seconds |
| `db_path` | `pxaudit_results.db` | Default SQLite output path |
| `request_delay` | `0.5` | Non-negative, finite delay before each PRIDE request |
| `bulk_delay` | `1.0` | Non-negative, finite delay between accessions after network use |
| `export_format` | unset | `tsv`, `csv`, or `json` |
| `color` | unset | `true` or `false`; unset enables color only for TTY output |

Booleans are rejected for numeric settings even though Python normally treats them as integers. Configuration precedence is:

```text
command-line flag > configuration file > built-in default
```

> [!NOTE]
> `config show` prints both the effective value and whether it came from a flag, the configuration file, or a built-in default.

Inspect the resolved value and source for every key:

```bash
pxaudit config show
```

## Terminal visual system

Color is an optional scan aid. The glyph and label carry the meaning when output is plain text, and data bodies such as manifest TSV and JSON are never colored.

| Meaning | Glyph | Color when enabled |
| --- | --- | --- |
| Passed | `✔` | Green |
| Failed | `✘` | Red |
| Unknown | `?` | Yellow |

FAIR and quantification tier names use one restrained color per tier: Diamond is cyan, Platinum is bright cyan, Gold is yellow, Silver is bright white, Bronze is dim yellow, Raw is muted, and None is dim. `Quant-Complete`, `Quant-Ready`, `Partial`, and `No Quant` use the same restrained treatment. There are no background fills, box frames, or decorative banners.

In `summary`, the `failed` and `unknown` gap markers use the same red and yellow outcome styles as checklist flags when color is enabled.

Color is enabled for a TTY unless `color = false`, `--no-color`, `NO_COLOR`, or quiet mode suppresses it. Non-TTY output is plain by default; an explicit `color = true` setting is the opt-in override. Windows Terminal and a modern UTF-8 locale are expected for the `✔`, `✘`, and `?` glyphs; `--no-color` changes styling only, not the glyph vocabulary.

## `pxaudit check`

Audit one accession:

```bash
pxaudit check [OPTIONS] ACCESSION
```

- `--db PATH` writes to this SQLite database instead of the configured path.
- `--refresh` skips fresh cache reads, fetches live, writes successful responses, and allows stale fallback after failure.
- `--no-cache` performs no fresh or stale cache reads and no cache writes.

Examples:

```bash
# Default cache and database
pxaudit check PXD000001

# Fetch current responses even when fresh cache entries exist
pxaudit check PXD000001 --refresh

# Perform a live-only audit without touching the cache
pxaudit check PXD000001 --no-cache

# Store the completed audit in another database
pxaudit check PXD000001 --db ~/audits/pride.db
```

Successful human-readable output follows this shape. Metadata values and the file count come from the accession; ANSI styling is omitted here:

```text
Accession : PXD000001
Tier      : Diamond
Quant Tier: Quant-Complete
------------------------------------------------
Metadata
  ✔ Title         Example study
  ✔ Organism      Homo sapiens (NEWT:9606)
  ✔ Instrument    Orbitrap Fusion
  ✔ Organism part annotated
  ✔ Publication   linked
  ✔ Quant metadata (CV methods)
------------------------------------------------
Files (5 total)
  ✔ Result/Search files present
  ✔ PSI-standard results (mzIdentML / mzTab-ID)
  ✔ Open spectra (mzML / MGF)
  ✔ SDRF file present
  ✔ mzTab summary present
  ✔ Tabular quant summary or matrix
------------------------------------------------
```

For automation, `-q` replaces the checklist with one stable line:

```text
PXD000001  Diamond  Quant-Complete  db=pxaudit_results.db
```

Input is trimmed and canonicalized to uppercase. A PRIDE accession must be `PXD` followed by at least six digits. Other identifiers may contain 3 to 64 ASCII letters, digits, dots, underscores, or hyphens, must begin and end with an alphanumeric character, and may not contain `..`. Safe non-PRIDE identifiers are stored as `Unverifiable` because PXAudit does not query their repositories.

On success, `check` prints the two tiers and their evidence, then replaces the study, file inventory, and audit rows in one transaction. Evidence uses `✔` for `passed`, `✘` for `failed`, and `?` for `unknown`. `study.fetched_at` records the project-response retrieval time. A cache hit preserves the original time instead of replacing it with the audit time.

### Cache modes

| Mode | Fresh read | Live request | Cache write | Stale fallback after live failure |
| --- | --- | --- | --- | --- |
| Default | Yes | On cache miss | Successful live response | Yes |
| `--refresh` | No | Yes | Successful live response | Yes |
| `--no-cache` | No | Yes | No | No |

Project metadata and files are cached separately. PXAudit warns when their snapshot identifiers differ or when an older compatible entry has no snapshot identifier. The audit may still complete, but the warning records that the two responses cannot be proven to come from one retrieval.

> [!WARNING]
> If project metadata is unavailable with no stale fallback, the command fails. If the files response is unavailable with no stale fallback, PXAudit treats the audit as incomplete and does not compute, display, or persist a new score. Existing rows for that accession remain unchanged.

## `pxaudit bulk-audit`

Audit accessions from a UTF-8 text file or standard input:

```bash
pxaudit bulk-audit --input PATH [OPTIONS]
```

| Option | Default | Effect |
| --- | --- | --- |
| `--input PATH` | Required | One accession per line, or `-` for standard input |
| `--db PATH` | Config or `pxaudit_results.db` | SQLite output path |
| `--format FMT` | Config or unset | Export `tsv`, `csv`, or `json` |
| `--output PATH` | Dated filename | Export destination |
| `--delay SECONDS` | Config or `1.0` | Delay after an accession used the network |
| `--continue-on-error` | Off | Count and skip malformed or failed accessions |
| `--overwrite` | Off | Replace an existing regular export file |
| `--batch-size N` | `1` | Commit completed accessions after each batch of `N` |

Input format:

```text
# comments and blank lines are ignored
PXD000001
pxd004683

PXD073444
```

Case variants are deduplicated after canonicalization. Without `--continue-on-error`, a malformed input line reports its physical line number and exits with code 2 before auditing. API and incomplete-audit failures exit with code 1. With continuation enabled, both kinds are counted as failures and the valid accessions continue.

```bash
# Basic batch
pxaudit bulk-audit --input accessions.txt

# Export audit rows
pxaudit bulk-audit \
  --input accessions.txt \
  --format tsv \
  --output audit.tsv

# Read from a pipeline and continue past failures
printf 'PXD000001\nPXD004683\n' | \
  pxaudit bulk-audit --input - --continue-on-error
```

The normal end block is compact and keeps progress counts separate from the tier distribution:

```text
Batch audit complete (<elapsed>s)
  Total     : 3
  Completed : 3
  Failed    : 0
    Gold         2
    Diamond      1
```

With `-q`, the end block becomes one machine-oriented line such as `bulk-audit  total=3  completed=3  failed=0`. Warnings and malformed-input details remain on standard error so a redirected export stays usable.

The inter-accession delay runs only after network use. Fresh two-endpoint cache hits do not incur it. On a TTY, the command displays a progress bar unless quiet mode is active. Interruption exits with code 130 after preserving completed database rows and attempting any requested partial export.

TSV, CSV, and JSON exports serialize every `has_*` outcome as the string `passed`, `failed`, or `unknown`. They also include `ambiguity_count` and `tier_logic_version`; consumers must not parse evidence columns as integer booleans.

Export paths are not silently replaced. Without `--overwrite`, an existing file is an input error. Symbolic links and non-file targets are refused.

## `pxaudit summary`

Print an aggregate snapshot from an existing audit database:

```bash
pxaudit summary --db pxaudit_results.db
pxaudit -q summary --db pxaudit_results.db
```

The default output has five sections: a header with the database path, accession counts, and `tier_logic_version`; FAIR tier counts; quantification tier counts; the six largest failed or unknown metadata gaps; and a footer pointing to the HTML report. FAIR counts cover verifiable rows, while the quantification section includes `Unverifiable` rows separately. The command queries audit aggregates only and does not scan `study_files`.

```text
PXAudit summary  results.db  (tier_logic v3.0)
  accessions  128   verifiable  120   unverifiable  8

FAIR tiers
  Diamond     4
  Platinum   11
  Gold       18
  Silver     31
  Bronze     27
  Raw        22
  None        7

Quant tiers
  Quant-Complete  9
  Quant-Ready    14
  Partial        41
  No Quant       56
  Unverifiable    8

Top gaps (failed / unknown)
  has_sdrf             failed   64   unknown    3
  has_tabular_quant    failed   51   unknown    0
  has_organism_part    failed   38   unknown    7
  has_publication      failed   29   unknown    1
  has_open_spectra     failed   22   unknown    0
  has_psi_results       failed   18   unknown    2

HTML report: pxaudit report --db results.db
```

Quiet mode emits one stable line for scripts and does not emit ANSI styling:

```text
summary 128 accessions verifiable=120 unverifiable=8 tier_logic=v3.0 diamond=4 platinum=11 gold=18 silver=31 bronze=27 raw=22 none=7 quant_complete=9 quant_ready=14 quant_partial=41 quant_no_quant=56 quant_unverifiable=8 quant_unknown=0
```

An empty valid database exits `0` with zero counts. A missing database path exits `2`; an unreadable or schema-incompatible database exits `1`. Legacy databases are opened through the normal migration path when possible.

## `pxaudit manifest`

Print the stored files for an accession:

```bash
pxaudit manifest ACCESSION [--db PATH] [--format tsv|json]
```

The default format is TSV. Each record contains:

```text
file_name
file_category
file_extension
ftp_location
file_size
checksum
checksum_type
```

Examples:

```bash
pxaudit manifest PXD004683
pxaudit manifest PXD004683 --format json --db cohort.db
pxaudit manifest PXD004683 > manifest.tsv
```

> [!NOTE]
> `manifest` opens an existing database read-only. It does not create a missing database or run migrations. Status and warning messages go to standard error, so redirected TSV or JSON output remains clean.

## `pxaudit report`

Generate a self-contained HTML report from a populated database:

```bash
pxaudit report --db PATH [OPTIONS]
```

- `--db PATH` selects the required existing SQLite database.
- `--output DIR` selects the directory for `report.html`; the default is the current directory.
- `--title TEXT` changes the page heading from the default `PXAudit Report`.
- `--overwrite` replaces an existing regular `report.html`.

Install the optional report dependencies from a source checkout:

```bash
uv sync --extra report
```

Then generate the report:

```bash
pxaudit report --db pxaudit_results.db
pxaudit report --db cohort.db --output report/ --title "PRIDE Cohort"
pxaudit report --db cohort.db --output report/ --overwrite
```

The input database is opened read-only. A missing path is not created and migrations do not run. The output directory may already exist; without `--overwrite`, only an existing `report.html` causes refusal. Symbolic-link and non-file report targets are refused.

The report contains summary counts, qualitative and quantitative distributions, confirmed metadata gaps, separate unknown counts, the ten largest organism and instrument cohorts, tier definitions, and a quality-sorted accession table. It normalizes both v2 integer flags and v3 text outcomes in read-only mode. Unknown evidence is shown as `?`, not present or absent. The accession table is static, not searchable or interactively sortable.

## Cache maintenance

Inspect the configured cache:

```bash
pxaudit cache info
```

The command prints the resolved directory, validated entry count, ignored entry count, total bytes, and oldest and newest modification times. A maintenance-owned entry must be a regular version-2 JSON envelope whose owner, accession, endpoint, payload shape, and filename agree.

Remove those same validated entries:

```bash
pxaudit cache clear
pxaudit cache clear --yes
```

> [!CAUTION]
> `--yes` skips confirmation only. It does not skip path or ownership checks. PXAudit refuses broad locations such as a filesystem root, the home directory, the working directory, or the system temporary directory.

Unrelated files, directories, symbolic links, temporary files, corrupt JSON, legacy payloads, and entries with mismatched identity are ignored rather than deleted.

## Exit codes

- `0`: success, including an empty bulk input.
- `1`: operational API, cache, database, export, report, encoding, or filesystem failure.
- `2`: invalid input, unsafe path, missing required path, or conflicting destination.
- `130`: interrupted by the user.

Warnings do not necessarily imply failure. Stale fallback and mixed-snapshot warnings may accompany a successful audit because the available evidence was usable but its provenance was not ideal.

## Complete workflow

```bash
# Audit a cohort and export its audit rows
pxaudit bulk-audit \
  --input accessions.txt \
  --format tsv \
  --output audit.tsv

# Inspect one stored file inventory
pxaudit manifest PXD004683 --format json > PXD004683-files.json

# Generate a report from the same database
pxaudit report \
  --db pxaudit_results.db \
  --output report/ \
  --title "PRIDE Audit"
```

See [[Tier System]] to interpret the scores and [[Database Schema]] to query the stored evidence directly.
