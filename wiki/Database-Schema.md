# Database Schema

PXAudit stores completed audits in SQLite. The default path is `pxaudit_results.db`, and `--db PATH` or the `db_path` configuration key can select another file.

The database is meant to remain useful outside PXAudit. Tables use ordinary SQLite types, evidence stays in named columns, and the examples below work with the `sqlite3` command-line client or any SQLite library.

## Data model

```text
study (one row per accession)
  |
  +-- study_files (zero or more files, foreign key to study.accession)

audit (one score row per accession, independent primary key in schema v2)
```

`study_files.accession` has an index and a foreign-key reference to `study.accession`. `audit.accession` is an independent primary key in schema v2. Adding its foreign key requires a table rebuild and is deferred to a future schema migration.

## Write and read behavior

A completed audit replaces its `study`, `study_files`, and `audit` records in one explicit transaction.

> [!IMPORTANT]
> Either all three changes commit or all three roll back. PXAudit never treats a partly written audit as complete.

If project or file evidence is unavailable and no stale cache response can be used, PXAudit does not write a partial audit. Existing rows for that accession remain unchanged. This is why a transport failure does not appear later as confirmed missing scientific evidence.

Commands that audit data open the database in write mode, enable foreign keys, use WAL journaling, create missing tables, and run idempotent v2 migrations. `manifest` and `report` open an existing regular file read-only with SQLite query-only enforcement. They do not create a missing file or apply migrations.

## `study`

One row per canonical accession.

| Column | SQLite type | Meaning |
| --- | --- | --- |
| `accession` | `TEXT NOT NULL PRIMARY KEY` | Canonical uppercase audit identifier |
| `title` | `TEXT` | Project title |
| `organism` | `TEXT` | First organism name returned by PRIDE |
| `organism_id` | `TEXT` | First organism taxonomy accession, such as `NEWT:9606` |
| `instrument` | `TEXT` | First instrument name returned by PRIDE |
| `submission_year` | `INTEGER` | Year parsed from the submission date |
| `submission_type` | `TEXT` | PRIDE submission type, normally `COMPLETE` or `PARTIAL` |
| `keywords` | `TEXT` | Comma-separated project keywords |
| `repository` | `TEXT` | `PRIDE` for PXD, inferred partner name for recognized prefixes, otherwise NULL |
| `fetched_at` | `TEXT` | ISO 8601 project-response retrieval time |

> [!NOTE]
> `fetched_at` is not the time the audit command ran. A fresh cache hit retains the response's original retrieval time. Compatible older cache formats fall back to the cache file modification time and produce an unverified-snapshot warning.

## `study_files`

One row per deposited file returned by the files endpoint.

| Column | SQLite type | Meaning |
| --- | --- | --- |
| `accession` | `TEXT NOT NULL` | Foreign key to `study.accession` |
| `file_name` | `TEXT NOT NULL` | PRIDE file name |
| `file_category` | `TEXT` | PRIDE `fileCategory.value` |
| `file_extension` | `TEXT` | Final suffix recorded for the manifest |
| `ftp_location` | `TEXT` | Public file location when supplied |
| `file_size` | `INTEGER` | Size in bytes |
| `checksum` | `TEXT` | Checksum value when supplied |
| `checksum_type` | `TEXT` | MD5, SHA-1, or SHA-256 when defensible, otherwise NULL |

The table does not use a synthetic row identifier. Re-auditing an accession deletes its prior file rows and inserts the newly fetched inventory inside the same transaction as the study and audit update.

## `audit`

One row per scored accession.

| Column | SQLite type | Meaning |
| --- | --- | --- |
| `accession` | `TEXT NOT NULL PRIMARY KEY` | Canonical audit identifier |
| `tier` | `TEXT` | FAIR tier from None through Diamond, or Unverifiable |
| `quant_tier` | `TEXT` | No Quant, Partial, Quant-Ready, Quant-Complete, or Unverifiable |
| `has_title` | `INTEGER` | Non-empty title present |
| `has_organism` | `INTEGER` | First organism name present |
| `has_organism_id` | `INTEGER` | First taxonomy accession present; not tier-gating |
| `has_instrument` | `INTEGER` | First instrument name present |
| `has_result_files` | `INTEGER` | Processed result evidence present |
| `has_psi_results` | `INTEGER` | Supported PSI proteomics identification result present |
| `has_open_spectra` | `INTEGER` | Open-format spectra present |
| `has_organism_part` | `INTEGER` | Organism-part list present |
| `has_publication` | `INTEGER` | Parseable non-zero PubMed ID linked |
| `has_tabular_quant` | `INTEGER` | Recognized abundance summary or matrix present |
| `has_quant_metadata` | `INTEGER` | Usable quantification-method CV name or accession present |
| `has_sdrf` | `INTEGER` | SDRF experimental-design evidence present |
| `has_mztab` | `INTEGER` | Proteomics mzTab filename present |
| `files_fetch_failed` | `INTEGER` | Historical incomplete-fetch marker; 0.5.1 does not create new failed rows |
| `is_unverifiable` | `INTEGER` | Identifier belongs outside the currently queried PRIDE scope |
| `tier_logic_version` | `TEXT` | Scoring contract version; current value is `v2.1` |

See [[Tier System]] for the exact meaning and boundary of every evidence flag.

## Boolean and NULL values

SQLite stores Python booleans as integers:

- `1` means confirmed present.
- `0` means confirmed absent.
- `NULL` means unknown, unavailable, or absent from an older schema row.

Current completed audits write explicit Boolean values. NULL remains important for migrated or externally created databases. Reports preserve that distinction: unknown values are not counted as present or missing.

> [!TIP]
> Use `IS NULL` when querying unknown evidence. `= 0` means confirmed absence and does not match NULL.

```sql
SELECT accession
FROM audit
WHERE has_sdrf IS NULL;
```

## Useful queries

### Tier distribution

```sql
SELECT tier, COUNT(*) AS datasets
FROM audit
GROUP BY tier
ORDER BY datasets DESC, tier;
```

### High-tier datasets with their titles

```sql
SELECT a.accession, s.title, a.tier, a.quant_tier
FROM audit AS a
LEFT JOIN study AS s USING (accession)
WHERE a.tier IN ('Gold', 'Platinum', 'Diamond')
ORDER BY
  CASE a.tier
    WHEN 'Diamond' THEN 1
    WHEN 'Platinum' THEN 2
    ELSE 3
  END,
  a.accession;
```

### Confirmed metadata gaps

```sql
SELECT
  SUM(has_organism_part = 0) AS missing_organism_part,
  SUM(has_publication = 0) AS missing_publication,
  SUM(has_quant_metadata = 0) AS missing_quant_metadata
FROM audit
WHERE is_unverifiable = 0;
```

### Unknown evidence separately

```sql
SELECT
  SUM(has_organism_part IS NULL) AS unknown_organism_part,
  SUM(has_publication IS NULL) AS unknown_publication,
  SUM(has_quant_metadata IS NULL) AS unknown_quant_metadata
FROM audit
WHERE is_unverifiable = 0;
```

### File categories for one accession

```sql
SELECT file_category, COUNT(*) AS files, SUM(file_size) AS bytes
FROM study_files
WHERE accession = 'PXD004683'
GROUP BY file_category
ORDER BY files DESC, file_category;
```

### Largest stored file inventories

```sql
SELECT accession, COUNT(*) AS files, SUM(file_size) AS bytes
FROM study_files
GROUP BY accession
ORDER BY files DESC
LIMIT 20;
```

### Rows that need re-scoring

```sql
SELECT accession, tier, tier_logic_version
FROM audit
WHERE tier_logic_version IS NULL
   OR tier_logic_version != 'v2.1'
ORDER BY accession;
```

## Export without changing the database

Use `manifest` for one accession's files:

```bash
pxaudit manifest PXD004683 --db pxaudit_results.db > files.tsv
```

Use SQLite directly for custom audit exports:

```bash
sqlite3 -header -csv pxaudit_results.db \
  "SELECT accession, tier, quant_tier FROM audit ORDER BY accession" \
  > tiers.csv
```

Both `manifest` and direct read-only queries leave the stored audit unchanged.

## Migrations

PXAudit carries idempotent migrations for schema-v1 databases:

- `migrate_audit_v2` adds the v2 evidence flags, `quant_tier`, and `study.submission_type` when absent.
- `migrate_study_v2` adds `study.fetched_at` when absent.
- `migrate_study_files_v2` adds `checksum` and `checksum_type` when absent.

> [!WARNING]
> Migrations add columns but do not invent evidence for old rows. Newly added values can remain NULL until the accession is audited again.

Migrations run when `check` or `bulk-audit` opens a writable database. Read-only commands do not migrate. If an old database must remain byte-for-byte unchanged, inspect a copy with SQLite rather than opening it through an audit command.
