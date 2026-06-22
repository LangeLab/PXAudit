# Database Schema

## Tables

### `study`

One row per accession.

| Column            | Type             | Description                         |
| ----------------- | ---------------- | ----------------------------------- |
| `accession`       | TEXT PRIMARY KEY | PRIDE accession (e.g. PXD000001)    |
| `title`           | TEXT             | Dataset title                       |
| `organism`        | TEXT             | Organism name                       |
| `organism_id`     | TEXT             | Taxonomy accession (e.g. NEWT:9606) |
| `instrument`      | TEXT             | Instrument name                     |
| `submission_year` | INTEGER          | Year of submission                  |
| `submission_type` | TEXT             | COMPLETE or PARTIAL                 |
| `keywords`        | TEXT             | Comma-separated keywords            |
| `repository`      | TEXT             | Source repository (always "PRIDE")  |
| `fetched_at`      | TEXT             | ISO 8601 fetch timestamp            |

### `study_files`

One row per file.

| Column           | Type          | Description                           |
| ---------------- | ------------- | ------------------------------------- |
| `accession`      | TEXT          | FK to study.accession                 |
| `file_name`      | TEXT NOT NULL | File name                             |
| `file_category`  | TEXT          | PRIDE fileCategory value              |
| `file_extension` | TEXT          | File extension                        |
| `ftp_location`   | TEXT          | FTP download URL                      |
| `file_size`      | INTEGER       | File size in bytes                    |
| `checksum`       | TEXT          | MD5 checksum when PRIDE provides one  |
| `checksum_type`  | TEXT          | Always "MD5" when checksum is present |

### `audit`

One row per accession.

| Column               | Type             | Description                                     |
| -------------------- | ---------------- | ----------------------------------------------- |
| `accession`          | TEXT PRIMARY KEY | FK to study.accession                           |
| `tier`               | TEXT             | Computed FAIR tier (None through Diamond)       |
| `quant_tier`         | TEXT             | Computed quant tier                             |
| `has_title`          | INTEGER          | Non-empty title present                         |
| `has_organism`       | INTEGER          | Organism name present                           |
| `has_organism_id`    | INTEGER          | Taxonomy ID present (recorded, not tier-gating) |
| `has_instrument`     | INTEGER          | Instrument name present                         |
| `has_result_files`   | INTEGER          | Processed result files found                    |
| `has_psi_results`    | INTEGER          | PSI-standard results (mzIdentML / mzTab)        |
| `has_open_spectra`   | INTEGER          | Open-format spectra (mzML / MGF)                |
| `has_organism_part`  | INTEGER          | Tissue or cell type annotated                   |
| `has_publication`    | INTEGER          | PubMed ID linked and non-zero                   |
| `has_tabular_quant`  | INTEGER          | Quant table present (QUANT_MATRIX or ID_LIST)   |
| `has_quant_metadata` | INTEGER          | Quantification methods described via CV terms   |
| `has_sdrf`           | INTEGER          | SDRF experimental-design file present           |
| `has_mztab`          | INTEGER          | mzTab summary present                           |
| `files_fetch_failed` | INTEGER          | File endpoint returned an error                 |
| `is_unverifiable`    | INTEGER          | Non-PRIDE accession that could not be checked   |
| `tier_logic_version` | TEXT             | Version of tier logic used to score             |

## Example queries

```sql
-- Tier distribution
SELECT tier, COUNT(*) AS n FROM audit GROUP BY tier ORDER BY n DESC;

-- All Diamond datasets
SELECT accession, quant_tier FROM audit WHERE tier = 'Diamond';

-- Datasets needing re-scoring after a logic update
SELECT accession FROM audit WHERE tier_logic_version != 'v2.0';

-- File breakdown for a single accession
SELECT file_category, COUNT(*) AS n
FROM study_files
WHERE accession = 'PXD004683'
GROUP BY file_category;

-- Studies with files but no SDRF (potential Silver tier)
SELECT s.accession, s.title
FROM study s
JOIN audit a ON s.accession = a.accession
WHERE a.has_sdrf = 0 AND a.has_result_files = 1;
```

## Migrations

These functions upgrade databases from earlier schema versions. All are idempotent and safe to run multiple times:

- `migrate_audit_v2(conn)`: adds v2 flag columns and `quant_tier` to `audit`, and `submission_type` to `study`
- `migrate_study_v2(conn)`: adds `fetched_at` to `study`
- `migrate_study_files_v2(conn)`: adds `checksum` and `checksum_type` to `study_files`
