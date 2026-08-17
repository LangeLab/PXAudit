# Tier System

One score was not enough for what I wanted to see. A submission can have good identification evidence and still be difficult to quantify, or it can contain useful abundance tables without enough metadata to understand them. PXAudit keeps those questions separate.

Every completed PRIDE audit therefore produces two scores:

- **Tier** measures progression through a seven-level FAIR evidence ladder.
- **Quant Tier** measures whether identification evidence, abundance tables, and method metadata are available together.

Both scores come from stored evidence flags. They are deterministic for the same project and file responses.

## FAIR ladder

The ladder stops at the first confirmed failed gate.

> [!IMPORTANT]
> Later evidence cannot skip an earlier failed requirement. An `unknown` outcome does not block progression, but it increments `ambiguity_count` so an apparently high tier is visibly uncertain.

| Tier         | Gate that stops the ladder                                                                | What would move it higher                    |
| ------------ | ----------------------------------------------------------------------------------------- | -------------------------------------------- |
| **None**     | Title, organism name, or instrument name is missing                                       | Complete all three mandatory metadata fields |
| **Raw**      | No processed result evidence is present                                                   | Deposit a recognized result or search output |
| **Bronze**   | Processed results exist, but no supported PSI identification result is present            | Deposit mzIdentML or proteomics mzTab        |
| **Silver**   | PSI identification results exist, but no SDRF is present                                  | Deposit an SDRF experimental-design table    |
| **Gold**     | SDRF exists, but open spectra or organism-part annotation is missing                      | Provide both open spectra and organism part  |
| **Platinum** | Open spectra and organism part exist, but no positive integer PubMed identifier is linked | Link a publication                           |
| **Diamond**  | Every FAIR gate is satisfied                                                              | Highest qualitative tier                     |

`has_organism_id` is recorded for analysis but does not gate the ladder. A missing taxonomy accession can coexist with a tier above None when the organism name is present.

### Worked ladder examples

These examples isolate the gate that determines each score. Other evidence may be present but cannot bypass that gate.

- **None:** the project has organism and instrument metadata, but its title is empty.
- **Raw:** mandatory metadata is complete, but the deposit contains only vendor `.raw` files.
- **Bronze:** `mascot.dat` establishes processed output, but no mzIdentML or mzTab is present.
- **Silver:** `results.mzid` and open spectra are present, but SDRF is missing.
- **Gold:** PSI results and SDRF are present, but organism-part annotation is missing.
- **Platinum:** PSI results, SDRF, open spectra, and organism part are present, but `pubmedID` is absent or `0`.
- **Diamond:** PSI results, SDRF, open spectra, organism part, and a positive integer PubMed ID are all present.

The ladder is versioned. PXAudit 0.5.4 writes `tier_logic_version = "v3.0"` into every new audit row. Re-auditing an accession applies current logic to current or cached evidence; it does not silently rewrite older rows in bulk.

## Three-valued outcomes

Every `has_*` evidence flag is `passed`, `failed`, or `unknown`:

- `passed`: usable evidence is present;
- `failed`: an explicit empty value or verified empty file response confirms absence; and
- `unknown`: the field is absent, malformed, structurally unusable, or unavailable.

Only `failed` outcomes block the qualitative or quant tier ladder. Unknown outcomes are optimistic by design and are counted in `audit.ambiguity_count`. The outcome itself does not retain a reason code, so users should inspect the source payload or provenance when an ambiguity matters.

## Quantification-readiness tier

The quant tier is independent of the FAIR ladder. It uses three flags:

- `has_psi_results`: supported PSI identification output is present;
- `has_tabular_quant`: a recognized abundance summary or matrix is present; and
- `has_quant_metadata`: at least one quantification-method CV entry has a nonblank name or accession.

| PSI results | Quant table | Usable method metadata | Quant Tier         |
| ----------- | ----------- | ---------------------- | ------------------ |
| No          | No          | Either                 | **No Quant**       |
| Yes         | No          | Either                 | **Partial**        |
| No          | Yes         | Either                 | **Partial**        |
| Yes         | Yes         | No                     | **Quant-Ready**    |
| Yes         | Yes         | Yes                    | **Quant-Complete** |

Method metadata alone does not raise the score. It matters only when both PSI identification evidence and a recognized abundance table are present.

The quant tier uses the same confirmed-failure rule. For example, if PSI evidence is `unknown` and the quant table is `failed`, the result is `Partial`; if both are `unknown`, neither `No Quant` nor `Partial` is asserted. An unavailable files response produces unknown file outcomes in the pure tier engine, while the CLI refuses to persist a newly incomplete audit.

Non-PRIDE identifiers receive `Unverifiable` on both axes because PXAudit has not queried the repository that owns them.

## Evidence flags

### Project metadata

- **`has_title`:** the project title contains at least one non-whitespace character.
- **`has_organism`:** the first organism entry has a nonblank name. Later entries do not repair an empty first entry.
- **`has_organism_id`:** the first organism entry has a nonblank taxonomy accession. This flag is recorded but does not gate the tier.
- **`has_instrument`:** the first instrument entry has a nonblank name. This is mandatory for leaving None.
- **`has_organism_part`:** at least one `organismParts` entry has a nonblank name. An empty mapping does not establish biological context.
- **`has_publication`:** `passed` when at least one reference has a positive integer `pubmedID`; `failed` for an explicit empty or zero-only reference set; `unknown` for malformed or unusable references.
- **`has_quant_metadata`:** at least one quantification method has a nonblank CV name or accession. A non-empty container alone is not enough.

### File evidence

- **`has_result_files`:** a recognized RESULT or SEARCH class is present. PARTIAL submissions also accept QUANT_MATRIX and ID_LIST as processed evidence.
- **`has_psi_results`:** a filename ends in `.mzid`, `.mzidentml`, or proteomics `.mztab` after compression removal. A PRIDE `RESULT` category alone is not PSI proof.
- **`has_open_spectra`:** at least one PEAK file such as mzML, mzXML, MGF, MS2, or DTA is present. Vendor raw files do not count as open spectra.
- **`has_sdrf`:** an SDRF token appears with a tabular extension or the PRIDE experimental-design category. Similar words such as `sdrfile` are rejected.
- **`has_mztab`:** a filename ends in proteomics `.mztab` after compression removal. This is stored separately from broader PSI evidence.
- **`has_tabular_quant`:** a recognized abundance summary or matrix is present. PSM, scan, and evidence lists are not quant tables.

## File classification

PRIDE categories are useful, but they are not always scientifically specific enough for scoring. PXAudit classifies filenames in a fixed order:

1. configured compound and ordinary extensions;
2. exact known stems;
3. SDRF token and category rules;
4. processed-result basename patterns;
5. quantitative matrix patterns;
6. identification-list patterns;
7. trusted PRIDE category fallback; and
8. `OTHER` when nothing matches.

Compression wrappers such as `.gz`, `.zip`, `.bz2`, `.7z`, and `.xz` are removed layer by layer before the underlying format is evaluated. `results.mzid.gz` therefore supplies the same PSI evidence as `results.mzid`.

### Classification examples

| Filename and PRIDE category      | File class   | Audit meaning                                           |
| -------------------------------- | ------------ | ------------------------------------------------------- |
| `run.raw`, `RAW`                 | RAW          | Vendor raw spectra only                                 |
| `run.mzML.gz`, `OTHER`           | PEAK         | Open spectra present                                    |
| `results.mzid`, `OTHER`          | RESULT       | Processed and PSI identification evidence               |
| `results.csv`, `RESULT`          | RESULT       | Processed evidence, but not PSI identification evidence |
| `mascot.dat`, `SEARCH`           | SEARCH       | Processed proprietary search output                     |
| `proteinGroups.txt`, `OTHER`     | QUANT_MATRIX | Recognized abundance summary                            |
| `evidence.txt`, `OTHER`          | ID_LIST      | Identification list, not a quant summary                |
| `study.sdrf.tsv.gz`, `OTHER`     | SDRF         | Experimental-design evidence                            |
| `quality.mzQC`, `RESULT`         | OTHER        | Quality-control file, not identification evidence       |
| `metabolomics.mztab-m`, `RESULT` | OTHER        | mzTab-M is outside the proteomics PSI gate              |

> [!NOTE]
> The broad RESULT class and the narrow `has_psi_results` flag deliberately answer different questions. The first asks whether processed output exists. The second asks whether a supported PSI proteomics identification format exists.

## PARTIAL submissions

PRIDE `PARTIAL` submissions may provide processed tables without a conventional result or search file. For these submissions only, QUANT_MATRIX and ID_LIST files can satisfy `has_result_files`.

> [!NOTE]
> The PARTIAL gate changes only what counts as processed-result presence. It does not change the meaning of PSI or quantitative evidence.

This relaxation does not change the meaning of the other flags:

- an ID list still does not become PSI-standard evidence;
- an ID list still does not become a quantification matrix; and
- a quantitative matrix still requires PSI results before the quant tier can exceed Partial.

## Dated live examples

The explicit live integration suite checked these PRIDE profiles on 2026-07-18 UTC. They are examples of observed live behavior on that date, not permanent promises about mutable remote records.

| Accession   | FAIR Tier | Quant Tier | Determining evidence                                |
| ----------- | --------- | ---------- | --------------------------------------------------- |
| `PXD057701` | Raw       | No Quant   | No processed result evidence                        |
| `PXD002244` | Bronze    | No Quant   | Processed evidence without supported PSI results    |
| `PXD000001` | Silver    | Partial    | PSI results present, SDRF absent                    |
| `PXD073444` | Platinum  | Partial    | All gates through organism part, publication absent |
| `PXD075811` | Platinum  | Partial    | All gates through organism part, publication absent |
| `PXD004683` | Diamond   | Partial    | Every FAIR gate present; no recognized quant matrix |

## Re-score older rows

Find rows produced by another tier-logic version:

```sql
SELECT accession, tier, tier_logic_version, ambiguity_count
FROM audit
WHERE tier_logic_version IS NULL
   OR tier_logic_version != 'v3.0';
```

Then audit those accessions again. Use `--refresh` when the re-score should use current PRIDE responses rather than fresh cached responses:

```bash
pxaudit check PXD000001 --refresh
```

See [[Database Schema]] for stored columns and [[CLI Reference]] for cache provenance and failure behavior.
