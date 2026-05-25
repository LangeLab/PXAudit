# Tier System

## FAIR ladder

PXAudit scores each dataset on a 7-tier FAIR ladder. Every tier adds one requirement to the previous; a dataset must meet all criteria up to and including the tier it claims.

| Tier         | Requirements                                                                       |
| ------------ | ---------------------------------------------------------------------------------- |
| **None**     | Missing a mandatory metadata field (title, organism, or instrument)                |
| **Raw**      | Mandatory metadata present; no processed result files found                        |
| **Bronze**   | Result/search files present, but none are PSI-standard (mzIdentML / mzTab)         |
| **Silver**   | PSI-standard results present; no SDRF experimental-design file                     |
| **Gold**     | SDRF present; open spectra (mzML / MGF) **or** organism-part annotation missing    |
| **Platinum** | Open spectra + organism-part annotation present; no linked PubMed publication      |
| **Diamond**  | All FAIR criteria met: PSI results, SDRF, open spectra, organism part, publication |

Tier logic is version-stamped (`tier_logic_version = "v2.0"`) and stored in the database so you can tell when re-scoring is needed after a logic update.

## Quant tier

The quant tier is independent of the FAIR ladder. It measures how ready a dataset is for quantitative analysis.

| Quant Tier         | Meaning                                                         |
| ------------------ | --------------------------------------------------------------- |
| **Unverifiable**   | Non-PRIDE accession; cannot be evaluated                        |
| **No Quant**       | No PSI-standard results and no tabular quant files              |
| **Partial**        | Either PSI-standard IDs **or** a quant table, but not both      |
| **Quant-Ready**    | PSI IDs + tabular quant table present; CV-term metadata missing |
| **Quant-Complete** | PSI IDs + tabular quant table + CV-term method metadata         |

## Non-PRIDE accessions

Accessions from MassIVE (`MSV`), jPOST (`JPST`), and iProX (`IPX`) are accepted but get `Unverifiable` on both scores with all flags set to `False`. PXAudit only talks to the PRIDE API, so it can't inspect data from other repositories.

## Flag inventory

These Boolean flags are computed for every PRIDE accession:

- **Project-level flags**
    - `has_title`: non-empty title present
    - `has_organism`: organism name present (first entry in the organisms list)
    - `has_organism_id`: taxonomy accession present (recorded but not used in tier gating)
    - `has_instrument`: instrument name present (first entry)
    - `has_organism_part`: tissue or cell type annotated
    - `has_publication`: PubMed ID linked and non-zero
    - `has_quant_metadata`: quantification methods described via CV terms

- **File-level flags**
    - `has_result_files`: FileClass RESULT or SEARCH files found
    - `has_psi_results`: FileClass RESULT (mzIdentML or mzTab)
    - `has_open_spectra`: FileClass PEAK (mzML, MGF, etc.)
    - `has_sdrf`: SDRF experimental-design file detected (PRIDE category + filename regex)
    - `has_mztab`: `.mzTab` extension found
    - `has_tabular_quant`: FileClass QUANT_MATRIX or ID_LIST found
