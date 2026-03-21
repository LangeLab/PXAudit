"""Boolean flag tier evaluator for pxaudit.

Public API
----------
compute_audit(accession, project_data, files_data, *, files_fetch_failed)
    → AuditResult

All flag computation is vectorized (pandas).  No row-level Python loops over
the files list.  The tier derivation mirrors the SQL CASE expression in
plan/database_schema.md exactly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd

from pxaudit import __version__

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_TIER_LOGIC_VERSION: str = f"v{__version__}"  # → "v0.1.0"

# Only PXD accessions are hosted by PRIDE and can be fully audited.
_PRIDE_PREFIX = "PXD"

# fileCategory.value strings that count as result/search evidence.
# Source: PRIDE CV — "Result file URI" → value "RESULT",
#         "Search engine output file URI" → value "SEARCH".
_RESULT_CATEGORIES: frozenset[str] = frozenset({"result", "search"})

# ---------------------------------------------------------------------------
# SDRF detection
# ---------------------------------------------------------------------------

# Primary path: PRIDE applies this category to all SDRF files in well-annotated
# submissions.  We require the filename to ALSO contain "sdrf" because the category
# can be applied to any experimental-design document (Excel, plain text) that is
# not an SDRF.
_SDRF_CATEGORY: str = "experimental design"

# Fallback path: pre-category-era submissions where the SDRF exists but was not
# tagged with the EXPERIMENTAL DESIGN category.
#
# Two requirements to avoid false positives:
#   1. The word-boundary lookbehind/lookahead (?<![a-zA-Z])sdrf(?![a-zA-Z]) ensures
#      that "sdrfile.txt", "asdrf.tsv", "prefixsdrfsuffix" are NOT matched.  Note:
#      underscores and digits are NOT letters, so "_sdrf_.tsv" and "123sdrf456.tsv"
#      still match — that is intentional and matches the token-boundary test suite.
#   2. The tabular extension guard \.(tsv|txt|csv) ensures "sdrf_instructions.pdf"
#      and "sdrf_template.docx" are NOT matched.  An SDRF must be a tab/comma-
#      delimited text file; the extension is the authoritative discriminator.
#   3. An optional compression suffix allows "PXD073444.sdrf.tsv.gz" to match.
#
# Spec note: the original draft proposed r"sdrf.*\.(tsv|txt|csv)...", which lacks
# the word-boundary guard and regresses sdrfile.txt / asdrf.tsv / sdrfdata.tsv.
_SDRF_FALLBACK_RE: re.Pattern[str] = re.compile(
    r"(?<![a-zA-Z])sdrf(?![a-zA-Z]).*\.(?:tsv|txt|csv)(?:\.(?:gz|zip|bz2|7z))?$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------


@dataclass
class AuditResult:
    """One audit row, ready for :func:`pxaudit.db.insert_audit` via ``asdict()``.

    Boolean flags map directly to the ``audit`` table columns.  The DB layer
    stores them as SQLite integers (0/1); Python ``bool`` is a subclass of
    ``int`` so no explicit conversion is needed.

    Field order must match ``pxaudit.db._AUDIT_COLS`` exactly — the
    schema-contract test ``test_audit_result_field_names_match_audit_cols_exactly``
    enforces this.
    """

    # ── Identifying (required) fields ─────────────────────────────────────────
    accession: str
    tier: str
    # ── Existing metadata flags ────────────────────────────────────────────────
    has_title: bool = False
    has_organism: bool = False
    has_organism_id: bool = False
    has_instrument: bool = False
    has_result_files: bool = False
    # ── v2 flags (C03 / C06) ──────────────────────────────────────────────────
    has_psi_results: bool = False  # FileClass.RESULT found (mzIdentML / mzTab)
    has_open_spectra: bool = False  # FileClass.PEAK found
    has_organism_part: bool = False  # len(project["organismParts"]) > 0
    has_publication: bool = False  # pubmedID present, non-null, != 0
    has_tabular_quant: bool = False  # FileClass.QUANT_MATRIX or ID_LIST found
    has_quant_metadata: bool = False  # quantificationMethods[] non-empty
    # ── Legacy flags (kept for backward compat) ───────────────────────────────
    has_sdrf: bool = False
    has_mztab: bool = False
    files_fetch_failed: bool = False
    is_unverifiable: bool = False
    tier_logic_version: str = _TIER_LOGIC_VERSION


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def compute_audit(
    accession: str,
    project_data: dict,
    files_data: list[dict],
    *,
    files_fetch_failed: bool = False,
) -> AuditResult:
    """Compute tier and Boolean audit flags for a single PRIDE accession.

    Parameters
    ----------
    accession:
        Accession string, e.g. ``"PXD000001"``.
    project_data:
        Raw JSON dict from ``GET /projects/{accession}``.
    files_data:
        Raw JSON list from ``GET /projects/{accession}/files``.
        Pass ``[]`` when the endpoint returned no files.
    files_fetch_failed:
        ``True`` when the files endpoint failed after all retries.
        All file-based flags are set to ``False``; tier is capped at Bronze.

    Returns
    -------
    AuditResult

    Raises
    ------
    ValueError
        If *accession* is empty or does not begin with an alphabetic character
        (e.g. pure numeric strings).
    """
    # ------------------------------------------------------------------
    # 1.  Input validation
    # ------------------------------------------------------------------
    if not accession or not accession[0].isalpha():
        raise ValueError(f"Invalid accession: {accession!r}")

    # ------------------------------------------------------------------
    # 2.  Non-PRIDE short-circuit
    # ------------------------------------------------------------------
    if not accession.upper().startswith(_PRIDE_PREFIX):
        return AuditResult(
            accession=accession,
            tier="Unverifiable",
            has_title=False,
            has_organism=False,
            has_organism_id=False,
            has_instrument=False,
            has_result_files=False,
            has_sdrf=False,
            has_mztab=False,
            files_fetch_failed=files_fetch_failed,
            is_unverifiable=True,
        )

    # ------------------------------------------------------------------
    # 3.  Normalise inputs
    # ------------------------------------------------------------------
    project_data = project_data or {}
    files_data = files_data or []

    # ------------------------------------------------------------------
    # 4.  Project-level flags
    # ------------------------------------------------------------------
    has_title = bool(project_data.get("title"))

    organisms: list[dict] = project_data.get("organisms") or []
    has_organism = bool(organisms and organisms[0].get("name"))
    has_organism_id = bool(organisms and organisms[0].get("accession"))

    instruments: list[dict] = project_data.get("instruments") or []
    has_instrument = bool(instruments and instruments[0].get("name"))

    # ------------------------------------------------------------------
    # 5.  File-level flags (vectorized)
    # ------------------------------------------------------------------
    if files_fetch_failed or not files_data:
        has_result_files = False
        has_sdrf = False
        has_mztab = False
    else:
        # Build flat Series from nested CvParam structures — one pass only.
        file_names = pd.Series(
            [f.get("fileName") or "" for f in files_data],
            dtype="object",
        )
        file_cats = pd.Series(
            [(f.get("fileCategory") or {}).get("value") or "" for f in files_data],
            dtype="object",
        )

        has_result_files = bool(file_cats.str.casefold().isin(_RESULT_CATEGORIES).any())

        # Two-stage SDRF detection — see module-level constants for rationale.
        # Primary: authoritative EXPERIMENTAL DESIGN category + "sdrf" in filename.
        experimental_design_mask = file_cats.str.casefold() == _SDRF_CATEGORY
        primary_sdrf = bool(
            experimental_design_mask.any()
            and file_names[experimental_design_mask]
            .str.contains(r"sdrf", case=False, na=False)
            .any()
        )
        # Fallback: filename pattern only (for pre-category-era submissions).
        fallback_sdrf = bool(file_names.str.contains(_SDRF_FALLBACK_RE, na=False).any())
        has_sdrf = primary_sdrf or fallback_sdrf

        has_mztab = bool(file_names.str.casefold().str.endswith(".mztab").any())

    # ------------------------------------------------------------------
    # 6.  Tier derivation  (mirrors SQL CASE in plan/database_schema.md)
    # ------------------------------------------------------------------
    if not has_title or not has_organism or not has_instrument:
        tier = "None"
    elif not has_organism_id or not has_result_files:
        tier = "Bronze"
    elif not has_sdrf:
        tier = "Silver"
    else:
        tier = "Gold"

    return AuditResult(
        accession=accession,
        tier=tier,
        has_title=has_title,
        has_organism=has_organism,
        has_organism_id=has_organism_id,
        has_instrument=has_instrument,
        has_result_files=has_result_files,
        has_sdrf=has_sdrf,
        has_mztab=has_mztab,
        files_fetch_failed=files_fetch_failed,
        is_unverifiable=False,
    )
