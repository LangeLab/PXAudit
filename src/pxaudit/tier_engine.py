"""Boolean flag tier evaluator for pxaudit.

Public API
----------
compute_audit(accession, project_data, files_data, *, files_fetch_failed)
    -> AuditResult

Flag computation uses two strategies:
- Project-level flags are derived directly from the ``project_data`` dict.
- File-level classes come from ``FileTypeClassifier``. Narrow PSI-identification
  and mzTab flags use exact supported filename suffixes after compression removal.

The tier derivation mirrors the SQL CASE expression in
the project wiki Database Schema page exactly.

The ``None`` tier applies when mandatory fields (title, organism, or
instrument) are missing. For live PRIDE accessions, these fields are
enforced at submission time so the ``None`` branch is not expected to
trigger in practice, though it is exercised by synthetic test payloads.
"""

from __future__ import annotations

from dataclasses import dataclass

from pxaudit import _PRIDE_PREFIX
from pxaudit.accession import normalize_accession
from pxaudit.file_classifier import FileClass, FileTypeClassifier, strip_compression

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_TIER_LOGIC_VERSION: str = "v2.1"
_PSI_RESULT_EXTENSIONS: tuple[str, ...] = (".mzid", ".mzidentml", ".mztab")

# Module-level classifier instance: stateless after construction, safe to share.
_classifier: FileTypeClassifier = FileTypeClassifier()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _safe_pubmed_id(value: object) -> int:
    """Convert a positive PRIDE ``pubmedID`` field to int, returning 0 otherwise.

    PRIDE returns ``pubmedID`` as an integer or ``0`` for unpublished entries.
    Older API responses occasionally carry ``None`` or an empty string; this
    guard prevents malformed or non-positive values from becoming publication evidence.
    """
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.strip().isascii() and value.strip().isdecimal():
        parsed = int(value.strip())
    else:
        return 0
    return parsed if parsed > 0 else 0


def _nonblank_text(value: object) -> bool:
    """Return whether a value is text containing non-whitespace characters."""
    return isinstance(value, str) and bool(value.strip())


def _has_cv_quant_method(methods: object) -> bool:
    """Return whether quantification methods contain a usable CV name or accession."""
    if not isinstance(methods, list):
        return False
    return any(
        isinstance(method, dict)
        and any(
            isinstance(value := method.get(field), str) and bool(value.strip())
            for field in ("name", "accession")
        )
        for method in methods
    )


def _is_psi_result(filename: str) -> bool:
    """Return whether a filename establishes supported PSI identification evidence."""
    base = strip_compression(filename).casefold()
    return base.endswith(_PSI_RESULT_EXTENSIONS)


# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------


@dataclass
class AuditResult:
    """One audit row, ready for :func:`pxaudit.db.insert_audit` via ``asdict()``.

    Boolean flags map directly to the ``audit`` table columns.  The DB layer
    stores them as SQLite integers (0/1); Python ``bool`` is a subclass of
    ``int`` so no explicit conversion is needed.

    Field names mirror ``pxaudit.db._AUDIT_COLS`` so the result can be passed
    through ``asdict()`` to the database layer.
    """

    # Identifying (required) fields
    accession: str
    tier: str
    # Metadata flags
    has_title: bool = False
    has_organism: bool = False
    has_organism_id: bool = False
    has_instrument: bool = False
    has_result_files: bool = False
    # File-level flags
    has_psi_results: bool = False
    has_open_spectra: bool = False  # FileClass.PEAK found
    has_organism_part: bool = False  # meaningful organism-part name present
    has_publication: bool = False  # pubmedID present, non-null, != 0
    has_tabular_quant: bool = False
    has_quant_metadata: bool = False
    # Legacy flags
    has_sdrf: bool = False
    has_mztab: bool = False
    files_fetch_failed: bool = False
    is_unverifiable: bool = False
    tier_logic_version: str = _TIER_LOGIC_VERSION
    quant_tier: str = "No Quant"


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
        ``True`` to interpret the input as a historical failed files fetch.
        All file-based flags are set to ``False``, so the tier cannot exceed
        ``Raw`` and remains ``None`` when mandatory metadata is absent.
        Current CLI audits do not compute or persist an audit when the files
        response is unavailable and no stale response can be used.

    Returns
    -------
    AuditResult

    Raises
    ------
    InvalidAccessionError
        If ``accession`` does not satisfy the PXAudit accession grammar.

    """
    accession = normalize_accession(accession)

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
            quant_tier="Unverifiable",
        )

    # ------------------------------------------------------------------
    # 3.  Normalise inputs
    # ------------------------------------------------------------------
    project_data = project_data or {}
    files_data = files_data or []

    # ------------------------------------------------------------------
    # 4.  Project-level flags
    # ------------------------------------------------------------------
    has_title = _nonblank_text(project_data.get("title"))

    organisms: list[dict] = project_data.get("organisms") or []
    has_organism = bool(organisms and _nonblank_text(organisms[0].get("name")))
    has_organism_id = bool(organisms and _nonblank_text(organisms[0].get("accession")))

    instruments: list[dict] = project_data.get("instruments") or []
    has_instrument = bool(instruments and _nonblank_text(instruments[0].get("name")))

    submission_type: str = project_data.get("submissionType") or ""

    organism_parts: list = project_data.get("organismParts") or []
    references: list = project_data.get("references") or []
    quant_methods: object = project_data.get("quantificationMethods") or []

    has_organism_part = any(
        isinstance(part, dict) and _nonblank_text(part.get("name")) for part in organism_parts
    )
    has_quant_metadata = _has_cv_quant_method(quant_methods)
    has_publication = any(_safe_pubmed_id(r.get("pubmedID")) != 0 for r in references)

    # ------------------------------------------------------------------
    # 5.  File-level flags
    # ------------------------------------------------------------------
    has_psi_results = False
    has_open_spectra = False
    has_tabular_quant = False

    if files_fetch_failed or not files_data:
        has_result_files = False
        has_sdrf = False
        has_mztab = False
    else:
        file_names = [f.get("fileName") or "" for f in files_data]
        file_classes: set[FileClass] = {
            _classifier.classify(
                f.get("fileName") or "",
                (f.get("fileCategory") or {}).get("value"),
            )
            for f in files_data
        }

        has_psi_results = any(_is_psi_result(filename) for filename in file_names)
        has_open_spectra = FileClass.PEAK in file_classes
        has_tabular_quant = FileClass.QUANT_MATRIX in file_classes

        # Submission-type-aware result gate:
        # PARTIAL submissions may lack PSI-standard result files; a processed table
        # (QUANT_MATRIX or ID_LIST) is accepted as evidence of processed results.
        if submission_type.upper() == "PARTIAL":
            result_gate: frozenset[FileClass] = frozenset(
                {FileClass.RESULT, FileClass.SEARCH, FileClass.QUANT_MATRIX, FileClass.ID_LIST}
            )
        else:
            result_gate = frozenset({FileClass.RESULT, FileClass.SEARCH})
        has_result_files = bool(file_classes & result_gate)

        has_sdrf = FileClass.SDRF in file_classes
        has_mztab = any(
            strip_compression(filename).casefold().endswith(".mztab") for filename in file_names
        )

    # ------------------------------------------------------------------
    # 6.  Tier derivation  (mirrors SQL CASE in the wiki Database Schema page)
    # ------------------------------------------------------------------
    # 7-tier FAIR ladder.  Each tier adds one more requirement:
    #   None     : missing basic metadata (title / organism / instrument)
    #   Raw      : has metadata but no processed result files at all
    #   Bronze   : has result files but none are PSI-standard (mzIdentML / mzTab)
    #   Silver   : PSI results present but no SDRF experimental-design file
    #   Gold     : SDRF present but missing open spectra OR organism part annotation
    #   Platinum : open spectra + organism part present but no linked publication
    #   Diamond  : all FAIR criteria met
    if not has_title or not has_organism or not has_instrument:
        tier = "None"
    elif not has_result_files:
        tier = "Raw"
    elif not has_psi_results:
        tier = "Bronze"
    elif not has_sdrf:
        tier = "Silver"
    elif not has_open_spectra or not has_organism_part:
        tier = "Gold"
    elif not has_publication:
        tier = "Platinum"
    else:
        tier = "Diamond"

    # ------------------------------------------------------------------
    # 7.  Quant-tier derivation (secondary scoring axis)
    # ------------------------------------------------------------------
    if not has_psi_results and not has_tabular_quant:
        quant_tier = "No Quant"
    elif not has_psi_results and has_tabular_quant:
        quant_tier = "Partial"  # tool-native tables only, no PSI standard
    elif has_psi_results and not has_tabular_quant:
        quant_tier = "Partial"  # PSI IDs present but no quant table
    elif not has_quant_metadata:
        quant_tier = "Quant-Ready"  # PSI + quant table but metadata missing
    else:
        quant_tier = "Quant-Complete"  # PSI + quant table + metadata

    return AuditResult(
        accession=accession,
        tier=tier,
        has_title=has_title,
        has_organism=has_organism,
        has_organism_id=has_organism_id,
        has_instrument=has_instrument,
        has_result_files=has_result_files,
        has_psi_results=has_psi_results,
        has_open_spectra=has_open_spectra,
        has_organism_part=has_organism_part,
        has_publication=has_publication,
        has_tabular_quant=has_tabular_quant,
        has_quant_metadata=has_quant_metadata,
        has_sdrf=has_sdrf,
        has_mztab=has_mztab,
        files_fetch_failed=files_fetch_failed,
        is_unverifiable=False,
        quant_tier=quant_tier,
    )


__all__ = [
    "AuditResult",
    "compute_audit",
]
