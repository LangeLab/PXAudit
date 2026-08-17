"""Three-valued flag tier evaluator for pxaudit.

Public API
----------
compute_audit(accession, project_data, files_data, *, files_fetch_failed)
    -> AuditResult

Flag computation uses two strategies:
- Project-level flags are derived directly from the ``project_data`` dict.
- File-level classes come from ``FileTypeClassifier``. Narrow PSI-identification
  and mzTab flags use exact supported filename suffixes after compression removal.

Each evidence flag is :class:`FlagOutcome.PASSED`,
:class:`FlagOutcome.FAILED`, or :class:`FlagOutcome.UNKNOWN`. Missing API fields
and structurally unusable values are unknown; present empty values are failed.
Unknown outcomes do not stop either tier ladder, but are counted in
``ambiguity_count``.

The tier derivation mirrors the SQL CASE expression in
the project wiki Database Schema page exactly.

The ``None`` tier applies when mandatory fields (title, organism, or
instrument) are missing. For live PRIDE accessions, these fields are
enforced at submission time so the ``None`` branch is not expected to
trigger in practice, though it is exercised by synthetic test payloads.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from pxaudit import _PRIDE_PREFIX
from pxaudit.accession import normalize_accession
from pxaudit.file_classifier import FileClass, FileTypeClassifier, strip_compression

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_TIER_LOGIC_VERSION: str = "v3.0"
_PSI_RESULT_EXTENSIONS: tuple[str, ...] = (".mzid", ".mzidentml", ".mztab")
_FLAG_FIELDS: tuple[str, ...] = (
    "has_title",
    "has_organism",
    "has_organism_id",
    "has_instrument",
    "has_result_files",
    "has_psi_results",
    "has_open_spectra",
    "has_organism_part",
    "has_publication",
    "has_tabular_quant",
    "has_quant_metadata",
    "has_sdrf",
    "has_mztab",
)

# Module-level classifier instance: stateless after construction, safe to share.
_classifier: FileTypeClassifier = FileTypeClassifier()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


class FlagOutcome(StrEnum):
    """Outcome vocabulary for one derived audit evidence flag."""

    PASSED = "passed"
    FAILED = "failed"
    UNKNOWN = "unknown"


def _outcome_for_value(value: object, *, present: bool) -> FlagOutcome:
    """Classify one scalar API value while preserving absence as unknown."""
    if not present:
        return FlagOutcome.UNKNOWN
    if isinstance(value, str):
        return FlagOutcome.PASSED if value.strip() else FlagOutcome.FAILED
    if value is None:
        return FlagOutcome.FAILED
    return FlagOutcome.UNKNOWN


def _first_entry_outcome(project_data: dict, collection_name: str, field_name: str) -> FlagOutcome:
    """Classify a field on the first entry of an optional project collection."""
    if collection_name not in project_data:
        return FlagOutcome.UNKNOWN
    entries = project_data.get(collection_name)
    if entries is None or entries == []:
        return FlagOutcome.FAILED
    if not isinstance(entries, list) or not isinstance(entries[0], dict):
        return FlagOutcome.UNKNOWN
    entry = entries[0]
    return _outcome_for_value(entry.get(field_name), present=field_name in entry)


def _named_collection_outcome(
    project_data: dict, collection_name: str, field_name: str
) -> FlagOutcome:
    """Classify whether an optional collection contains a usable named entry."""
    if collection_name not in project_data:
        return FlagOutcome.UNKNOWN
    entries = project_data.get(collection_name)
    if entries is None or entries == []:
        return FlagOutcome.FAILED
    if not isinstance(entries, list):
        return FlagOutcome.UNKNOWN

    has_unknown = False
    for entry in entries:
        if not isinstance(entry, dict):
            has_unknown = True
            continue
        if (
            _outcome_for_value(entry.get(field_name), present=field_name in entry)
            is FlagOutcome.PASSED
        ):
            return FlagOutcome.PASSED
        if field_name not in entry or not isinstance(entry.get(field_name), str):
            has_unknown = True
    return FlagOutcome.UNKNOWN if has_unknown else FlagOutcome.FAILED


def _publication_outcome(project_data: dict) -> FlagOutcome:
    """Classify publication evidence while distinguishing malformed references."""
    if "references" not in project_data:
        return FlagOutcome.UNKNOWN
    references = project_data.get("references")
    if references is None or references == []:
        return FlagOutcome.FAILED
    if not isinstance(references, list):
        return FlagOutcome.UNKNOWN

    has_unknown = False
    for reference in references:
        if not isinstance(reference, dict) or "pubmedID" not in reference:
            has_unknown = True
            continue
        value = reference["pubmedID"]
        if isinstance(value, bool):
            has_unknown = True
            continue
        if isinstance(value, int):
            if value > 0:
                return FlagOutcome.PASSED
            continue
        if isinstance(value, str) and value.strip().isascii() and value.strip().isdecimal():
            if int(value.strip()) > 0:
                return FlagOutcome.PASSED
            continue
        has_unknown = True
    return FlagOutcome.UNKNOWN if has_unknown else FlagOutcome.FAILED


def _quant_metadata_outcome(project_data: dict) -> FlagOutcome:
    """Classify quantification-method metadata with an explicit unknown state."""
    if "quantificationMethods" not in project_data:
        return FlagOutcome.UNKNOWN
    methods = project_data.get("quantificationMethods")
    if methods is None or methods == []:
        return FlagOutcome.FAILED
    if not isinstance(methods, list):
        return FlagOutcome.UNKNOWN

    has_unknown = False
    for method in methods:
        if not isinstance(method, dict):
            has_unknown = True
            continue
        if any(
            isinstance(method.get(field), str) and bool(method[field].strip())
            for field in ("name", "accession")
        ):
            return FlagOutcome.PASSED
        if not all(
            field in method and isinstance(method[field], str) for field in ("name", "accession")
        ):
            has_unknown = True
    return FlagOutcome.UNKNOWN if has_unknown else FlagOutcome.FAILED


def _outcome_from_presence(passed: bool, uncertain: bool = False) -> FlagOutcome:
    """Return a flag outcome from positive and indeterminate evidence checks."""
    if passed:
        return FlagOutcome.PASSED
    return FlagOutcome.UNKNOWN if uncertain else FlagOutcome.FAILED


def _failed(value: FlagOutcome) -> bool:
    """Return whether an outcome is a confirmed failed gate."""
    return value is FlagOutcome.FAILED


def _unknown_count(values: dict[str, FlagOutcome]) -> int:
    """Count unknown evidence outcomes in the complete flag mapping."""
    return sum(values[name] is FlagOutcome.UNKNOWN for name in _FLAG_FIELDS)


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

    Evidence flags map directly to the ``audit`` table columns. The DB layer
    stores their :class:`FlagOutcome` values as SQLite text.

    Field names mirror ``pxaudit.db._AUDIT_COLS`` so the result can be passed
    through ``asdict()`` to the database layer.
    """

    # Identifying (required) fields
    accession: str
    tier: str
    # Metadata flags
    has_title: FlagOutcome = FlagOutcome.UNKNOWN
    has_organism: FlagOutcome = FlagOutcome.UNKNOWN
    has_organism_id: FlagOutcome = FlagOutcome.UNKNOWN
    has_instrument: FlagOutcome = FlagOutcome.UNKNOWN
    has_result_files: FlagOutcome = FlagOutcome.UNKNOWN
    # File-level flags
    has_psi_results: FlagOutcome = FlagOutcome.UNKNOWN
    has_open_spectra: FlagOutcome = FlagOutcome.UNKNOWN  # FileClass.PEAK found
    has_organism_part: FlagOutcome = FlagOutcome.UNKNOWN  # meaningful organism-part name present
    has_publication: FlagOutcome = FlagOutcome.UNKNOWN  # positive PubMed ID linked
    has_tabular_quant: FlagOutcome = FlagOutcome.UNKNOWN
    has_quant_metadata: FlagOutcome = FlagOutcome.UNKNOWN
    # Legacy flags
    has_sdrf: FlagOutcome = FlagOutcome.UNKNOWN
    has_mztab: FlagOutcome = FlagOutcome.UNKNOWN
    files_fetch_failed: bool = False
    is_unverifiable: bool = False
    ambiguity_count: int = 0
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
    """Compute tier and three-valued audit flags for a single PRIDE accession.

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
        ``True`` to interpret the input as an unavailable files fetch. File-based
        flags become ``unknown``. Current CLI audits do not compute or persist an
        audit when the files response is unavailable and no stale response can be used.

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
        unknown_flags = {name: FlagOutcome.UNKNOWN for name in _FLAG_FIELDS}
        return AuditResult(
            accession=accession,
            tier="Unverifiable",
            **unknown_flags,
            files_fetch_failed=files_fetch_failed,
            is_unverifiable=True,
            ambiguity_count=len(_FLAG_FIELDS),
            quant_tier="Unverifiable",
        )

    # ------------------------------------------------------------------
    # 3.  Normalise inputs
    # ------------------------------------------------------------------
    project_data = project_data if isinstance(project_data, dict) else {}

    # ------------------------------------------------------------------
    # 4.  Project-level flags
    # ------------------------------------------------------------------
    has_title = _outcome_for_value(project_data.get("title"), present="title" in project_data)
    has_organism = _first_entry_outcome(project_data, "organisms", "name")
    has_organism_id = _first_entry_outcome(project_data, "organisms", "accession")
    has_instrument = _first_entry_outcome(project_data, "instruments", "name")
    has_organism_part = _named_collection_outcome(project_data, "organismParts", "name")
    has_quant_metadata = _quant_metadata_outcome(project_data)
    has_publication = _publication_outcome(project_data)

    submission_type_value = project_data.get("submissionType")
    submission_type = (
        submission_type_value.strip().upper() if isinstance(submission_type_value, str) else None
    )

    # ------------------------------------------------------------------
    # 5.  File-level flags
    # ------------------------------------------------------------------
    has_psi_results = FlagOutcome.UNKNOWN
    has_open_spectra = FlagOutcome.UNKNOWN
    has_tabular_quant = FlagOutcome.UNKNOWN

    if files_fetch_failed:
        has_result_files = FlagOutcome.UNKNOWN
        has_sdrf = FlagOutcome.UNKNOWN
        has_mztab = FlagOutcome.UNKNOWN
    elif files_data is None or not isinstance(files_data, list):
        has_result_files = FlagOutcome.UNKNOWN
        has_psi_results = FlagOutcome.UNKNOWN
        has_open_spectra = FlagOutcome.UNKNOWN
        has_tabular_quant = FlagOutcome.UNKNOWN
        has_sdrf = FlagOutcome.UNKNOWN
        has_mztab = FlagOutcome.UNKNOWN
    elif not files_data:
        has_result_files = FlagOutcome.FAILED
        has_psi_results = FlagOutcome.FAILED
        has_open_spectra = FlagOutcome.FAILED
        has_tabular_quant = FlagOutcome.FAILED
        has_sdrf = FlagOutcome.FAILED
        has_mztab = FlagOutcome.FAILED
    else:
        file_names: list[str] = []
        uncertain_filename = False
        uncertain_file = False
        file_classes: set[FileClass] = set()
        for file_data in files_data:
            if not isinstance(file_data, dict):
                uncertain_file = True
                uncertain_filename = True
                continue
            raw_filename = file_data.get("fileName")
            if isinstance(raw_filename, str):
                filename = raw_filename
                if not filename.strip():
                    uncertain_filename = True
            else:
                filename = ""
                uncertain_filename = True
            file_names.append(filename)
            raw_category = file_data.get("fileCategory")
            if raw_category is None:
                category = None
            elif isinstance(raw_category, dict) and (
                raw_category.get("value") is None or isinstance(raw_category.get("value"), str)
            ):
                category = raw_category.get("value")
            else:
                category = None
                uncertain_file = True
            file_classes.add(_classifier.classify(filename, category))

        uncertain_file = uncertain_file or uncertain_filename
        has_psi_results = _outcome_from_presence(
            any(_is_psi_result(filename) for filename in file_names), uncertain_filename
        )
        has_open_spectra = _outcome_from_presence(FileClass.PEAK in file_classes, uncertain_file)
        has_tabular_quant = _outcome_from_presence(
            FileClass.QUANT_MATRIX in file_classes, uncertain_filename
        )

        # PARTIAL submissions may use tool-native tables as processed evidence.
        if submission_type == "PARTIAL":
            result_gate: frozenset[FileClass] = frozenset(
                {FileClass.RESULT, FileClass.SEARCH, FileClass.QUANT_MATRIX, FileClass.ID_LIST}
            )
            result_uncertain = uncertain_file
        elif submission_type == "COMPLETE":
            result_gate = frozenset({FileClass.RESULT, FileClass.SEARCH})
            result_uncertain = uncertain_file
        else:
            result_gate = frozenset({FileClass.RESULT, FileClass.SEARCH})
            result_uncertain = uncertain_file or bool(
                file_classes & frozenset({FileClass.QUANT_MATRIX, FileClass.ID_LIST})
            )
        has_result_files = _outcome_from_presence(
            bool(file_classes & result_gate), result_uncertain
        )

        has_sdrf = _outcome_from_presence(FileClass.SDRF in file_classes, uncertain_filename)
        has_mztab = _outcome_from_presence(
            any(
                strip_compression(filename).casefold().endswith(".mztab") for filename in file_names
            ),
            uncertain_filename,
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
    flag_values: dict[str, FlagOutcome] = {
        "has_title": has_title,
        "has_organism": has_organism,
        "has_organism_id": has_organism_id,
        "has_instrument": has_instrument,
        "has_result_files": has_result_files,
        "has_psi_results": has_psi_results,
        "has_open_spectra": has_open_spectra,
        "has_organism_part": has_organism_part,
        "has_publication": has_publication,
        "has_tabular_quant": has_tabular_quant,
        "has_quant_metadata": has_quant_metadata,
        "has_sdrf": has_sdrf,
        "has_mztab": has_mztab,
    }

    if any(_failed(flag_values[name]) for name in ("has_title", "has_organism", "has_instrument")):
        tier = "None"
    elif _failed(has_result_files):
        tier = "Raw"
    elif _failed(has_psi_results):
        tier = "Bronze"
    elif _failed(has_sdrf):
        tier = "Silver"
    elif _failed(has_open_spectra) or _failed(has_organism_part):
        tier = "Gold"
    elif _failed(has_publication):
        tier = "Platinum"
    else:
        tier = "Diamond"

    # ------------------------------------------------------------------
    # 7.  Quant-tier derivation (secondary scoring axis)
    # ------------------------------------------------------------------
    if _failed(has_psi_results) and _failed(has_tabular_quant):
        quant_tier = "No Quant"
    elif _failed(has_psi_results) or _failed(has_tabular_quant):
        quant_tier = "Partial"
    elif _failed(has_quant_metadata):
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
        ambiguity_count=_unknown_count(flag_values),
        quant_tier=quant_tier,
    )


__all__ = [
    "AuditResult",
    "FlagOutcome",
    "compute_audit",
]
