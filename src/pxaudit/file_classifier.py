"""File-type classifier for PRIDE proteomics submissions.

Public API
----------
FileClass
    Enum of recognised file types.
strip_compression(filename)
    Strip all compression suffixes to expose the true format extension.
FileTypeClassifier
    Classify a filename (+ optional PRIDE fileCategory) into a FileClass.

Design notes
------------
PRIDE's ``fileCategory.value`` is convenient but demonstrably wrong in systematically
important cases (e.g. ``.mztab.gz`` routes to ``OTHER``). This module classifies files by
``fileName`` using our own extension and basename registries; the PRIDE category is retained
only as a last-resort fallback.

All pattern searches (SDRF, PSI, QUANT_MATRIX, ID_LIST) operate on the *de-compressed*,
lower-case base filename so that compressed variants like ``report.tsv.gz`` are handled
correctly by the ``$``-anchored patterns.
"""

from __future__ import annotations

import re
from enum import StrEnum

# ---------------------------------------------------------------------------
# FileClass
# ---------------------------------------------------------------------------


class FileClass(StrEnum):
    """Broad category for a single file in a PRIDE submission.

    Values mirror the PRIDE CV ``fileCategory`` strings where applicable (RAW, PEAK,
    RESULT, SEARCH) and extend it with FAIR-relevant categories that PRIDE does not
    distinguish (SDRF, FASTA, QUANT_MATRIX, ID_LIST).
    """

    RAW = "RAW"  # Vendor-proprietary raw spectra
    PEAK = "PEAK"  # Open-format spectra (mzML, mzXML, MGF …)
    RESULT = "RESULT"  # PSI-standard identification results (mzIdentML, mzTab)
    SEARCH = "SEARCH"  # Proprietary / non-standard analysis results (.dat, .msf, Skyline, TPP…)
    # Note: semantically this bucket covers "non-standard processed results", not only raw
    # search-engine output.  Skyline (.sky.zip), TPP XML (.pep.xml/.prot.xml), and Percolator
    # files belong here because they are PRIDE-SEARCH-compatible but not PSI-standard.
    SDRF = "SDRF"  # FAIR experimental-design file
    FASTA = "FASTA"  # Sequence database
    QUANT_MATRIX = "QUANT_MATRIX"  # Protein/peptide-level intensity summary table
    ID_LIST = "ID_LIST"  # PSM / evidence-level scan list (no quant summary)
    OTHER = "OTHER"  # Ancillary or unrecognised file


# ---------------------------------------------------------------------------
# Compression stripping
# ---------------------------------------------------------------------------

# Ordered longest-first so .tar.gz is matched before .gz when iterating.
_COMPRESSION_EXTS: tuple[str, ...] = (
    ".tar.gz",
    ".tar.bz2",
    ".tar.xz",  # compound tar archives
    ".tgz",  # tar.gz alias
    ".gz",
    ".bz2",
    ".zip",
    ".7z",
    ".xz",  # single-layer wrappers
)


def strip_compression(filename: str) -> str:
    """Remove all trailing compression suffixes to expose the true format extension.

    Uses a ``while`` loop so multi-layer archives (e.g. ``data.txt.zip.gz``) are
    fully unwrapped regardless of the order in which compression layers were applied.

    Note: Audit fix (Issue 4) — the original ``for``-loop implementation stopped after
    removing the outermost suffix and silently left inner layers in place.

    Examples
    --------
    >>> strip_compression("results.mzML.gz")
    'results.mzML'
    >>> strip_compression("data.txt.zip.gz")
    'data.txt'
    >>> strip_compression("archive.tar.gz")
    'archive'
    >>> strip_compression("results.mzid")
    'results.mzid'
    """
    lower = filename.lower()
    while True:
        stripped = False
        for ext in _COMPRESSION_EXTS:
            if lower.endswith(ext):
                filename = filename[: -len(ext)]
                lower = filename.lower()
                stripped = True
                break  # restart outer while with the shortened name
        if not stripped:
            break
    return filename


# ---------------------------------------------------------------------------
# Extension registry
# ---------------------------------------------------------------------------

_EXTENSION_TO_CLASS: dict[str, FileClass] = {
    # ── Vendor RAW (proprietary mass-spec native) ────────────────────────────────────────────
    ".raw": FileClass.RAW,  # Thermo Fisher (Xcalibur)
    ".wiff": FileClass.RAW,  # Sciex
    ".wiff2": FileClass.RAW,  # Sciex (newer format)
    ".wiff.scan": FileClass.RAW,  # Sciex scan-index companion to .wiff
    ".d": FileClass.RAW,  # Bruker (directory; submitted as .d.zip)
    ".baf": FileClass.RAW,  # Bruker analysis file
    ".tdf": FileClass.RAW,  # Bruker timsTOF data file
    ".tdf_bin": FileClass.RAW,  # Bruker timsTOF binary
    ".lcd": FileClass.RAW,  # Shimadzu
    ".mis": FileClass.RAW,  # Waters MassLynx index
    ".mih": FileClass.RAW,  # Waters MassLynx header
    ".iff": FileClass.RAW,  # ABI/Sciex IDA
    ".t2d": FileClass.RAW,  # ABI 4700/4800
    ".yep": FileClass.RAW,  # Bruker (older; pre-compact format)
    ".fid": FileClass.RAW,  # Bruker raw FID signal file
    ".uimf": FileClass.RAW,  # PNNL ion-mobility raw format (SLIM/IMS)
    # ── Open-format spectra (PEAK) ───────────────────────────────────────────────────────────
    ".mzml": FileClass.PEAK,  # HUPO-PSI; gold-standard open format
    ".mzxml": FileClass.PEAK,  # ProteoWizard legacy; widely supported
    ".mgf": FileClass.PEAK,  # Mascot Generic Format; text peak list
    ".ms2": FileClass.PEAK,  # SEQUEST/Crux peak list
    ".mzdata": FileClass.PEAK,  # Old PSI format (pre-mzML); rare
    ".pkl": FileClass.PEAK,  # Micromass/Waters peak list
    ".dta": FileClass.PEAK,  # Sequest per-scan DTA format
    ".apl": FileClass.PEAK,  # Andromeda peak list (MaxQuant input)
    ".ms1": FileClass.PEAK,  # MS1 spectrum file
    ".cms2": FileClass.PEAK,  # Crux/SEQUEST MS2 variant (newer)
    # ── PSI-standard identification results (RESULT) ─────────────────────────────────────────
    ".mzidentml": FileClass.RESULT,  # PSI mzIdentML; canonical open-standard
    ".mzid": FileClass.RESULT,  # alias for .mzidentml
    ".mztab": FileClass.RESULT,  # PSI mzTab; quant + identification (tabular)
    ".mztab-m": FileClass.RESULT,  # mzTab-M (metabolomics; rare in proteomics)
    # Note: mzTab is a container format: it can hold IDs, quant, and metadata.  The
    # classification to RESULT is correct for PRIDE compatibility; downstream tier logic
    # should use quantificationMethods[] to distinguish quant-capable mzTab files.
    ".idxml": FileClass.RESULT,  # OpenMS peptide identification format
    ".mzqc": FileClass.RESULT,  # PSI mzQC quality-control format
    # Note: .xml is intentionally absent — too broad (workflow.xml, parameters.xml).
    # PRIDE XML (pride_exp_complete/partial) is caught via _PSI_BASENAME_PATTERNS.
    # ── Proprietary search-engine output (SEARCH) ────────────────────────────────────────────
    ".dat": FileClass.SEARCH,  # Mascot result file
    ".msf": FileClass.SEARCH,  # Thermo Proteome Discoverer
    ".pdresult": FileClass.SEARCH,  # PD 2.x result file
    ".tandem": FileClass.SEARCH,  # X!Tandem output
    ".pep.xml": FileClass.SEARCH,  # Trans-Proteomic Pipeline — peptide level
    ".prot.xml": FileClass.SEARCH,  # TPP — protein level
    ".pepxml": FileClass.SEARCH,  # pepXML alias (flat, no compound dot)
    ".protxml": FileClass.SEARCH,  # protXML alias
    ".idpdb": FileClass.SEARCH,  # IDPicker database
    ".sqt": FileClass.SEARCH,  # Crux/SEQUEST SQT
    ".omx": FileClass.SEARCH,  # OMSSA output
    ".mzrt": FileClass.SEARCH,  # Progenesis
    ".sky.zip": FileClass.SEARCH,  # Skyline document archive (DIA results)
    ".pin": FileClass.SEARCH,  # Percolator input (post-search feature table)
    ".pout": FileClass.SEARCH,  # Percolator output (re-scored PSMs)
    # ── Sequence databases (FASTA) ───────────────────────────────────────────────────────────
    ".fasta": FileClass.FASTA,
    ".fa": FileClass.FASTA,
    ".fas": FileClass.FASTA,
    ".faa": FileClass.FASTA,  # protein FASTA (NCBI convention)
    ".fna": FileClass.FASTA,  # nucleotide FASTA (NCBI convention; rare in proteomics)
    # ── OpenMS feature file (ID_LIST) ────────────────────────────────────────────────────────
    # Placed in the extension registry (not in _ID_LIST_PATTERNS) so that compressed
    # variants like .featurexml.gz are correctly handled via Step 1 after compression strip.
    # Classification note: featureXML stores LC-MS feature detections (precursor-level), not
    # strictly PSM-level IDs.  A case can be made for QUANT_MATRIX, but until a FEATURE
    # category is introduced in the tier model, ID_LIST is the closest semantic bucket.
    ".featurexml": FileClass.ID_LIST,
    # ── SDRF (bare extension) ─────────────────────────────────────────────────────────────────
    # Covers files named e.g. 'experiment.sdrf'.  The more common '.sdrf.tsv' / '.sdrf.txt'
    # compound forms are handled upstream in classify() Step 3 (substring + tabular guard).
    ".sdrf": FileClass.SDRF,
}


# ---------------------------------------------------------------------------
# Exact-stem registry (MaxQuant fixed-output filenames)
# ---------------------------------------------------------------------------

_EXACT_STEM_TO_CLASS: dict[str, FileClass] = {
    # QUANT_MATRIX — protein/peptide-level aggregated quantification tables
    "proteingroups": FileClass.QUANT_MATRIX,
    "peptides": FileClass.QUANT_MATRIX,
    # ID_LIST — per-PSM / per-scan identification tables (no quant summary)
    "evidence": FileClass.ID_LIST,
    "allpeptides": FileClass.ID_LIST,
    "msms": FileClass.ID_LIST,
    "modificationspecificpeptides": FileClass.ID_LIST,
    # 'summary' and 'parameters' are intentionally absent — stems too generic.
}


# ---------------------------------------------------------------------------
# Basename / pattern registries
# ---------------------------------------------------------------------------

# PSI-standard files identifiable only by name, not extension alone.
# Checked BEFORE _QUANT_MATRIX_PATTERNS so mzTab-named files (e.g. ``-mztab.txt``,
# ``.mztab.gz``) correctly route to FileClass.RESULT, not FileClass.QUANT_MATRIX.
# Audit fix (Issue 2): mztab was previously in a RESULT_TOOL pattern; moved here.
_PSI_BASENAME_PATTERNS: re.Pattern[str] = re.compile(
    r"mztab|pride_exp_complete|pride_exp_partial",
    re.IGNORECASE,
)

# Protein/peptide-level intensity summary tables from common quantitative tools.
# Patterns are anchored with ``$`` and searched on the de-compressed, lower-case base
# filename so that ``.tsv.gz`` variants match correctly after strip_compression().
_QUANT_MATRIX_PATTERNS: re.Pattern[str] = re.compile(
    r"report\.tsv$"  # DIA-NN full report
    r"|report\.pg_matrix\.tsv$"  # DIA-NN protein-group matrix
    r"|report\.pr_matrix\.tsv$"  # DIA-NN precursor matrix
    r"|combined_protein\.tsv$"  # FragPipe / Philosopher protein summary
    r"|combined_peptide\.tsv$"  # FragPipe peptide summary
    r"|pg_matrix\.tsv$"  # FragPipe protein-group matrix
    r"|precursor_matrix\.tsv$"  # FragPipe precursor matrix
    r"|\.sr\.tsv$"  # Spectronaut sample report
    r"|\.pg\.tsv$",  # Spectronaut protein-group report
    re.IGNORECASE,
)

# PSM / scan-level identification lists (granular, without quant summary).
_ID_LIST_PATTERNS: re.Pattern[str] = re.compile(
    r"psm\.tsv$"  # general PSM table (FragPipe and others)
    r"|psm\.txt$"  # PSM table — .txt variant (some tools output .txt)
    r"|psms\.txt$"  # PSM table — plural .txt variant
    r"|combined_ion\.tsv$",  # FragPipe ion-level table
    re.IGNORECASE,
)

# PRIDE fileCategory → FileClass fallback.
# "OTHER" is intentionally absent; the function's default return is FileClass.OTHER.
_PRIDE_CATEGORY_MAP: dict[str, FileClass] = {
    "RAW": FileClass.RAW,
    "PEAK": FileClass.PEAK,
    "RESULT": FileClass.RESULT,
    "SEARCH": FileClass.SEARCH,
    "EXPERIMENTAL DESIGN": FileClass.SDRF,
}


# ---------------------------------------------------------------------------
# FileTypeClassifier
# ---------------------------------------------------------------------------


class FileTypeClassifier:
    """Classify proteomics filenames from PRIDE submissions into FileClass values.

    The built-in registries cover the vast majority of known submission formats.
    For formats not yet in the registry, pass custom overrides at construction time.

    Parameters
    ----------
    extra_extensions:
        Additional ``{extension: FileClass}`` mappings that extend (and can override)
        the built-in ``_EXTENSION_TO_CLASS`` dict.  Keys must be lower-case and include
        the leading dot, e.g. ``{".osw": FileClass.SEARCH}``.
    extra_basenames:
        Additional ``{lowercase_stem: FileClass}`` mappings checked after extension
        matching and before the regex patterns.  E.g.
        ``{"protein_report": FileClass.QUANT_MATRIX}``.

    Examples
    --------
    >>> clf = FileTypeClassifier()
    >>> clf.classify("run1.mzML")
    <FileClass.PEAK: 'PEAK'>
    >>> clf.classify("proteinGroups.txt")
    <FileClass.QUANT_MATRIX: 'QUANT_MATRIX'>
    >>> clf.classify("T091-mztab.txt")
    <FileClass.RESULT: 'RESULT'>
    >>> clf.classify("PXD073444.sdrf.tsv.gz")
    <FileClass.SDRF: 'SDRF'>
    """

    def __init__(
        self,
        extra_extensions: dict[str, FileClass] | None = None,
        extra_basenames: dict[str, FileClass] | None = None,
    ) -> None:
        self._ext_map = {**_EXTENSION_TO_CLASS, **(extra_extensions or {})}
        self._stem_map = {**_EXACT_STEM_TO_CLASS, **(extra_basenames or {})}

    # ------------------------------------------------------------------

    def classify(self, filename: str, pride_category: str | None = None) -> FileClass:
        """Classify a single filename into a FileClass.

        Precedence (first match wins):

        1a. Extension registry on the original filename
            (catches compound format extensions like ``.sky.zip`` before ``.zip``
            is stripped by ``strip_compression``)
        1b. Extension registry on the de-compressed filename
            (handles ``.mzml.gz``, ``.raw.gz``, ``.mzid.gz`` etc.)
        2.  Exact-stem map (MaxQuant fixed names + custom)
        3.  SDRF check — ``"sdrf"`` in name **and** tabular extension (.tsv/.txt/.csv)
        4.  PSI basename patterns → FileClass.RESULT
            (mzTab name variants + PRIDE XML; must precede quant patterns)
        5.  Quant-matrix patterns → FileClass.QUANT_MATRIX
        6.  ID-list patterns → FileClass.ID_LIST
        7.  PRIDE fileCategory fallback
        8.  FileClass.OTHER

        All pattern checks (steps 3–6) operate on the *de-compressed*, lower-case
        base filename so that e.g. ``report.tsv.gz`` correctly matches step 5 after
        strip_compression() removes the ``.gz`` wrapper.

        Parameters
        ----------
        filename:
            Raw filename string as returned by the PRIDE API ``fileName`` field.
        pride_category:
            Value of ``fileCategory.value`` from the PRIDE API (e.g. ``"RAW"``,
            ``"EXPERIMENTAL DESIGN"``).  Used only as a last-resort fallback.
        """
        # Step 1a — Extension registry on the ORIGINAL filename.
        # Must run before strip_compression so compound format extensions that happen
        # to end with .zip (e.g. .sky.zip — Skyline native format) are recognised as
        # format identifiers before .zip is mistakenly stripped as a compression wrapper.
        lower_filename = filename.lower()
        ext = self._extract_ext(lower_filename)
        if ext and ext in self._ext_map:
            return self._ext_map[ext]

        # Strip compression wrappers to reveal the true extension / basename.
        base = strip_compression(filename)
        lower_base = base.lower()

        # Step 1b — Extension registry on the de-compressed filename (.mzml.gz, .raw.gz …).
        ext = self._extract_ext(lower_base)
        if ext and ext in self._ext_map:
            return self._ext_map[ext]

        # Step 2 — Exact stem (MaxQuant fixed-output filenames like proteinGroups.txt).
        stem = lower_base.rsplit(".", 1)[0] if "." in lower_base else lower_base
        if stem in self._stem_map:
            return self._stem_map[stem]

        # Steps 3–6 all search on lower_base (de-compressed + lower-case).

        # Step 3 — SDRF: must contain "sdrf" AND end with a tabular extension.
        # Audit fix (Issue 8): bare "sdrf" check matched sdrf_instructions.pdf;
        # the endswith guard ensures only proper SDRF tabular files are accepted.
        if "sdrf" in lower_base and lower_base.endswith((".tsv", ".txt", ".csv")):
            return FileClass.SDRF

        # Step 4 — PSI basename patterns → RESULT.
        # Audit fix (Issue 2): catches -mztab.txt, .mztab.gz (after strip), PRIDE XML.
        # Runs before quant patterns so mzTab files are not mis-routed to QUANT_MATRIX.
        if _PSI_BASENAME_PATTERNS.search(lower_base):
            return FileClass.RESULT

        # Step 5 — Quant-matrix patterns (protein/peptide-level summaries).
        if _QUANT_MATRIX_PATTERNS.search(lower_base):
            return FileClass.QUANT_MATRIX

        # Step 6 — ID-list patterns (PSM / scan-level lists).
        if _ID_LIST_PATTERNS.search(lower_base):
            return FileClass.ID_LIST

        # Step 7 — PRIDE fileCategory fallback (only for categories we can trust).
        if pride_category:
            mapped = _PRIDE_CATEGORY_MAP.get(pride_category.upper())
            if mapped is not None:
                return mapped

        return FileClass.OTHER

    # ------------------------------------------------------------------

    @staticmethod
    def _extract_ext(lower_base: str) -> str:
        """Return the longest matching extension from *lower_base*.

        Checks known compound extensions first (.pep.xml, .wiff.scan, etc.), then
        falls back to the last single-part extension via ``str.rsplit``.

        Parameters
        ----------
        lower_base:
            Lower-case filename string, already stripped of compression suffixes.
        """
        for candidate in (".pep.xml", ".prot.xml", ".wiff.scan", ".mztab-m", ".sky.zip"):
            if lower_base.endswith(candidate):
                return candidate
        return "." + lower_base.rsplit(".", 1)[-1] if "." in lower_base else ""
