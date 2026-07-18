"""Tests for pxaudit.file_classifier.

Coverage target: 100% branch coverage on file_classifier.py.

Test organisation
-----------------
1.  strip_compression : single, multi-layer, tgz alias, no-op
2.  Extension registry : vendor RAW, PEAK, PSI RESULT, SEARCH, FASTA
3.  Compressed extensions : strip then classify
4.  Compound extensions : .pep.xml, .prot.xml, .wiff.scan, .sky.zip, .mztab-m
5.  Exact-stem map : MaxQuant QUANT_MATRIX and ID_LIST
6.  SDRF detection : canonical, compressed, imposters
7.  Processed-result basename patterns : mzTab name variants, PRIDE XML
8.  Quant-matrix patterns : DIA-NN, FragPipe, Spectronaut
9.  ID-list patterns : PSM tables
10. PRIDE category fallback : all mapped categories, unmapped
11. FileClass.OTHER : unknown extension, no fallback
12. Custom extension / basename overrides
13. _extract_ext static method : compound and single-part
14. FileClass enum : StrEnum membership and string equality
"""

from __future__ import annotations

import pytest

from pxaudit.file_classifier import (
    _COMPOUND_EXTS,
    _COMPRESSION_EXTS,
    _EXACT_STEM_TO_CLASS,
    _EXTENSION_TO_CLASS,
    FileClass,
    FileTypeClassifier,
    strip_compression,
)

# ---------------------------------------------------------------------------
# Module-level classifier instance shared across tests
# ---------------------------------------------------------------------------

clf = FileTypeClassifier()


# ---------------------------------------------------------------------------
# 1. strip_compression
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, expected",
    [
        ("results.mzML.gz", "results.mzML"),
        ("results.mzML.bz2", "results.mzML"),
        ("results.mzML.xz", "results.mzML"),
        ("archive.tar.gz", "archive"),
        ("archive.tar.bz2", "archive"),
        ("archive.tar.xz", "archive"),
        ("archive.tgz", "archive"),
        ("data.txt.zip.gz", "data.txt"),  # multi-layer : critical while-loop test
        ("data.raw.zip.gz", "data.raw"),  # multi-layer RAW
        ("results.mzid", "results.mzid"),  # no compression : no-op
        ("README", "README"),  # no extension : no-op
    ],
    ids=[
        "gz",
        "bz2",
        "xz",
        "tar-gz",
        "tar-bz2",
        "tar-xz",
        "tgz",
        "multi-layer-txt",
        "multi-layer-raw",
        "no-compression",
        "no-extension",
    ],
)
def test_strip_compression(filename: str, expected: str) -> None:
    assert strip_compression(filename) == expected


def test_strip_compression_preserves_case() -> None:
    """strip_compression must not change the case of the base filename."""
    assert strip_compression("Sample.Raw.GZ") == "Sample.Raw"


def test_compression_exts_ordered_longest_first() -> None:
    """Longest entries must come before their sub-strings to avoid premature matching."""
    for i, ext in enumerate(_COMPRESSION_EXTS):
        for later in _COMPRESSION_EXTS[i + 1 :]:
            # A later (shorter) entry must NOT end with an earlier (longer) entry :
            # that would mean the shorter one comes first and would match prematurely.
            assert not later.endswith(ext), (
                f"'{ext}' appears before '{later}' but '{later}' ends with '{ext}' : wrong order"
            )


# ---------------------------------------------------------------------------
# 2. Extension registry : basic classify calls
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, expected",
    [
        # Vendor RAW
        ("sample.raw", FileClass.RAW),
        ("sample.wiff", FileClass.RAW),
        ("sample.wiff2", FileClass.RAW),
        ("bruker.d", FileClass.RAW),
        ("bruker.baf", FileClass.RAW),
        ("bruker.tdf", FileClass.RAW),
        ("bruker.tdf_bin", FileClass.RAW),
        ("shimadzu.lcd", FileClass.RAW),
        ("waters.mis", FileClass.RAW),
        ("waters.mih", FileClass.RAW),
        ("sciex.iff", FileClass.RAW),
        ("abi.t2d", FileClass.RAW),
        ("bruker.yep", FileClass.RAW),  # Bruker older format
        ("bruker.fid", FileClass.RAW),  # Bruker FID signal
        ("sample.uimf", FileClass.RAW),  # PNNL ion-mobility
        # PEAK
        ("run.mzML", FileClass.PEAK),
        ("run.mzXML", FileClass.PEAK),
        ("run.mgf", FileClass.PEAK),
        ("run.ms2", FileClass.PEAK),
        ("run.mzdata", FileClass.PEAK),
        ("run.pkl", FileClass.PEAK),
        ("run.dta", FileClass.PEAK),
        ("run.apl", FileClass.PEAK),
        ("run.ms1", FileClass.PEAK),
        ("run.cms2", FileClass.PEAK),  # Crux/SEQUEST MS2 variant
        # PSI identification RESULT
        ("res.mzIdentML", FileClass.RESULT),
        ("res.mzid", FileClass.RESULT),
        ("res.mztab", FileClass.RESULT),
        ("ids.idxml", FileClass.SEARCH),
        ("qc.mzqc", FileClass.OTHER),
        # SEARCH
        ("mascot.dat", FileClass.SEARCH),
        ("pd.msf", FileClass.SEARCH),
        ("pd.pdresult", FileClass.SEARCH),
        ("xtandem.tandem", FileClass.SEARCH),
        ("crux.sqt", FileClass.SEARCH),
        ("omssa.omx", FileClass.SEARCH),
        ("prog.mzrt", FileClass.SEARCH),
        ("flat.pepxml", FileClass.SEARCH),
        ("flat.protxml", FileClass.SEARCH),
        ("idpicker.idpdb", FileClass.SEARCH),
        ("percolator.pin", FileClass.SEARCH),  # Percolator input
        ("percolator.pout", FileClass.SEARCH),  # Percolator output
        # FASTA
        ("human.fasta", FileClass.FASTA),
        ("human.fa", FileClass.FASTA),
        ("human.fas", FileClass.FASTA),
        ("human.faa", FileClass.FASTA),
        ("genome.fna", FileClass.FASTA),  # nucleotide FASTA
        # SDRF bare extension
        ("experiment.sdrf", FileClass.SDRF),
        # ID_LIST via extension
        ("features.featurexml", FileClass.ID_LIST),
    ],
)
def test_extension_registry(filename: str, expected: FileClass) -> None:
    assert clf.classify(filename) == expected


def test_extension_registry_is_case_insensitive() -> None:
    """Extensions must match regardless of case in the filename."""
    assert clf.classify("run.MZML") == FileClass.PEAK
    assert clf.classify("RUN.Raw") == FileClass.RAW
    assert clf.classify("RES.MZIdentML") == FileClass.RESULT


def test_result_extensions_are_supported_psi_identification_formats() -> None:
    """Only the three supported PSI proteomics suffixes use the RESULT extension class."""
    result_extensions = {
        extension
        for extension, file_class in _EXTENSION_TO_CLASS.items()
        if file_class is FileClass.RESULT
    }
    assert result_extensions == {".mzid", ".mzidentml", ".mztab"}


# ---------------------------------------------------------------------------
# 3. Compressed extensions : strip then classify
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, expected",
    [
        ("sample.raw.gz", FileClass.RAW),
        ("sample.raw.zip", FileClass.RAW),
        ("run.mzML.gz", FileClass.PEAK),
        ("run.mgf.gz", FileClass.PEAK),
        ("res.mzid.gz", FileClass.RESULT),
        ("res.mztab.gz", FileClass.RESULT),
        ("mascot.dat.gz", FileClass.SEARCH),
        ("human.fasta.gz", FileClass.FASTA),
        ("features.featurexml.gz", FileClass.ID_LIST),
    ],
)
def test_compressed_extensions(filename: str, expected: FileClass) -> None:
    assert clf.classify(filename) == expected


# ---------------------------------------------------------------------------
# 4. Compound extensions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, expected",
    [
        ("results.pep.xml", FileClass.SEARCH),  # TPP peptide
        ("results.prot.xml", FileClass.SEARCH),  # TPP protein
        ("sample.wiff.scan", FileClass.RAW),  # Sciex scan index
        ("method.sky.zip", FileClass.SEARCH),  # Skyline : .zip is format, not compression
        ("res.mztab-m", FileClass.OTHER),
    ],
)
def test_compound_extensions(filename: str, expected: FileClass) -> None:
    assert clf.classify(filename) == expected


def test_sky_zip_not_stripped_as_compression() -> None:
    """Skyline's .sky.zip must NOT have .zip stripped before classification."""
    # If .zip were stripped first, 'method.sky' would fall through to OTHER.
    assert clf.classify("method.sky.zip") == FileClass.SEARCH


def test_compound_exts_derived_from_registry() -> None:
    """_COMPOUND_EXTS must be a subset of _EXTENSION_TO_CLASS keys."""
    for ext in _COMPOUND_EXTS:
        assert ext in _EXTENSION_TO_CLASS, f"_COMPOUND_EXTS contains '{ext}' not in registry"


def test_compound_exts_all_have_dot_after_first_char() -> None:
    """Every compound extension must contain a dot after the leading dot."""
    for ext in _COMPOUND_EXTS:
        assert "." in ext[1:], f"'{ext}' does not have a compound dot pattern"


# ---------------------------------------------------------------------------
# 5. Exact-stem map : MaxQuant
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, expected",
    [
        ("proteinGroups.txt", FileClass.QUANT_MATRIX),
        ("peptides.txt", FileClass.QUANT_MATRIX),
        ("evidence.txt", FileClass.ID_LIST),
        ("allPeptides.txt", FileClass.ID_LIST),
        ("msms.txt", FileClass.ID_LIST),
        ("modificationSpecificPeptides.txt", FileClass.ID_LIST),
    ],
)
def test_exact_stem_maxquant(filename: str, expected: FileClass) -> None:
    assert clf.classify(filename) == expected


def test_exact_stem_map_is_case_insensitive() -> None:
    assert clf.classify("PROTEINGROUPS.txt") == FileClass.QUANT_MATRIX
    assert clf.classify("Evidence.txt") == FileClass.ID_LIST


def test_peptides_fasta_uses_extension_not_stem() -> None:
    """'peptides.fasta' : extension registry (FASTA) must win over stem map (QUANT_MATRIX)."""
    assert clf.classify("peptides.fasta") == FileClass.FASTA


def test_exact_stem_map_contents() -> None:
    """All MaxQuant QUANT_MATRIX stems must not accidentally include generic names."""
    assert "summary" not in _EXACT_STEM_TO_CLASS
    assert "parameters" not in _EXACT_STEM_TO_CLASS


# ---------------------------------------------------------------------------
# 6. SDRF detection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, expected",
    [
        ("PXD073444.sdrf.tsv", FileClass.SDRF),
        ("sdrf.tsv", FileClass.SDRF),
        ("my_sdrf_file.txt", FileClass.SDRF),
        ("PXD073444.sdrf.tsv.gz", FileClass.SDRF),  # compressed SDRF
        ("experimental_design.sdrf.tsv", FileClass.SDRF),
    ],
)
def test_sdrf_detected(filename: str, expected: FileClass) -> None:
    assert clf.classify(filename) == expected


@pytest.mark.parametrize(
    "filename",
    [
        "sdrf_instructions.pdf",  # not a tabular file
        "not_an_sdrf.xlsx",  # wrong extension
    ],
    ids=["pdf-imposter", "xlsx-imposter"],
)
def test_sdrf_imposter_non_tabular(filename: str) -> None:
    """Files with 'sdrf' in the name that are NOT tabular must not classify as SDRF."""
    result = clf.classify(filename)
    assert result != FileClass.SDRF


def test_sdrf_requires_token_boundary() -> None:
    """A longer alphabetic token containing ``sdrf`` is not an SDRF filename."""
    assert clf.classify("sdrfile.tsv") == FileClass.OTHER


@pytest.mark.parametrize(
    ("filename", "category", "expected"),
    [
        ("sdrf_instructions.pdf", "EXPERIMENTAL DESIGN", FileClass.SDRF),
        ("sdrf_instructions.pdf", "OTHER", FileClass.OTHER),
        ("sdrfile.tsv", "EXPERIMENTAL DESIGN", FileClass.OTHER),
        ("isa_metadata.tsv", "EXPERIMENTAL DESIGN", FileClass.OTHER),
    ],
)
def test_sdrf_category_and_token_contract(
    filename: str, category: str, expected: FileClass
) -> None:
    """Categorized and fallback SDRF paths share the same token boundary."""
    assert clf.classify(filename, category) is expected


# ---------------------------------------------------------------------------
# 7. Processed-result basename patterns
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename",
    [
        "T091_F066956-mascot-bucket-mztab.txt",  # Mascot mzTab export
        "results-mztab.txt",
        "pride_exp_complete.xml",  # PRIDE XML complete
        "pride_exp_partial.xml",  # PRIDE XML partial
        "pride_exp_partial.xml.gz",  # compressed PRIDE XML
    ],
)
def test_processed_result_basename_patterns_route_to_search(filename: str) -> None:
    assert clf.classify(filename) == FileClass.SEARCH


def test_processed_result_pattern_precedes_quant_patterns() -> None:
    """An mzTab basename hint is processed-result evidence, not a quant matrix."""
    assert clf.classify("results-mztab.txt") == FileClass.SEARCH


# ---------------------------------------------------------------------------
# 8. Quant-matrix patterns
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, expected",
    [
        # DIA-NN
        ("report.tsv", FileClass.QUANT_MATRIX),
        ("report.pg_matrix.tsv", FileClass.QUANT_MATRIX),
        ("report.pr_matrix.tsv", FileClass.QUANT_MATRIX),
        # FragPipe
        ("combined_protein.tsv", FileClass.QUANT_MATRIX),
        ("combined_peptide.tsv", FileClass.QUANT_MATRIX),
        ("combined_ion.tsv", FileClass.QUANT_MATRIX),
        ("pg_matrix.tsv", FileClass.QUANT_MATRIX),
        ("precursor_matrix.tsv", FileClass.QUANT_MATRIX),
        # Spectronaut
        ("sample_run.sr.tsv", FileClass.QUANT_MATRIX),
        ("experiment.pg.tsv", FileClass.QUANT_MATRIX),
    ],
)
def test_quant_matrix_patterns(filename: str, expected: FileClass) -> None:
    assert clf.classify(filename) == expected


def test_quant_matrix_compressed(filename: str = "report.tsv.gz") -> None:
    """Compressed DIA-NN report must still classify as QUANT_MATRIX."""
    assert clf.classify("report.tsv.gz") == FileClass.QUANT_MATRIX


@pytest.mark.parametrize(
    "filename",
    ["audit_report.tsv", "uncombined_ion.tsv", "backup_combined_protein.tsv"],
)
def test_quant_matrix_fixed_names_reject_prefixed_lookalikes(filename: str) -> None:
    """Fixed tool outputs do not match unrelated filenames that share a suffix."""
    assert clf.classify(filename) == FileClass.OTHER


# ---------------------------------------------------------------------------
# 9. ID-list patterns
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, expected",
    [
        ("psm.tsv", FileClass.ID_LIST),
        ("psm.txt", FileClass.ID_LIST),
        ("psms.txt", FileClass.ID_LIST),
    ],
)
def test_id_list_patterns(filename: str, expected: FileClass) -> None:
    assert clf.classify(filename) == expected


# ---------------------------------------------------------------------------
# 10. PRIDE category fallback
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "category, expected",
    [
        ("RAW", FileClass.RAW),
        ("PEAK", FileClass.PEAK),
        ("RESULT", FileClass.RESULT),
        ("SEARCH", FileClass.SEARCH),
        ("EXPERIMENTAL DESIGN", FileClass.OTHER),
    ],
)
def test_pride_category_fallback(category: str, expected: FileClass) -> None:
    assert clf.classify("unknown.bin", pride_category=category) == expected


def test_pride_category_fallback_case_insensitive() -> None:
    assert clf.classify("unknown.bin", pride_category="raw") == FileClass.RAW
    assert clf.classify("unknown.bin", pride_category="Experimental Design") == FileClass.OTHER


def test_pride_category_other_not_mapped() -> None:
    """PRIDE's 'OTHER' category must not be mapped : fall through to FileClass.OTHER."""
    assert clf.classify("unknown.bin", pride_category="OTHER") == FileClass.OTHER


def test_pride_category_unknown_not_mapped() -> None:
    assert clf.classify("unknown.bin", pride_category="GARBAGE") == FileClass.OTHER


# ---------------------------------------------------------------------------
# 11. FileClass.OTHER : unknown, no fallback
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename",
    [
        "random.bin",
        "README",
        "workflow.xml",  # .xml intentionally absent from ext registry
        "parameters.xml",
        "archive.tar.gz",  # stripped to bare 'archive' : no extension
    ],
)
def test_other_for_unknown_files(filename: str) -> None:
    assert clf.classify(filename) == FileClass.OTHER


def test_no_pride_category_gives_other() -> None:
    assert clf.classify("unknown.bin") == FileClass.OTHER
    assert clf.classify("unknown.bin", pride_category=None) == FileClass.OTHER


# ---------------------------------------------------------------------------
# 12. Custom extension / basename overrides
# ---------------------------------------------------------------------------


def test_custom_extension_override() -> None:
    custom = FileTypeClassifier(extra_extensions={".osw": FileClass.SEARCH})
    assert custom.classify("result.osw") == FileClass.SEARCH
    # Ensure built-in extensions still work in the custom instance
    assert custom.classify("run.mzML") == FileClass.PEAK


def test_custom_extension_overrides_builtin() -> None:
    """A custom mapping can intentionally override a built-in classification."""
    custom = FileTypeClassifier(extra_extensions={".dat": FileClass.OTHER})
    assert custom.classify("mascot.dat") == FileClass.OTHER


def test_custom_compound_extension_is_matched_before_final_suffix() -> None:
    """A custom compound extension participates in longest-extension matching."""
    custom = FileTypeClassifier(extra_extensions={".protein.tsv": FileClass.QUANT_MATRIX})
    assert custom.classify("sample.protein.tsv") == FileClass.QUANT_MATRIX
    assert custom.classify("sample.protein.tsv.gz") == FileClass.QUANT_MATRIX


def test_custom_basename_override() -> None:
    custom = FileTypeClassifier(extra_basenames={"protein_report": FileClass.QUANT_MATRIX})
    assert custom.classify("protein_report.tsv") == FileClass.QUANT_MATRIX


def test_default_instance_unaffected_by_custom() -> None:
    """Creating a custom instance must not mutate the module-level registries."""
    FileTypeClassifier(extra_extensions={".osw": FileClass.SEARCH})
    assert ".osw" not in _EXTENSION_TO_CLASS


# ---------------------------------------------------------------------------
# 13. _extract_ext static method
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "lower_base, expected",
    [
        ("results.pep.xml", ".pep.xml"),
        ("results.prot.xml", ".prot.xml"),
        ("sample.wiff.scan", ".wiff.scan"),
        ("method.sky.zip", ".sky.zip"),
        ("res.mztab-m", ".mztab-m"),
        ("run.mzml", ".mzml"),
        ("archive", ""),  # no extension
    ],
)
def test_extract_ext(lower_base: str, expected: str) -> None:
    assert clf._extract_ext(lower_base) == expected


# ---------------------------------------------------------------------------
# 14. FileClass enum : StrEnum behaviour
# ---------------------------------------------------------------------------


def test_fileclass_string_equality() -> None:
    """FileClass(StrEnum) values must compare equal to plain strings."""
    assert FileClass.RAW == "RAW"
    assert FileClass.QUANT_MATRIX == "QUANT_MATRIX"


def test_fileclass_all_members_present() -> None:
    members = {m.value for m in FileClass}
    assert members == {
        "RAW",
        "PEAK",
        "RESULT",
        "SEARCH",
        "SDRF",
        "FASTA",
        "QUANT_MATRIX",
        "ID_LIST",
        "OTHER",
    }
