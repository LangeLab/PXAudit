"""Semantic, boundary, and precedence tests for filename classification."""

from __future__ import annotations

import pytest

from pxaudit.file_classifier import (
    _COMPRESSION_EXTS,
    _EXACT_STEM_TO_CLASS,
    _EXTENSION_TO_CLASS,
    FileClass,
    FileTypeClassifier,
    strip_compression,
)

_CLASSIFIER = FileTypeClassifier()

_EXTENSION_GROUPS: tuple[tuple[FileClass, str], ...] = (
    (
        FileClass.RAW,
        ".raw .wiff .wiff2 .wiff.scan .d .baf .tdf .tdf_bin .lcd .mis .mih "
        ".iff .t2d .yep .fid .uimf",
    ),
    (FileClass.PEAK, ".mzml .mzxml .mgf .ms2 .mzdata .pkl .dta .apl .ms1 .cms2"),
    (FileClass.RESULT, ".mzidentml .mzid .mztab"),
    (
        FileClass.SEARCH,
        ".idxml .dat .msf .pdresult .tandem .pep.xml .prot.xml .pepxml .protxml "
        ".idpdb .sqt .omx .mzrt .sky.zip .pin .pout",
    ),
    (FileClass.OTHER, ".mztab-m .mzqc"),
    (FileClass.FASTA, ".fasta .fa .fas .faa .fna"),
    (FileClass.ID_LIST, ".featurexml"),
    (FileClass.SDRF, ".sdrf"),
)


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("results.mzML.gz", "results.mzML"),
        ("archive.tar.gz", "archive"),
        ("archive.tgz", "archive"),
        ("data.txt.zip.gz", "data.txt"),
        ("Sample.Raw.GZ", "Sample.Raw"),
        ("results.mzid", "results.mzid"),
        ("README", "README"),
        (".gz", ""),
        ("", ""),
    ],
)
def test_strip_compression_contract(filename: str, expected: str) -> None:
    """Compression removal handles nesting, aliases, case, and no-op inputs."""
    assert strip_compression(filename) == expected


def test_strip_compression_nested_wrapper_matrix() -> None:
    """Every supported pair of wrappers is removed without changing base case."""
    for inner in _COMPRESSION_EXTS:
        for outer in _COMPRESSION_EXTS:
            filename = f"Sample.Raw{inner.upper()}{outer.upper()}"
            assert strip_compression(filename) == "Sample.Raw", filename


def test_extension_registry_matches_reviewed_semantics() -> None:
    """The reviewed semantic table covers every built-in extension exactly once."""
    reviewed: dict[str, FileClass] = {}
    for expected, extension_text in _EXTENSION_GROUPS:
        for extension in extension_text.split():
            reviewed[extension] = expected
            assert _CLASSIFIER.classify(f"sample{extension}") is expected

    assert reviewed == _EXTENSION_TO_CLASS


def test_registered_extensions_are_case_insensitive() -> None:
    """Every registered extension keeps its class under ASCII case changes."""
    for expected, extension_text in _EXTENSION_GROUPS:
        for extension in extension_text.split():
            filename = f"SAMPLE{extension.upper()}"
            assert _CLASSIFIER.classify(filename) is expected, filename


def test_registered_extensions_survive_every_outer_wrapper() -> None:
    """Compression must not erase a format suffix, including Skyline's ``.sky.zip``."""
    for expected, extension_text in _EXTENSION_GROUPS:
        for extension in extension_text.split():
            for wrapper in _COMPRESSION_EXTS:
                filename = f"sample{extension}{wrapper}"
                assert _CLASSIFIER.classify(filename) is expected, filename


@pytest.mark.parametrize(
    ("filename", "category", "expected"),
    [
        ("peptides.fasta", None, FileClass.FASTA),
        ("result.mzid", "RAW", FileClass.RESULT),
        ("ids.idxml", "RESULT", FileClass.SEARCH),
        ("proteinGroups.txt", "RAW", FileClass.QUANT_MATRIX),
        ("PXD.sdrf.tsv", "RAW", FileClass.SDRF),
        ("results-mztab.txt", "RAW", FileClass.SEARCH),
        ("unknown.bin", "RAW", FileClass.RAW),
    ],
)
def test_classification_precedence(
    filename: str, category: str | None, expected: FileClass
) -> None:
    """Filename evidence wins in documented order before category fallback."""
    assert _CLASSIFIER.classify(filename, category) is expected


def test_exact_stem_registry_matches_reviewed_semantics() -> None:
    """The fixed-stem registry contains only reviewed MaxQuant outputs."""
    assert {
        "proteingroups": FileClass.QUANT_MATRIX,
        "peptides": FileClass.QUANT_MATRIX,
        "evidence": FileClass.ID_LIST,
        "allpeptides": FileClass.ID_LIST,
        "msms": FileClass.ID_LIST,
        "modificationspecificpeptides": FileClass.ID_LIST,
    } == _EXACT_STEM_TO_CLASS


def test_exact_stems_are_case_insensitive_and_compression_aware() -> None:
    """Fixed MaxQuant stems retain their class across case and compression."""
    for stem, file_class in _EXACT_STEM_TO_CLASS.items():
        assert _CLASSIFIER.classify(f"{stem.swapcase()}.txt.gz") is file_class


@pytest.mark.parametrize("stem", ["summary", "parameters"])
def test_generic_stems_are_not_classified(stem: str) -> None:
    """Generic table names do not imply a tool output class."""
    assert _CLASSIFIER.classify(f"{stem}.txt") is FileClass.OTHER


@pytest.mark.parametrize(
    ("filename", "category", "expected"),
    [
        ("PXD073444.sdrf.tsv", None, FileClass.SDRF),
        ("sdrf.tsv.gz", None, FileClass.SDRF),
        ("my_sdrf_file.txt", None, FileClass.SDRF),
        ("sdrf-instructions.pdf", "EXPERIMENTAL DESIGN", FileClass.SDRF),
        ("sdrfile.tsv", "EXPERIMENTAL DESIGN", FileClass.OTHER),
        ("isa_metadata.tsv", "EXPERIMENTAL DESIGN", FileClass.OTHER),
        ("sdrf_instructions.pdf", "OTHER", FileClass.OTHER),
        ("not_an_sdrf.xlsx", None, FileClass.OTHER),
    ],
)
def test_sdrf_token_category_and_tabular_boundaries(
    filename: str, category: str | None, expected: FileClass
) -> None:
    """SDRF evidence requires a token plus either its category or a tabular suffix."""
    assert _CLASSIFIER.classify(filename, category) is expected


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("T091_F066956-mascot-bucket-mztab.txt", FileClass.SEARCH),
        ("results_mztab.txt.gz", FileClass.SEARCH),
        ("pride_exp_complete.xml", FileClass.SEARCH),
        ("pride_exp_partial.xml.gz", FileClass.SEARCH),
        ("premztab.txt", FileClass.OTHER),
        ("pride_exp_incomplete.xml", FileClass.OTHER),
    ],
)
def test_processed_result_token_boundaries(filename: str, expected: FileClass) -> None:
    """Processed-result hints match delimited tokens, including compressed names."""
    assert _CLASSIFIER.classify(filename) is expected


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("report.tsv", FileClass.QUANT_MATRIX),
        ("report.pg_matrix.tsv.gz", FileClass.QUANT_MATRIX),
        ("report.pr_matrix.tsv", FileClass.QUANT_MATRIX),
        ("combined_protein.tsv", FileClass.QUANT_MATRIX),
        ("combined_peptide.tsv", FileClass.QUANT_MATRIX),
        ("combined_ion.tsv", FileClass.QUANT_MATRIX),
        ("pg_matrix.tsv", FileClass.QUANT_MATRIX),
        ("precursor_matrix.tsv", FileClass.QUANT_MATRIX),
        ("sample_run.sr.tsv", FileClass.QUANT_MATRIX),
        ("experiment.pg.tsv", FileClass.QUANT_MATRIX),
        ("audit_report.tsv", FileClass.OTHER),
        ("uncombined_ion.tsv", FileClass.OTHER),
        ("backup_combined_protein.tsv", FileClass.OTHER),
        ("report.tsv.bak", FileClass.OTHER),
    ],
)
def test_quant_matrix_exact_and_dynamic_name_boundaries(filename: str, expected: FileClass) -> None:
    """Fixed quant outputs are exact while documented dynamic prefixes remain valid."""
    assert _CLASSIFIER.classify(filename) is expected


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("psm.tsv", FileClass.ID_LIST),
        ("sample-psm.tsv", FileClass.ID_LIST),
        ("sample_psm.txt", FileClass.ID_LIST),
        ("psms.txt.gz", FileClass.ID_LIST),
        ("notpsm.tsv", FileClass.OTHER),
        ("psm.csv", FileClass.OTHER),
        ("psms.tsv", FileClass.OTHER),
        ("psm.tsv.bak", FileClass.OTHER),
    ],
)
def test_id_list_token_and_suffix_boundaries(filename: str, expected: FileClass) -> None:
    """PSM lists require a delimited token and a supported terminal suffix."""
    assert _CLASSIFIER.classify(filename) is expected


@pytest.mark.parametrize(
    ("category", "expected"),
    [
        ("RAW", FileClass.RAW),
        ("raw", FileClass.RAW),
        ("Peak", FileClass.PEAK),
        ("RESULT", FileClass.RESULT),
        ("search", FileClass.SEARCH),
        ("EXPERIMENTAL DESIGN", FileClass.OTHER),
        ("OTHER", FileClass.OTHER),
        ("GARBAGE", FileClass.OTHER),
        ("", FileClass.OTHER),
        (None, FileClass.OTHER),
    ],
)
def test_pride_category_fallback(category: str | None, expected: FileClass) -> None:
    """Only trusted PRIDE categories participate in case-insensitive fallback."""
    assert _CLASSIFIER.classify("unknown.bin", category) is expected


@pytest.mark.parametrize("filename", ["", "random.bin", "README", "workflow.xml", "archive.tar.gz"])
def test_unknown_files_fall_back_to_other(filename: str) -> None:
    """Empty, extensionless, generic XML, and exhausted archive names remain unknown."""
    assert _CLASSIFIER.classify(filename) is FileClass.OTHER


def test_custom_extensions_extend_and_override_registry() -> None:
    """One classifier can add an extension and override a built-in mapping."""
    custom = FileTypeClassifier(
        extra_extensions={".osw": FileClass.SEARCH, ".dat": FileClass.OTHER}
    )
    assert custom.classify("result.osw") is FileClass.SEARCH
    assert custom.classify("mascot.dat") is FileClass.OTHER
    assert custom.classify("run.mzML") is FileClass.PEAK


def test_custom_compound_extension_survives_outer_compression() -> None:
    """A custom compound format remains recognizable beneath an outer wrapper."""
    custom = FileTypeClassifier(extra_extensions={".bundle.zip": FileClass.QUANT_MATRIX})

    assert custom.classify("sample.bundle.zip.gz") is FileClass.QUANT_MATRIX


def test_custom_basename_is_classified_before_patterns() -> None:
    """A custom exact basename participates at the documented precedence stage."""
    custom = FileTypeClassifier(extra_basenames={"protein_report": FileClass.QUANT_MATRIX})

    assert custom.classify("protein_report.tsv") is FileClass.QUANT_MATRIX


def test_custom_configuration_does_not_mutate_defaults() -> None:
    """Per-instance extension configuration leaves built-in classifiers unchanged."""
    FileTypeClassifier(extra_extensions={".osw": FileClass.SEARCH})

    assert _CLASSIFIER.classify("result.osw") is FileClass.OTHER
    assert ".osw" not in _EXTENSION_TO_CLASS


def test_file_class_public_values() -> None:
    """The string enum exposes the complete stable set of public class values."""
    assert {member.value for member in FileClass} == {
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
    assert FileClass.RAW == "RAW"
