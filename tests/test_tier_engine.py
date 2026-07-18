"""Tests for pxaudit.tier_engine.

Coverage target: 100% branch coverage on tier_engine.py.

Test organisation
-----------------
1.  Invalid accession → ValueError
2.  Non-PXD prefix routing → Unverifiable
3.  Tier boundary (parametrized): Gold / Silver / Bronze×2 / None×3
4.  Historical files_fetch_failed override: produces a Raw result
5.  SDRF pattern : case sensitivity and token-boundary checks
6.  mzTab extension : matches .mztab variants, rejects .mztabdata
7.  fileCategory canonical matching : RESULT/SEARCH recognised, RESULTS not
8.  Null / empty project-data inputs : graceful False flags
9.  Empty files_data list : all file flags False
10. AuditResult structure : fields match _AUDIT_COLS; tier_logic_version correct
"""

from __future__ import annotations

import dataclasses

import pytest

from pxaudit.tier_engine import (
    _TIER_LOGIC_VERSION,
    AuditResult,
    compute_audit,
)

# ---------------------------------------------------------------------------
# Helpers : build minimal synthetic API payloads
# ---------------------------------------------------------------------------


def _project(
    *,
    title: str | None = "Test study",
    organism_name: str | None = "Homo sapiens",
    organism_id: str | None = "NEWT:9606",
    instrument_name: str | None = "Orbitrap Fusion",
) -> dict:
    """Return a minimal /projects response dict."""
    p: dict = {}
    if title is not None:
        p["title"] = title
    organisms = []
    if organism_name is not None or organism_id is not None:
        entry: dict = {}
        if organism_name is not None:
            entry["name"] = organism_name
        if organism_id is not None:
            entry["accession"] = organism_id
        organisms.append(entry)
    p["organisms"] = organisms
    instruments = []
    if instrument_name is not None:
        instruments.append({"name": instrument_name})
    p["instruments"] = instruments
    return p


def _file(file_name: str, category_value: str = "RAW") -> dict:
    """Return a minimal file dict matching the PRIDE v3 /files shape."""
    return {
        "fileName": file_name,
        "fileCategory": {
            "@type": "CvParam",
            "cvLabel": "PRIDE",
            "value": category_value,
            "name": "",
        },
        "fileSizeBytes": 1024,
        "publicFileLocations": [],
    }


def _result_files() -> list[dict]:
    """Return a minimal files list that satisfies has_result_files."""
    return [_file("results.mzid", "RESULT")]


def _sdrf_files() -> list[dict]:
    """Return a minimal files list that satisfies both has_result_files and has_sdrf."""
    return [
        _file("results.mzid", "RESULT"),
        _file("sdrf.tsv", "OTHER"),
    ]


def _gold_files() -> list[dict]:
    """Result + SDRF + mzTab : satisfies all file-based flags for Gold."""
    return [
        _file("results.mzid", "RESULT"),
        _file("sdrf.tsv", "OTHER"),
        _file("results.mzTab", "OTHER"),
    ]


# ---------------------------------------------------------------------------
# 1. Invalid accession → ValueError
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_accession", ["", "12345", "000001"])
def test_invalid_accession_raises_value_error(bad_accession: str) -> None:
    with pytest.raises(ValueError, match="Invalid accession"):
        compute_audit(bad_accession, {}, [])


# ---------------------------------------------------------------------------
# 2. Non-PXD prefix → Unverifiable (short-circuit, no API data needed)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("accession", ["MSV000001", "JPST0001", "IPX0001", "MTBLS001"])
def test_non_pxd_returns_unverifiable(accession: str) -> None:
    result = compute_audit(accession, {}, [])
    assert result.tier == "Unverifiable"
    assert result.is_unverifiable is True
    assert result.accession == accession


def test_non_pxd_all_flags_false() -> None:
    """For an unverifiable accession, all Boolean metadata flags must be False."""
    r = compute_audit("MSV000001", _project(), _gold_files())
    assert r.has_title is False
    assert r.has_organism is False
    assert r.has_organism_id is False
    assert r.has_instrument is False
    assert r.has_result_files is False
    assert r.has_sdrf is False
    assert r.has_mztab is False


# ---------------------------------------------------------------------------
# 3. Tier boundary tests (parametrized)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "project, files, expected_tier",
    [
        # Gold: title + organism + organism_id + instrument + result + sdrf
        (
            _project(),
            _gold_files(),
            "Gold",
        ),
        # Silver: everything except SDRF (organism_id present, result present, no SDRF)
        (
            _project(),
            _result_files(),
            "Silver",
        ),
        # Silver: organism_id missing : 7-tier dropped organism_id from gate;
        # result files present → Bronze (no PSI)? No : mzid IS PSI → Silver
        (
            _project(organism_id=None),
            _result_files(),
            "Silver",
        ),
        # Raw: organism_id present but no result/search/quant files at all
        (
            _project(),
            [_file("raw.raw", "RAW")],
            "Raw",
        ),
        # None case 1: title missing
        (
            _project(title=None),
            _gold_files(),
            "None",
        ),
        # None case 2: organism missing
        (
            _project(organism_name=None, organism_id=None),
            _gold_files(),
            "None",
        ),
        # None case 3: instrument missing
        (
            _project(instrument_name=None),
            _gold_files(),
            "None",
        ),
    ],
    ids=[
        "Gold",
        "Silver",
        "Silver-no-organism_id",
        "Raw-no-result",
        "None-no-title",
        "None-no-organism",
        "None-no-instrument",
    ],
)
def test_tier_boundaries(project: dict, files: list[dict], expected_tier: str) -> None:
    result = compute_audit("PXD000001", project, files)
    assert result.tier == expected_tier
    assert result.is_unverifiable is False


# Silver explicitly excludes Gold: SDRF must be the single missing element.
def test_silver_has_result_files_but_no_sdrf() -> None:
    r = compute_audit("PXD000001", _project(), _result_files())
    assert r.tier == "Silver"
    assert r.has_result_files is True
    assert r.has_sdrf is False


# Raw explicitly excludes Bronze+: no result files means Raw in 7-tier.
def test_raw_has_no_result_files() -> None:
    r = compute_audit("PXD000001", _project(), [_file("raw.raw", "RAW")])
    assert r.tier == "Raw"
    assert r.has_result_files is False
    assert r.has_organism_id is True


# None check: title present but empty string must yield has_title=False.
def test_none_tier_empty_string_title() -> None:
    r = compute_audit("PXD000001", _project(title=""), _gold_files())
    assert r.has_title is False
    assert r.tier == "None"


# 3b. New 7-tier levels : Platinum and Diamond
# ---------------------------------------------------------------------------


def _platinum_project() -> dict:
    """Project with organism_part and no publication : enables Platinum/Diamond tests."""
    return {**_project(), "organismParts": [{"name": "brain"}], "references": []}


def _platinum_files() -> list[dict]:
    """Files that satisfy all flags up to Platinum: result + SDRF + open spectra."""
    return [
        _file("run1.mzML", "PEAK"),
        _file("results.mzid", "RESULT"),
        _file("sdrf.tsv", "OTHER"),
    ]


def test_bronze_tier_with_search_only_no_psi() -> None:
    """result files present (SEARCH) but none are PSI-standard → Bronze."""
    files = [_file("results.dat", "SEARCH")]  # .dat → FileClass.SEARCH, not RESULT
    r = compute_audit("PXD000001", _project(), files)
    assert r.tier == "Bronze"
    assert r.has_result_files is True
    assert r.has_psi_results is False


def test_gold_tier_missing_open_spectra() -> None:
    """PSI results + SDRF but no open spectra → Gold."""
    files = [_file("results.mzid", "RESULT"), _file("sdrf.tsv", "OTHER")]
    r = compute_audit("PXD000001", {**_project(), "organismParts": [{"name": "brain"}]}, files)
    assert r.tier == "Gold"
    assert r.has_psi_results is True
    assert r.has_sdrf is True
    assert r.has_open_spectra is False


def test_gold_tier_missing_organism_part() -> None:
    """PSI results + SDRF + open spectra but no organism part → Gold."""
    files = _platinum_files()
    r = compute_audit("PXD000001", _project(), files)  # _project() has no organismParts
    assert r.tier == "Gold"
    assert r.has_open_spectra is True
    assert r.has_organism_part is False


def test_platinum_tier_sdrf_open_spectra_org_part_no_pub() -> None:
    """All file flags met + organism_part but no publication → Platinum."""
    r = compute_audit("PXD000001", _platinum_project(), _platinum_files())
    assert r.tier == "Platinum"
    assert r.has_open_spectra is True
    assert r.has_organism_part is True
    assert r.has_publication is False


def test_diamond_tier_all_flags_met() -> None:
    """All FAIR criteria met → Diamond."""
    project = {
        **_platinum_project(),
        "references": [{"pubmedID": 12345}],
    }
    r = compute_audit("PXD000001", project, _platinum_files())
    assert r.tier == "Diamond"
    assert r.has_publication is True


# ---------------------------------------------------------------------------
# 4. files_fetch_failed override
# ---------------------------------------------------------------------------


def test_files_fetch_failed_caps_tier_at_raw() -> None:
    """All metadata present, files_fetch_failed=True → has_result_files=False → tier Raw."""
    r = compute_audit("PXD000001", _project(), [], files_fetch_failed=True)
    assert r.tier == "Raw"
    assert r.files_fetch_failed is True


def test_files_fetch_failed_sets_file_flags_false() -> None:
    """Even if files_data is non-empty, files_fetch_failed overrides all file flags."""
    r = compute_audit("PXD000001", _project(), _gold_files(), files_fetch_failed=True)
    assert r.has_result_files is False
    assert r.has_sdrf is False
    assert r.has_mztab is False
    # Must not be Silver or Gold
    assert r.tier not in ("Silver", "Gold")


def test_files_fetch_failed_false_with_empty_files_still_raw() -> None:
    """files_fetch_failed=False but empty files list → file flags False, tier Raw."""
    r = compute_audit("PXD000001", _project(), [], files_fetch_failed=False)
    assert r.has_result_files is False
    assert r.tier == "Raw"
    assert r.files_fetch_failed is False


# ---------------------------------------------------------------------------
# 5. SDRF pattern : token boundaries and case sensitivity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "file_name, expected",
    [
        ("sdrf.tsv", True),
        ("SDRF.tsv", True),
        ("Sdrf.TSV", True),
        ("my_sdrf_file.txt", True),
        ("experimental_design.sdrf.tsv", True),
        ("sdrfile.txt", False),  # sdrf immediately followed by a letter
        ("sdrfdata.tsv", False),  # same
        ("not_related.tsv", False),
        ("sdrf_instructions.pdf", False),  # tabular-ext guard: .pdf must NOT match
        ("PXD073444.sdrf.tsv.gz", True),  # compressed SDRF : .tsv.gz suffix allowed
    ],
    ids=[
        "lowercase",
        "uppercase",
        "mixedcase",
        "sdrf-in-middle",
        "sdrf-after-dot",
        "sdrfile-no-match",
        "sdrfdata-no-match",
        "unrelated",
        "sdrf-pdf-no-match",
        "compressed-sdrf",
    ],
)
def test_sdrf_pattern_matching(file_name: str, expected: bool) -> None:
    files = [
        _file(file_name, "RESULT"),  # result so tier would be Silver, not None
        _file("result.mzid", "RESULT"),
    ]
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_sdrf is expected


# ---------------------------------------------------------------------------
# 5b. SDRF primary path : EXPERIMENTAL DESIGN category
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "file_name, category, expected",
    [
        ("sdrf_sample.tsv", "EXPERIMENTAL DESIGN", True),  # canonical PRIDE casing
        ("experimental_design.sdrf.tsv", "Experimental Design", True),  # mixed casing
        ("SDRF_data.tsv", "experimental design", True),  # all-lowercase category
        ("isa_metadata.tsv", "EXPERIMENTAL DESIGN", False),  # category OK, no sdrf in name
        ("sdrfile.tsv", "EXPERIMENTAL DESIGN", False),  # token is part of a longer word
        ("sdrf_instructions.pdf", "EXPERIMENTAL DESIGN", True),  # categorized primary path
        ("sdrf.tsv", "OTHER", True),  # category mismatch → falls back to filename match
    ],
    ids=[
        "primary-canonical",
        "primary-mixed-case-cat",
        "primary-lowercase-cat",
        "primary-no-sdrf-in-name",
        "primary-rejects-longer-token",
        "primary-category-allows-nontabular",
        "fallback-other-category",
    ],
)
def test_sdrf_primary_path(file_name: str, category: str, expected: bool) -> None:
    files = [
        _file(file_name, category),
        _file("result.mzid", "RESULT"),
    ]
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_sdrf is expected


# ---------------------------------------------------------------------------
# 6. mzTab extension matching
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "file_name, expected",
    [
        ("results.mzTab", True),
        ("results.mztab", True),
        ("results.MZTAB", True),
        ("results.mzTabData", False),  # must NOT match
        ("results.mztabdata", False),  # same
        ("results.mztab.gz", True),  # compressed : strip_compression exposes .mztab
        ("results.mztab.zip", True),  # compressed : strip_compression exposes .mztab
        ("results.mztab.bz2", True),  # compressed : strip_compression exposes .mztab
        ("results.mzTab.GZ", True),  # compressed : case-insensitive after strip
        ("results.mzid", False),
    ],
    ids=[
        "mzTab",
        "mztab",
        "MZTAB",
        "mzTabData",
        "mztabdata",
        "mztab-gz",
        "mztab-zip",
        "mztab-bz2",
        "mzTab-GZ-case",
        "mzid",
    ],
)
def test_mztab_extension_matching(file_name: str, expected: bool) -> None:
    files = [_file(file_name, "RESULT")]
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_mztab is expected


# ---------------------------------------------------------------------------
# 7. fileCategory canonical matching
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "category_value, expected",
    [
        ("RESULT", True),
        ("result", True),
        ("Result", True),
        ("SEARCH", True),
        ("search", True),
        ("RESULTS", False),  # plural must NOT match
        ("OTHER", False),
        ("RAW", False),
        ("PEAK", False),
        ("", False),
    ],
    ids=[
        "RESULT",
        "result",
        "Result",
        "SEARCH",
        "search",
        "RESULTS-plural",
        "OTHER",
        "RAW",
        "PEAK",
        "empty",
    ],
)
def test_file_category_matching(category_value: str, expected: bool) -> None:
    # Use an unrecognised filename so the PRIDE category fallback (step 7 of the
    # classifier) is the deciding factor.  A vendor .raw file would be classified
    # as FileClass.RAW by extension regardless of the PRIDE category string.
    files = [_file("unknown.bin", category_value)]
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_result_files is expected


# ---------------------------------------------------------------------------
# 8. Null / empty project-data input handling
# ---------------------------------------------------------------------------


def test_empty_project_dict_gives_none_tier() -> None:
    """Missing all keys: title/organisms/instruments all absent → tier None."""
    r = compute_audit("PXD000001", {}, _gold_files())
    assert r.tier == "None"
    assert r.has_title is False
    assert r.has_organism is False
    assert r.has_organism_id is False
    assert r.has_instrument is False


def test_none_title_gives_has_title_false() -> None:
    r = compute_audit("PXD000001", _project(title=None), _gold_files())
    assert r.has_title is False


def test_empty_organisms_list_gives_organism_flags_false() -> None:
    project = {"title": "T", "organisms": [], "instruments": [{"name": "I"}]}
    r = compute_audit("PXD000001", project, _result_files())
    assert r.has_organism is False
    assert r.has_organism_id is False


def test_organism_with_no_name_field_gives_has_organism_false() -> None:
    """Organism entry present but name is missing → has_organism False."""
    project = {
        "title": "T",
        "organisms": [{"accession": "NEWT:9606"}],  # name absent
        "instruments": [{"name": "I"}],
    }
    r = compute_audit("PXD000001", project, _result_files())
    assert r.has_organism is False
    assert r.has_organism_id is True  # accession IS present


def test_organism_with_no_accession_field_gives_has_organism_id_false() -> None:
    """Organism entry present but accession missing → has_organism_id False."""
    project = {
        "title": "T",
        "organisms": [{"name": "Homo sapiens"}],  # accession absent
        "instruments": [{"name": "I"}],
    }
    r = compute_audit("PXD000001", project, _result_files())
    assert r.has_organism is True
    assert r.has_organism_id is False


def test_empty_instruments_list_gives_has_instrument_false() -> None:
    project = {
        "title": "T",
        "organisms": [{"name": "H", "accession": "NEWT:9606"}],
        "instruments": [],
    }
    r = compute_audit("PXD000001", project, _result_files())
    assert r.has_instrument is False
    assert r.tier == "None"


def test_none_project_data_handled_as_empty_dict() -> None:
    """Caller passes None for project_data : must not raise, all flags False."""
    r = compute_audit("PXD000001", None, [])  # type: ignore[arg-type]
    assert r.tier == "None"
    assert r.has_title is False


# ---------------------------------------------------------------------------
# 9. Empty files list
# ---------------------------------------------------------------------------


def test_empty_files_list_gives_all_file_flags_false() -> None:
    r = compute_audit("PXD000001", _project(), [])
    assert r.has_result_files is False
    assert r.has_sdrf is False
    assert r.has_mztab is False
    assert r.tier == "Raw"  # no result files → Raw in 7-tier


def test_file_with_none_file_name_handled_gracefully() -> None:
    """A file dict with fileName=None must not raise : treated as empty string."""
    files = [{"fileName": None, "fileCategory": {"value": "RESULT"}, "fileSizeBytes": 0}]
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_result_files is True  # category is still RESULT
    assert r.has_sdrf is False  # fileName is None → empty → no sdrf


def test_file_with_missing_category_key_gives_false() -> None:
    """A file dict without fileCategory key must not raise."""
    files = [{"fileName": "data.raw"}]  # no fileCategory
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_result_files is False


# ---------------------------------------------------------------------------
# 10. AuditResult structure
# ---------------------------------------------------------------------------


def test_audit_result_accession_preserved() -> None:
    r = compute_audit("PXD999999", _project(), _gold_files())
    assert r.accession == "PXD999999"


def test_audit_result_tier_logic_version() -> None:
    r = compute_audit("PXD000001", _project(), _gold_files())
    assert r.tier_logic_version == _TIER_LOGIC_VERSION == "v2.1"


def test_audit_result_is_dataclass_instance() -> None:
    r = compute_audit("PXD000001", _project(), _gold_files())
    assert dataclasses.is_dataclass(r)


def test_audit_result_dataclass_fields_match_audit_cols() -> None:
    """AuditResult field names must match _AUDIT_COLS in db.py exactly."""
    from pxaudit.db import _AUDIT_COLS

    result_fields = {f.name for f in dataclasses.fields(AuditResult)}
    assert result_fields == set(_AUDIT_COLS)


def test_gold_all_flags_true() -> None:
    """Gold tier means every Boolean flag is True."""
    r = compute_audit("PXD000001", _project(), _gold_files())
    assert r.tier == "Gold"
    assert r.has_title is True
    assert r.has_organism is True
    assert r.has_organism_id is True
    assert r.has_instrument is True
    assert r.has_result_files is True
    assert r.has_sdrf is True
    assert r.is_unverifiable is False
    assert r.files_fetch_failed is False


# ---------------------------------------------------------------------------
# 11. New project-level flags (C06)
# ---------------------------------------------------------------------------


def test_has_organism_part_true_when_organism_parts_non_empty() -> None:
    project = {**_project(), "organismParts": [{"name": "brain"}]}
    r = compute_audit("PXD000001", project, [])
    assert r.has_organism_part is True


def test_has_organism_part_false_when_absent() -> None:
    r = compute_audit("PXD000001", _project(), [])
    assert r.has_organism_part is False


def test_has_quant_metadata_true_when_quant_methods_non_empty() -> None:
    project = {**_project(), "quantificationMethods": [{"name": "iTRAQ"}]}
    r = compute_audit("PXD000001", project, [])
    assert r.has_quant_metadata is True


@pytest.mark.parametrize(
    "quantification_methods",
    [
        [],
        [{}],
        [{"name": ""}],
        [{"accession": "  "}],
        [{"value": "label free"}],
        ["iTRAQ"],
        [None],
    ],
)
def test_has_quant_metadata_requires_usable_cv_term(
    quantification_methods: list[object],
) -> None:
    """Non-CV or blank quantification entries do not establish quant metadata."""
    project = {**_project(), "quantificationMethods": quantification_methods}
    result = compute_audit("PXD000001", project, [])
    assert result.has_quant_metadata is False


def test_has_quant_metadata_accepts_cv_accession() -> None:
    """A nonblank controlled-vocabulary accession establishes quant metadata."""
    project = {**_project(), "quantificationMethods": [{"accession": "MS:1001834"}]}
    result = compute_audit("PXD000001", project, [])
    assert result.has_quant_metadata is True


def test_has_quant_metadata_rejects_non_list_container() -> None:
    """A mapping in place of the PRIDE method list does not establish CV metadata."""
    project = {**_project(), "quantificationMethods": {"name": "iTRAQ"}}
    result = compute_audit("PXD000001", project, [])
    assert result.has_quant_metadata is False


@pytest.mark.parametrize(
    "pubmed_value, expected",
    [
        (12345, True),  # valid integer pubmedID
        (0, False),  # zero : PRIDE sentinel for unpublished
        (None, False),  # None : older API responses; safe_pubmed_id handles TypeError
        ("", False),  # empty string : safe_pubmed_id handles ValueError
    ],
    ids=["valid-int", "zero", "none", "empty-str"],
)
def test_has_publication_from_pubmed_id(pubmed_value: object, expected: bool) -> None:
    project = {**_project(), "references": [{"pubmedID": pubmed_value}]}
    r = compute_audit("PXD000001", project, [])
    assert r.has_publication is expected


def test_has_publication_false_when_references_empty() -> None:
    project = {**_project(), "references": []}
    r = compute_audit("PXD000001", project, [])
    assert r.has_publication is False


# ---------------------------------------------------------------------------
# 12. FileTypeClassifier-derived file flags (C06)
# ---------------------------------------------------------------------------


def test_has_psi_results_true_for_mzid_file() -> None:
    files = [_file("results.mzid", "RESULT")]
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_psi_results is True


def test_has_psi_results_false_for_raw_only() -> None:
    files = [_file("run1.raw", "RAW")]
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_psi_results is False


@pytest.mark.parametrize(
    ("filename", "category", "expected_result", "expected_psi"),
    [
        ("results.mzid", "OTHER", True, True),
        ("results.mzidentml.gz", "OTHER", True, True),
        ("results.mztab.zip", "OTHER", True, True),
        ("results.csv", "RESULT", True, False),
        ("results.idxml", "RESULT", True, False),
        ("quality.mzqc", "RESULT", False, False),
        ("metabolomics.mztab-m", "RESULT", False, False),
        ("pride_exp_complete.xml", "OTHER", True, False),
        ("results-mztab.txt", "OTHER", True, False),
        ("results.mztabdata", "OTHER", False, False),
        ("mascot.dat", "SEARCH", True, False),
    ],
)
def test_processed_and_psi_result_evidence_are_independent(
    filename: str,
    category: str,
    expected_result: bool,
    expected_psi: bool,
) -> None:
    """Only supported PSI suffixes cross the PSI identification gate."""
    result = compute_audit("PXD000001", _project(), [_file(filename, category)])
    assert result.has_result_files is expected_result
    assert result.has_psi_results is expected_psi


def test_has_open_spectra_true_for_mzml_file() -> None:
    files = [_file("run1.mzML", "PEAK"), _file("results.mzid", "RESULT")]
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_open_spectra is True


def test_has_open_spectra_false_for_raw_only() -> None:
    files = [_file("run1.raw", "RAW")]
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_open_spectra is False


@pytest.mark.parametrize("filename", ["proteinGroups.txt", "combined_ion.tsv"])
def test_has_tabular_quant_true_for_quant_matrix(filename: str) -> None:
    """Recognized abundance summaries and matrices establish tabular quant evidence."""
    files = [_file(filename, "OTHER"), _file("results.mzid", "RESULT")]
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_tabular_quant is True


def test_has_tabular_quant_false_for_id_list() -> None:
    """A PSM or evidence list is not an abundance summary or matrix."""
    files = [_file("evidence.txt", "OTHER"), _file("results.mzid", "RESULT")]
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_tabular_quant is False


def test_has_tabular_quant_false_for_raw_only() -> None:
    files = [_file("run1.raw", "RAW")]
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_tabular_quant is False


# ---------------------------------------------------------------------------
# 13. Submission-type-aware result gate (C06)
# ---------------------------------------------------------------------------


def test_partial_submission_counts_quant_matrix_as_result() -> None:
    """PARTIAL: a MaxQuant proteinGroups.txt satisfies has_result_files."""
    project = {**_project(), "submissionType": "PARTIAL"}
    files = [_file("proteinGroups.txt", "OTHER")]  # no RESULT or SEARCH file
    r = compute_audit("PXD000001", project, files)
    assert r.has_result_files is True


def test_complete_submission_does_not_count_quant_matrix_as_result() -> None:
    """COMPLETE: only RESULT/SEARCH count; QUANT_MATRIX alone is not enough."""
    project = {**_project(), "submissionType": "COMPLETE"}
    files = [_file("proteinGroups.txt", "OTHER")]  # no RESULT or SEARCH file
    r = compute_audit("PXD000001", project, files)
    assert r.has_result_files is False


def test_partial_submission_id_list_counts_as_result() -> None:
    """PARTIAL: an ID_LIST file (evidence.txt) also satisfies has_result_files."""
    project = {**_project(), "submissionType": "PARTIAL"}
    files = [_file("evidence.txt", "OTHER")]  # ID_LIST, no RESULT or SEARCH
    r = compute_audit("PXD000001", project, files)
    assert r.has_result_files is True
    assert r.has_tabular_quant is False
    assert r.quant_tier == "No Quant"


# ---------------------------------------------------------------------------
# 14. quant_tier secondary scoring axis (C09)
# ---------------------------------------------------------------------------


def test_quant_tier_no_quant_when_no_psi_and_no_tabular() -> None:
    """No PSI results and no tabular quant → quant_tier = 'No Quant'."""
    files = [_file("run1.raw", "RAW")]
    r = compute_audit("PXD000001", _project(), files)
    assert r.quant_tier == "No Quant"


def test_quant_tier_partial_tool_native_only() -> None:
    """Tabular quant present but no PSI file → 'Partial' (tool-native tables only)."""
    # proteinGroups.txt → FileClass.QUANT_MATRIX; no RESULT/SEARCH file
    project = {**_project(), "submissionType": "PARTIAL"}
    files = [_file("proteinGroups.txt", "OTHER")]
    r = compute_audit("PXD000001", project, files)
    assert r.has_psi_results is False
    assert r.has_tabular_quant is True
    assert r.quant_tier == "Partial"


def test_quant_tier_partial_psi_only() -> None:
    """PSI file present but no tabular quant → 'Partial' (PSI IDs, no quant table)."""
    files = [_file("results.mzid", "RESULT")]  # RESULT only, no quant matrix
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_psi_results is True
    assert r.has_tabular_quant is False
    assert r.quant_tier == "Partial"


def test_quant_tier_quant_ready_psi_and_tabular_no_metadata() -> None:
    """PSI + tabular quant present but no quantificationMethods → 'Quant-Ready'."""
    files = [
        _file("results.mzid", "RESULT"),
        _file("proteinGroups.txt", "OTHER"),
    ]
    r = compute_audit("PXD000001", _project(), files)
    assert r.has_psi_results is True
    assert r.has_tabular_quant is True
    assert r.has_quant_metadata is False
    assert r.quant_tier == "Quant-Ready"


def test_quant_tier_quant_complete_all_three_present() -> None:
    """PSI + tabular quant + quantificationMethods → 'Quant-Complete'."""
    project = {**_project(), "quantificationMethods": [{"name": "iTRAQ"}]}
    files = [
        _file("results.mzid", "RESULT"),
        _file("proteinGroups.txt", "OTHER"),
    ]
    r = compute_audit("PXD000001", project, files)
    assert r.has_psi_results is True
    assert r.has_tabular_quant is True
    assert r.has_quant_metadata is True
    assert r.quant_tier == "Quant-Complete"


@pytest.mark.parametrize(
    ("files", "quant_methods", "expected_psi", "expected_table", "expected_tier"),
    [
        ([_file("results.mztab", "RESULT")], [], True, False, "Partial"),
        (
            [_file("results.mztab", "RESULT"), _file("proteinGroups.txt", "OTHER")],
            [],
            True,
            True,
            "Quant-Ready",
        ),
        (
            [_file("results.mztab", "RESULT"), _file("proteinGroups.txt", "OTHER")],
            [{"accession": "MS:1001834"}],
            True,
            True,
            "Quant-Complete",
        ),
        (
            [_file("results.mzid", "RESULT"), _file("evidence.txt", "OTHER")],
            [{"name": "label free"}],
            True,
            False,
            "Partial",
        ),
        (
            [_file("results.mzid", "RESULT"), _file("proteinGroups.txt", "OTHER")],
            [{"name": " "}],
            True,
            True,
            "Quant-Ready",
        ),
    ],
)
def test_quant_tier_requires_independent_abundance_and_cv_evidence(
    files: list[dict],
    quant_methods: list[dict],
    expected_psi: bool,
    expected_table: bool,
    expected_tier: str,
) -> None:
    """mzTab, abundance tables, ID lists, and CV metadata contribute independently."""
    project = {**_project(), "quantificationMethods": quant_methods}
    result = compute_audit("PXD000001", project, files)
    assert result.has_psi_results is expected_psi
    assert result.has_tabular_quant is expected_table
    assert result.quant_tier == expected_tier


def test_quant_tier_unverifiable_for_non_pxd_accession() -> None:
    """Non-PXD accession takes the early-return path → quant_tier = 'Unverifiable'."""
    r = compute_audit("MSV000079514", {}, [])
    assert r.is_unverifiable is True
    assert r.quant_tier == "Unverifiable"
