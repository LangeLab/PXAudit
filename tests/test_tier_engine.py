"""Scientific evidence and tier-derivation contracts for :mod:`pxaudit.tier_engine`."""

from __future__ import annotations

import dataclasses
from itertools import product

import pytest

from pxaudit.db import _AUDIT_COLS
from pxaudit.tier_engine import _TIER_LOGIC_VERSION, AuditResult, compute_audit

_ACCESSION = "PXD000001"


def _project(**overrides: object) -> dict:
    """Return a minimal project response with complete mandatory metadata."""
    project: dict[str, object] = {
        "title": "Test study",
        "organisms": [{"name": "Homo sapiens", "accession": "NEWT:9606"}],
        "instruments": [{"name": "Orbitrap Fusion"}],
    }
    project.update(overrides)
    return project


def _file(filename: str | None, category: str | None = "RAW") -> dict:
    """Return one minimal PRIDE file mapping."""
    return {"fileName": filename, "fileCategory": {"value": category}}


@pytest.mark.parametrize(
    "accession",
    ["", "PXD12345", "PXDABCDEF", "MSV/000001", "../PXD000001", "PXD000001\nX"],
)
def test_invalid_accession_is_rejected_before_scoring(accession: str) -> None:
    """Malformed or unsafe identifiers fail before project or file evidence is used."""
    with pytest.raises(ValueError):
        compute_audit(accession, _project(), [_file("results.mzid", "RESULT")])


@pytest.mark.parametrize(
    ("raw_accession", "canonical", "files_fetch_failed"),
    [
        ("MSV000001", "MSV000001", False),
        (" jpst0001 ", "JPST0001", False),
        ("IPX0001", "IPX0001", True),
        ("MTBLS001", "MTBLS001", False),
        ("12345", "12345", False),
    ],
)
def test_partner_accession_short_circuits_to_exact_unverifiable_result(
    raw_accession: str,
    canonical: str,
    files_fetch_failed: bool,
) -> None:
    """Partner identifiers ignore PRIDE evidence and preserve only fetch provenance."""
    result = compute_audit(
        raw_accession,
        _project(),
        [_file("results.mzid", "RESULT")],
        files_fetch_failed=files_fetch_failed,
    )

    assert result == AuditResult(
        accession=canonical,
        tier="Unverifiable",
        files_fetch_failed=files_fetch_failed,
        is_unverifiable=True,
        quant_tier="Unverifiable",
    )


def test_qualitative_ladder_obeys_every_representative_evidence_combination() -> None:
    """All 128 representative evidence combinations obey the first-failing-gate order."""
    flag_names = (
        "basic_metadata",
        "processed_result",
        "psi_result",
        "sdrf",
        "open_spectra",
        "organism_part",
        "publication",
    )
    for values in product((False, True), repeat=len(flag_names)):
        flags = dict(zip(flag_names, values, strict=True))
        project = _project(
            title="Test study" if flags["basic_metadata"] else None,
            organismParts=[{"name": "brain"}] if flags["organism_part"] else [],
            references=[{"pubmedID": 1}] if flags["publication"] else [],
        )
        files = []
        if flags["processed_result"]:
            files.append(_file("mascot.dat", "SEARCH"))
        if flags["psi_result"]:
            files.append(_file("results.mzid", "RESULT"))
        if flags["sdrf"]:
            files.append(_file("study.sdrf.tsv", "OTHER"))
        if flags["open_spectra"]:
            files.append(_file("spectra.mzML", "PEAK"))

        result = compute_audit(_ACCESSION, project, files)
        has_result_files = flags["processed_result"] or flags["psi_result"]
        expected_tier = next(
            tier
            for gate_failed, tier in (
                (not flags["basic_metadata"], "None"),
                (not has_result_files, "Raw"),
                (not flags["psi_result"], "Bronze"),
                (not flags["sdrf"], "Silver"),
                (
                    not flags["open_spectra"] or not flags["organism_part"],
                    "Gold",
                ),
                (not flags["publication"], "Platinum"),
                (True, "Diamond"),
            )
            if gate_failed
        )

        assert result.tier == expected_tier, flags
        assert result.is_unverifiable is False
        assert (
            result.has_result_files,
            result.has_psi_results,
            result.has_sdrf,
            result.has_open_spectra,
            result.has_organism_part,
            result.has_publication,
        ) == (
            has_result_files,
            flags["psi_result"],
            flags["sdrf"],
            flags["open_spectra"],
            flags["organism_part"],
            flags["publication"],
        ), flags


@pytest.mark.parametrize(
    ("project", "expected_flags", "expected_tier"),
    [
        (_project(), (True, True, True, True), "Silver"),
        ({}, (False, False, False, False), "None"),
        (_project(title=""), (False, True, True, True), "None"),
        (_project(organisms=[]), (True, False, False, True), "None"),
        (
            _project(organisms=[{"accession": "NEWT:9606"}]),
            (True, False, True, True),
            "None",
        ),
        (
            _project(organisms=[{"name": "Homo sapiens"}]),
            (True, True, False, True),
            "Silver",
        ),
        (_project(instruments=[]), (True, True, True, False), "None"),
        (
            _project(
                organisms=[
                    {"name": "", "accession": ""},
                    {"name": "Homo sapiens", "accession": "NEWT:9606"},
                ],
                instruments=[{"name": ""}, {"name": "Orbitrap Fusion"}],
            ),
            (True, False, False, False),
            "None",
        ),
    ],
    ids=(
        "complete",
        "empty-project",
        "empty-title",
        "empty-organisms",
        "organism-id-only",
        "organism-name-only",
        "empty-instruments",
        "first-entry-contract",
    ),
)
def test_mandatory_metadata_flags_and_none_tier(
    project: dict,
    expected_flags: tuple[bool, bool, bool, bool],
    expected_tier: str,
) -> None:
    """Mandatory fields use the first organism and instrument entries as documented."""
    result = compute_audit(_ACCESSION, project, [_file("results.mzid", "RESULT")])

    assert (
        result.has_title,
        result.has_organism,
        result.has_organism_id,
        result.has_instrument,
    ) == expected_flags
    assert result.tier == expected_tier


def test_none_project_and_files_are_treated_as_empty_inputs() -> None:
    """Legacy callers passing null payloads receive a deterministic empty-evidence result."""
    result = compute_audit(_ACCESSION, None, None)  # type: ignore[arg-type]

    assert result.tier == "None"
    assert result.quant_tier == "No Quant"
    assert not any(
        (
            result.has_title,
            result.has_organism,
            result.has_organism_id,
            result.has_instrument,
            result.has_result_files,
            result.has_psi_results,
            result.has_open_spectra,
            result.has_organism_part,
            result.has_publication,
            result.has_tabular_quant,
            result.has_quant_metadata,
            result.has_sdrf,
            result.has_mztab,
        )
    )


@pytest.mark.parametrize(
    ("organism_parts", "expected"),
    [(None, False), ([], False), ([{}], True), ([{"name": "brain"}], True)],
    ids=("absent", "empty", "empty-entry", "named-entry"),
)
def test_organism_part_flag_represents_list_presence(
    organism_parts: object,
    expected: bool,
) -> None:
    """Any nonempty project organism-parts list establishes coarse biological context."""
    project = _project()
    if organism_parts is not None:
        project["organismParts"] = organism_parts

    result = compute_audit(_ACCESSION, project, [])

    assert result.has_organism_part is expected


@pytest.mark.parametrize(
    ("quantification_methods", "expected"),
    [
        (None, False),
        ([], False),
        ({"name": "iTRAQ"}, False),
        ([{}], False),
        ([{"name": ""}], False),
        ([{"accession": "  "}], False),
        ([{"value": "label free"}], False),
        (["iTRAQ"], False),
        ([None], False),
        ([{"name": "iTRAQ"}], True),
        ([{"accession": "MS:1001834"}], True),
        ([{"name": " ", "accession": "MS:1001834"}], True),
    ],
)
def test_quant_metadata_requires_a_nonblank_cv_name_or_accession(
    quantification_methods: object,
    expected: bool,
) -> None:
    """Container presence alone cannot establish controlled-vocabulary quant metadata."""
    project = _project(quantificationMethods=quantification_methods)

    result = compute_audit(_ACCESSION, project, [])

    assert result.has_quant_metadata is expected


@pytest.mark.parametrize(
    ("references", "expected"),
    [
        (None, False),
        ([], False),
        ([{"pubmedID": 0}], False),
        ([{"pubmedID": None}], False),
        ([{"pubmedID": ""}], False),
        ([{"pubmedID": "not-an-id"}], False),
        ([{"pubmedID": 23692960}], True),
        ([{"pubmedID": "23692960"}], True),
        ([{"pubmedID": 0}, {"pubmedID": "23692960"}], True),
    ],
)
def test_publication_requires_any_parseable_nonzero_pubmed_id(
    references: object,
    expected: bool,
) -> None:
    """Missing, malformed, and zero PubMed identifiers do not imply publication."""
    result = compute_audit(_ACCESSION, _project(references=references), [])

    assert result.has_publication is expected


@pytest.mark.parametrize(
    ("files", "expected"),
    [
        ([_file("results.mzid", "OTHER")], (True, True, False, False, False, False)),
        ([_file("results.mzidentml.gz", "OTHER")], (True, True, False, False, False, False)),
        ([_file("results.MZTAB.zip", "OTHER")], (True, True, True, False, False, False)),
        ([_file("results.csv", "RESULT")], (True, False, False, False, False, False)),
        ([_file("results.idxml", "RESULT")], (True, False, False, False, False, False)),
        ([_file("quality.mzqc", "RESULT")], (False, False, False, False, False, False)),
        ([_file("metabolomics.mztab-m", "RESULT")], (False, False, False, False, False, False)),
        ([_file("pride_exp_complete.xml", "OTHER")], (True, False, False, False, False, False)),
        ([_file("results-mztab.txt", "OTHER")], (True, False, False, False, False, False)),
        ([_file("results.mztabdata", "OTHER")], (False, False, False, False, False, False)),
        ([_file("mascot.dat", "SEARCH")], (True, False, False, False, False, False)),
        ([_file(None, "RESULT")], (True, False, False, False, False, False)),
        ([{"fileName": "run.raw"}], (False, False, False, False, False, False)),
        ([_file("run.mzML", "PEAK")], (False, False, False, True, False, False)),
        ([_file("proteinGroups.txt", "OTHER")], (False, False, False, False, True, False)),
        ([_file("evidence.txt", "OTHER")], (False, False, False, False, False, False)),
        ([_file("study.sdrf.tsv", "OTHER")], (False, False, False, False, False, True)),
        ([_file("SDRF.tsv", "OTHER")], (False, False, False, False, False, True)),
        ([_file("123sdrf456.tsv", "OTHER")], (False, False, False, False, False, True)),
        ([_file("asdrf.tsv", "OTHER")], (False, False, False, False, False, False)),
        ([_file("sdrf", "OTHER")], (False, False, False, False, False, False)),
        (
            [_file("sdrf_instructions.pdf", "OTHER")],
            (False, False, False, False, False, False),
        ),
        (
            [_file("sdrf_instructions.pdf", "EXPERIMENTAL DESIGN")],
            (False, False, False, False, False, True),
        ),
        (
            [_file("sdrfile.tsv", "EXPERIMENTAL DESIGN")],
            (False, False, False, False, False, False),
        ),
    ],
)
def test_file_evidence_preserves_semantic_boundaries(
    files: list[dict],
    expected: tuple[bool, bool, bool, bool, bool, bool],
) -> None:
    """Representative classifier outputs remain distinct audit evidence signals."""
    result = compute_audit(_ACCESSION, _project(), files)

    assert (
        result.has_result_files,
        result.has_psi_results,
        result.has_mztab,
        result.has_open_spectra,
        result.has_tabular_quant,
        result.has_sdrf,
    ) == expected


@pytest.mark.parametrize(
    "category",
    ("RESULTS", "result ", " RESULT", "SEARCHING", "SEARCH ", "RESULTSET", "re sult"),
)
def test_near_match_categories_do_not_establish_processed_results(category: str) -> None:
    """Whitespace, pluralization, and longer category tokens remain untrusted."""
    result = compute_audit(_ACCESSION, _project(), [_file("unknown.bin", category)])

    assert result.has_result_files is False


@pytest.mark.parametrize(
    ("submission_type", "filename", "expected_result", "expected_table"),
    [
        ("PARTIAL", "proteinGroups.txt", True, True),
        ("partial", "evidence.txt", True, False),
        ("COMPLETE", "proteinGroups.txt", False, True),
        (None, "proteinGroups.txt", False, True),
        ("PARTIAL", "run.raw", False, False),
    ],
)
def test_partial_submission_expands_only_the_processed_result_gate(
    submission_type: str | None,
    filename: str,
    expected_result: bool,
    expected_table: bool,
) -> None:
    """Partial submissions accept tool tables without changing their quant meaning."""
    result = compute_audit(
        _ACCESSION,
        _project(submissionType=submission_type),
        [_file(filename, "OTHER")],
    )

    assert result.has_result_files is expected_result
    assert result.has_tabular_quant is expected_table


@pytest.mark.parametrize(
    ("project", "files", "files_fetch_failed", "expected_tier"),
    [
        (_project(), [], False, "Raw"),
        (
            _project(),
            [_file("results.mzid", "RESULT"), _file("study.sdrf.tsv", "OTHER")],
            True,
            "Raw",
        ),
        (
            {},
            [_file("results.mzid", "RESULT"), _file("study.sdrf.tsv", "OTHER")],
            True,
            "None",
        ),
    ],
    ids=("verified-empty", "historical-failure", "failure-with-missing-metadata"),
)
def test_unavailable_or_empty_files_never_become_positive_file_evidence(
    project: dict,
    files: list[dict],
    files_fetch_failed: bool,
    expected_tier: str,
) -> None:
    """Empty evidence and historical fetch failure remain distinct but score no file flags."""
    result = compute_audit(
        _ACCESSION,
        project,
        files,
        files_fetch_failed=files_fetch_failed,
    )

    assert (
        result.has_result_files,
        result.has_psi_results,
        result.has_open_spectra,
        result.has_tabular_quant,
        result.has_sdrf,
        result.has_mztab,
    ) == (False, False, False, False, False, False)
    assert result.files_fetch_failed is files_fetch_failed
    assert result.tier == expected_tier
    assert result.quant_tier == "No Quant"


def test_quantitative_ladder_is_exhaustive_over_every_evidence_combination() -> None:
    """All eight PSI, abundance-table, and CV-metadata combinations score exactly."""
    for has_psi_results, has_tabular_quant, has_quant_metadata in product((False, True), repeat=3):
        files = []
        if has_psi_results:
            files.append(_file("results.mzid", "RESULT"))
        if has_tabular_quant:
            files.append(_file("proteinGroups.txt", "OTHER"))
        project = _project(
            quantificationMethods=[{"accession": "MS:1001834"}] if has_quant_metadata else []
        )

        result = compute_audit(_ACCESSION, project, files)
        if not has_psi_results and not has_tabular_quant:
            expected_tier = "No Quant"
        elif has_psi_results != has_tabular_quant:
            expected_tier = "Partial"
        elif has_quant_metadata:
            expected_tier = "Quant-Complete"
        else:
            expected_tier = "Quant-Ready"

        assert (
            result.has_psi_results,
            result.has_tabular_quant,
            result.has_quant_metadata,
            result.quant_tier,
        ) == (
            has_psi_results,
            has_tabular_quant,
            has_quant_metadata,
            expected_tier,
        )


def test_audit_result_schema_identity_and_logic_version_are_exact() -> None:
    """The result remains directly serializable into the named audit schema."""
    result = compute_audit(" pxd999999 ", _project(), [_file("results.mzid", "RESULT")])

    assert dataclasses.is_dataclass(result)
    assert {field.name for field in dataclasses.fields(AuditResult)} == set(_AUDIT_COLS)
    assert result.accession == "PXD999999"
    assert result.tier_logic_version == _TIER_LOGIC_VERSION == "v2.1"
