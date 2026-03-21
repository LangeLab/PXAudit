"""Tests for pxaudit.tier_engine.

Coverage target: 100% branch coverage on tier_engine.py.

Test organisation
-----------------
1.  Invalid accession → ValueError
2.  Non-PXD prefix routing → Unverifiable
3.  Tier boundary (parametrized): Gold / Silver / Bronze×2 / None×3
4.  files_fetch_failed override — caps tier at Bronze
5.  SDRF pattern — case sensitivity and token-boundary checks
6.  mzTab extension — matches .mztab variants, rejects .mztabdata
7.  fileCategory canonical matching — RESULT/SEARCH recognised, RESULTS not
8.  Null / empty project-data inputs — graceful False flags
9.  Empty files_data list — all file flags False
10. AuditResult structure — fields match _AUDIT_COLS; tier_logic_version correct
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
# Helpers — build minimal synthetic API payloads
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
    """Result + SDRF + mzTab — satisfies all file-based flags for Gold."""
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
        # Silver: everything except SDRF
        (
            _project(),
            _result_files(),
            "Silver",
        ),
        # Bronze case 1: organism_id missing, but result files present
        (
            _project(organism_id=None),
            _result_files(),
            "Bronze",
        ),
        # Bronze case 2: organism_id present but no result/search category files
        (
            _project(),
            [_file("raw.raw", "RAW")],
            "Bronze",
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
        "Bronze-no-organism_id",
        "Bronze-no-result",
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


# Bronze explicitly excludes Silver/Gold.
def test_bronze_has_no_result_files() -> None:
    r = compute_audit("PXD000001", _project(), [_file("raw.raw", "RAW")])
    assert r.tier == "Bronze"
    assert r.has_result_files is False
    # Tier is Bronze — result_files is the single missing element.
    assert r.has_organism_id is True


# None check: title present but empty string must yield has_title=False.
def test_none_tier_empty_string_title() -> None:
    r = compute_audit("PXD000001", _project(title=""), _gold_files())
    assert r.has_title is False
    assert r.tier == "None"


# ---------------------------------------------------------------------------
# 4. files_fetch_failed override
# ---------------------------------------------------------------------------


def test_files_fetch_failed_caps_tier_at_bronze() -> None:
    """All metadata present, files_fetch_failed=True → tier must be Bronze."""
    r = compute_audit("PXD000001", _project(), [], files_fetch_failed=True)
    assert r.tier == "Bronze"
    assert r.files_fetch_failed is True


def test_files_fetch_failed_sets_file_flags_false() -> None:
    """Even if files_data is non-empty, files_fetch_failed overrides all file flags."""
    r = compute_audit("PXD000001", _project(), _gold_files(), files_fetch_failed=True)
    assert r.has_result_files is False
    assert r.has_sdrf is False
    assert r.has_mztab is False
    # Must not be Silver or Gold
    assert r.tier not in ("Silver", "Gold")


def test_files_fetch_failed_false_with_empty_files_still_bronze() -> None:
    """files_fetch_failed=False but empty files list → file flags False, tier Bronze."""
    r = compute_audit("PXD000001", _project(), [], files_fetch_failed=False)
    assert r.has_result_files is False
    assert r.tier == "Bronze"
    assert r.files_fetch_failed is False


# ---------------------------------------------------------------------------
# 5. SDRF pattern — token boundaries and case sensitivity
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
        ("results.mztab.gz", False),  # compressed — not a bare .mztab
        ("results.mzid", False),
    ],
    ids=["mzTab", "mztab", "MZTAB", "mzTabData", "mztabdata", "mztab-gz", "mzid"],
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
    files = [_file("data.raw", category_value)]
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
    """Caller passes None for project_data — must not raise, all flags False."""
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
    assert r.tier == "Bronze"  # organism_id is present but result_files is not


def test_file_with_none_file_name_handled_gracefully() -> None:
    """A file dict with fileName=None must not raise — treated as empty string."""
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
    assert r.tier_logic_version == _TIER_LOGIC_VERSION
    assert r.tier_logic_version.startswith("v")


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
