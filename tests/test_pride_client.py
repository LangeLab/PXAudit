"""Contract, resilience, and bounded-pagination tests for the PRIDE client."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from unittest.mock import Mock, call

import pytest
import requests

from pxaudit import __version__, pride_client
from pxaudit.accession import InvalidAccessionError
from pxaudit.pride_client import (
    PrideAPIError,
    PrideNotFoundError,
    PrideRateLimitError,
    fetch_files,
    fetch_project,
)

_TIMEOUT = (pride_client._CONNECT_TIMEOUT, pride_client._READ_TIMEOUT)
_Fetch = Callable[..., object]


@dataclass(frozen=True)
class _Transport:
    """Controllable session and clock used by one public client operation."""

    factory: Mock
    session: Mock
    clock: Mock


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch) -> _Transport:
    """Replace network and wall-clock effects with inspectable test doubles."""
    session = Mock()
    session.headers = {}
    factory = Mock(return_value=session)
    clock = Mock()
    monkeypatch.setattr(pride_client.requests, "Session", factory)
    monkeypatch.setattr(pride_client, "time", clock)
    return _Transport(factory, session, clock)


def _response(
    body: object = None,
    *,
    status: int = 200,
    headers: requests.structures.CaseInsensitiveDict[str] | dict[str, str] | None = None,
) -> Mock:
    """Build the minimal response surface consumed by the client."""
    response = Mock(status_code=status)
    response.headers = {} if headers is None else headers
    response.json.return_value = body
    return response


def _outcomes(transport: _Transport, *outcomes: object) -> None:
    """Set the ordered response or exception sequence for session GETs."""
    transport.session.get.side_effect = list(outcomes)


def _page(size: int, offset: int = 0) -> list[dict[str, int]]:
    """Return a page whose record identities remain unique across offsets."""
    return [{"id": offset + index} for index in range(size)]


@pytest.mark.parametrize(
    ("fetch", "accession", "body", "url"),
    [
        (
            fetch_project,
            "pxd000001",
            {"accession": "PXD000001"},
            f"{pride_client._BASE_URL}/projects/PXD000001",
        ),
        (
            fetch_files,
            "PXD000001",
            [{"fileName": "sample.raw"}],
            f"{pride_client._BASE_URL}/projects/PXD000001/files"
            "?page=0&pageSize=100&sortDirection=DESC&sortCondition=id",
        ),
    ],
    ids=("project", "files"),
)
def test_public_fetch_contract(
    transport: _Transport,
    fetch: _Fetch,
    accession: str,
    body: object,
    url: str,
) -> None:
    """Public fetches canonicalize routes and own all transport effects."""
    _outcomes(transport, _response(body))

    assert fetch(accession, delay=0.25) == body
    transport.session.get.assert_called_once_with(url, timeout=_TIMEOUT)
    assert transport.session.headers == {"User-Agent": pride_client._USER_AGENT}
    transport.clock.sleep.assert_called_once_with(0.25)
    transport.session.close.assert_called_once_with()


def test_public_exception_hierarchy_is_stable() -> None:
    """Specialized HTTP failures remain catchable as PRIDE API failures."""
    assert issubclass(PrideNotFoundError, PrideAPIError)
    assert issubclass(PrideRateLimitError, PrideAPIError)


def test_user_agent_identifies_version_and_repository() -> None:
    """The HTTP client identifies its installed version and upstream project."""
    assert __version__ in pride_client._USER_AGENT
    assert "github.com/LangeLab/PXAudit" in pride_client._USER_AGENT


@pytest.mark.parametrize("fetch", [fetch_project, fetch_files], ids=("project", "files"))
def test_partner_accessions_fail_before_session_creation(
    transport: _Transport,
    fetch: _Fetch,
) -> None:
    """Partner accessions never reach PRIDE or allocate a session."""
    with pytest.raises(InvalidAccessionError, match="PXD accession"):
        fetch("MSV000000001", delay=0)

    transport.factory.assert_not_called()


@pytest.mark.parametrize("status", range(400, 500))
def test_every_4xx_status_has_a_bounded_typed_failure(
    transport: _Transport,
    status: int,
) -> None:
    """The complete client-error space is classified without accidental gaps."""
    attempts = pride_client._MAX_RETRIES + 1 if status == 429 else 1
    _outcomes(transport, *(_response(status=status) for _ in range(attempts)))
    expected: type[PrideAPIError] = {
        404: PrideNotFoundError,
        429: PrideRateLimitError,
    }.get(status, PrideAPIError)

    with pytest.raises(PrideAPIError, match=str(status)) as caught:
        fetch_project("PXD000001", delay=0)

    assert type(caught.value) is expected
    assert transport.session.get.call_count == attempts
    expected_sleeps = [call(0), call(1.0), call(2.0)] if status == 429 else [call(0)]
    assert transport.clock.sleep.call_args_list == expected_sleeps
    transport.session.close.assert_called_once_with()


@pytest.mark.parametrize("status", range(500, 600))
def test_every_5xx_status_retries_then_preserves_status(
    transport: _Transport,
    status: int,
) -> None:
    """Every server-error status follows the same finite retry policy."""
    attempts = pride_client._MAX_RETRIES + 1
    _outcomes(transport, *(_response(status=status) for _ in range(attempts)))

    with pytest.raises(PrideAPIError, match=rf"HTTP {status}\b") as caught:
        fetch_project("PXD000001", delay=0)

    assert type(caught.value) is PrideAPIError
    assert transport.session.get.call_count == attempts
    assert transport.clock.sleep.call_args_list == [call(0), call(1.0), call(2.0)]
    transport.session.close.assert_called_once_with()


@pytest.mark.parametrize(
    ("first", "sleeps"),
    [
        (_response(status=503), [call(0), call(1.0)]),
        (requests.ConnectTimeout("connect"), [call(0), call(1.0)]),
        (requests.ConnectionError("reset"), [call(0), call(1.0)]),
        (_response(status=429, headers={"Retry-After": "7"}), [call(0), call(7.0)]),
    ],
    ids=("server", "timeout", "connection", "rate-limit"),
)
def test_transient_failures_recover_with_expected_backoff(
    transport: _Transport,
    first: object,
    sleeps: list[object],
) -> None:
    """Each transient failure class can recover on the next bounded attempt."""
    project = {"accession": "PXD000001"}
    _outcomes(transport, first, _response(project))

    assert fetch_project("PXD000001", delay=0) == project
    assert transport.clock.sleep.call_args_list == sleeps
    assert transport.session.get.call_count == 2
    transport.session.close.assert_called_once_with()


@pytest.mark.parametrize(
    "error_type",
    [requests.ConnectTimeout, requests.ReadTimeout, requests.ConnectionError],
)
def test_transport_retry_exhaustion_preserves_final_cause(
    transport: _Transport,
    error_type: type[requests.RequestException],
) -> None:
    """Exhausted transport retries expose the final actionable cause."""
    errors = [error_type(f"failure-{index}") for index in range(3)]
    _outcomes(transport, *errors)

    with pytest.raises(PrideAPIError, match="after 2 retries") as caught:
        fetch_project("PXD000001", delay=0)

    assert caught.value.__cause__ is errors[-1]
    assert transport.session.get.call_count == 3
    transport.session.close.assert_called_once_with()


def test_transient_failure_classes_share_one_retry_budget(transport: _Transport) -> None:
    """Changing failure class cannot reset the operation's bounded retry budget."""
    unused_success = _response({"accession": "PXD000001"})
    _outcomes(
        transport,
        requests.ConnectTimeout("connect"),
        _response(status=503),
        _response(status=429),
        unused_success,
    )

    with pytest.raises(PrideRateLimitError):
        fetch_project("PXD000001", delay=0)

    assert transport.session.get.call_count == pride_client._MAX_RETRIES + 1
    assert transport.clock.sleep.call_args_list == [call(0), call(1.0), call(2.0)]
    unused_success.json.assert_not_called()
    transport.session.close.assert_called_once_with()


def test_non_retryable_request_error_fails_immediately(transport: _Transport) -> None:
    """Configuration-level request failures are not hidden by retries."""
    error = requests.exceptions.InvalidURL("invalid")
    _outcomes(transport, error)

    with pytest.raises(PrideAPIError, match="request failed") as caught:
        fetch_project("PXD000001", delay=0)

    assert caught.value.__cause__ is error
    transport.session.get.assert_called_once()
    transport.session.close.assert_called_once_with()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        (" 12 ", 12.0),
        ("0", 0.0),
        ("999999", pride_client._MAX_RETRY_DELAY),
        ("-1", None),
        ("1.5", None),
        ("not-a-date", None),
        ("Wed, 21 Oct 2015 07:28:00 GMT", 0.0),
        ("Wed, 21 Oct 2015 07:28:00", None),
        ("\uff11\uff12", None),
        ("9" * 5000, None),
    ],
    ids=(
        "missing",
        "empty",
        "whitespace",
        "zero",
        "capped",
        "negative",
        "fraction",
        "invalid",
        "past-date",
        "naive-date",
        "non-ascii",
        "oversized",
    ),
)
def test_retry_after_parser_accepts_only_bounded_unambiguous_values(
    value: str | None,
    expected: float | None,
) -> None:
    """Retry-After parsing remains bounded and strict for hostile headers."""
    assert pride_client._retry_after_seconds(value) == expected


def test_retry_after_future_date_is_capped() -> None:
    """Far-future HTTP dates cannot force an unbounded sleep."""
    future = format_datetime(datetime.now(UTC) + timedelta(minutes=10), usegmt=True)

    assert pride_client._retry_after_seconds(future) == pride_client._MAX_RETRY_DELAY


@pytest.mark.parametrize(
    "decode_error",
    [
        json.JSONDecodeError("invalid", "x", 0),
        requests.exceptions.JSONDecodeError("invalid", "x", 0),
        UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid"),
    ],
    ids=("stdlib", "requests", "unicode"),
)
def test_json_decode_failures_are_normalized(
    transport: _Transport,
    decode_error: Exception,
) -> None:
    """All decoder failure modes cross the API boundary as one typed error."""
    response = _response({})
    response.json.side_effect = decode_error
    _outcomes(transport, response)

    with pytest.raises(PrideAPIError, match="invalid JSON") as caught:
        fetch_project("PXD000001", delay=0)

    assert caught.value.__cause__ is decode_error
    transport.session.close.assert_called_once_with()


@pytest.mark.parametrize(
    ("fetch", "body", "message"),
    [
        (fetch_project, [], "project response"),
        (fetch_project, None, "project response"),
        (fetch_files, {}, "files response"),
        (fetch_files, [1], "files response"),
        (fetch_files, [{"id": 1}, "bad"], "files response"),
    ],
    ids=("project-list", "project-null", "files-object", "files-item", "files-mixed"),
)
def test_success_status_still_requires_endpoint_shape(
    transport: _Transport,
    fetch: _Fetch,
    body: object,
    message: str,
) -> None:
    """HTTP success cannot bypass endpoint-specific JSON validation."""
    _outcomes(transport, _response(body))

    with pytest.raises(PrideAPIError, match=message):
        fetch("PXD000001", delay=0)

    transport.session.close.assert_called_once_with()


@pytest.mark.parametrize(
    "body",
    [
        {"organisms": ["Homo sapiens"]},
        {"organisms": "Homo sapiens"},
        {"keywords": [123]},
        {"submissionDate": 2024},
    ],
    ids=("organism-item", "organism-list", "keyword-item", "submission-date"),
)
def test_project_success_requires_consumed_nested_fields_to_be_well_formed(
    transport: _Transport, body: dict
) -> None:
    """Malformed project fields fail at the API boundary as typed errors."""
    _outcomes(transport, _response(body))

    with pytest.raises(PrideAPIError, match="invalid"):
        fetch_project("PXD000001", delay=0)

    transport.session.close.assert_called_once_with()


@pytest.mark.parametrize(
    "body",
    [
        [{"fileName": 1}],
        [{"fileCategory": "RESULT"}],
        [{"publicFileLocations": ["ftp"]}],
        [{"fileCategory": {"value": 1}}],
        [{"publicFileLocations": [{"name": 1}]}],
    ],
    ids=("file-name", "file-category", "locations-item", "category-value", "location-name"),
)
def test_files_success_requires_consumed_nested_fields_to_be_well_formed(
    transport: _Transport, body: list[dict]
) -> None:
    """Malformed file fields fail before classification or persistence can crash."""
    _outcomes(transport, _response(body))

    with pytest.raises(PrideAPIError, match="invalid"):
        fetch_files("PXD000001", delay=0)

    transport.session.close.assert_called_once_with()


def _pagination_responses(
    sizes: Sequence[int],
    total: int | None,
) -> tuple[list[Mock], list[dict[str, int]]]:
    """Build sequential pages and their expected flattened records."""
    responses: list[Mock] = []
    expected: list[dict[str, int]] = []
    offset = 0
    headers = {} if total is None else {"total_records": str(total)}
    for size in sizes:
        batch = _page(size, offset)
        responses.append(_response(batch, headers=headers))
        expected.extend(batch)
        offset += size
    return responses, expected


@pytest.mark.parametrize(
    ("sizes", "total"),
    [
        ([0], None),
        ([1], None),
        ([100, 0], None),
        ([100, 7], None),
        ([100, 100, 1], None),
        ([0], 0),
        ([100, 100], 200),
    ],
    ids=("empty", "short", "exact-fallback", "two-page", "three-page", "zero-total", "exact-total"),
)
def test_pagination_termination_matrix(
    transport: _Transport,
    sizes: Sequence[int],
    total: int | None,
) -> None:
    """Empty, partial, and exact pages terminate without loss or an extra request."""
    responses, expected = _pagination_responses(sizes, total)
    _outcomes(transport, *responses)

    assert fetch_files("PXD000001", delay=0.1) == expected
    assert transport.session.get.call_count == len(sizes)
    assert transport.clock.sleep.call_args_list == [call(0.1)] * len(sizes)
    expected_urls = [
        call(
            f"{pride_client._BASE_URL}/projects/PXD000001/files"
            f"?page={page}&pageSize=100&sortDirection=DESC&sortCondition=id",
            timeout=_TIMEOUT,
        )
        for page in range(len(sizes))
    ]
    assert transport.session.get.call_args_list == expected_urls
    transport.session.close.assert_called_once_with()


@pytest.mark.parametrize(
    "value",
    ["", "-1", "+1", "1.0", " 1 ", "\uff11", "1_0", "NaN", "9" * 5000],
    ids=(
        "empty",
        "negative",
        "explicit-positive",
        "fraction",
        "whitespace",
        "non-ascii",
        "separator",
        "not-a-number",
        "oversized",
    ),
)
def test_total_records_rejects_noncanonical_integers(
    transport: _Transport,
    value: str,
) -> None:
    """Malformed count metadata fails before it can steer pagination."""
    _outcomes(transport, _response([], headers={"total_records": value}))

    with pytest.raises(PrideAPIError, match="invalid total_records"):
        fetch_files("PXD000001", delay=0)

    transport.session.close.assert_called_once_with()


def test_total_records_header_lookup_matches_requests_semantics(
    transport: _Transport,
) -> None:
    """Record-count lookup remains case-insensitive like real response headers."""
    headers = requests.structures.CaseInsensitiveDict({"TOTAL_RECORDS": "0"})
    _outcomes(transport, _response([], headers=headers))

    assert fetch_files("PXD000001", delay=0) == []
    transport.session.get.assert_called_once()
    transport.session.close.assert_called_once_with()


def test_semantically_repeated_page_is_detected(transport: _Transport) -> None:
    """Object key order cannot evade the repeated-page safety guard."""
    first = [{"id": index, "size": index + 1} for index in range(100)]
    reordered = [{"size": item["size"], "id": item["id"]} for item in first]
    _outcomes(transport, _response(first), _response(reordered))

    with pytest.raises(PrideAPIError, match="repeated a files page"):
        fetch_files("PXD000001", delay=0)

    assert transport.session.get.call_count == 2
    transport.session.close.assert_called_once_with()


def test_pagination_has_a_hard_page_bound(
    transport: _Transport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A server emitting endless unique full pages cannot cause an endless scan."""
    monkeypatch.setattr(pride_client, "_MAX_PAGES", 2)
    _outcomes(transport, _response(_page(100)), _response(_page(100, 100)))

    with pytest.raises(PrideAPIError, match="exceeded 2 pages"):
        fetch_files("PXD000001", delay=0)

    assert transport.session.get.call_count == 2
    transport.session.close.assert_called_once_with()


@pytest.mark.parametrize(
    ("responses", "message"),
    [
        (
            [_response([], headers={"total_records": "100001"})],
            "count exceeds the pagination safety limit",
        ),
        (
            [
                _response(_page(100), headers={"total_records": "150"}),
                _response([], headers={"total_records": "151"}),
            ],
            "total_records changed",
        ),
        (
            [_response(_page(2), headers={"total_records": "1"})],
            "more files than total_records",
        ),
        (
            [_response(_page(1), headers={"total_records": "2"})],
            "ended before total_records",
        ),
    ],
    ids=("capacity", "changed-total", "too-many", "premature-short"),
)
def test_pagination_metadata_guards_reject_inconsistent_streams(
    transport: _Transport,
    responses: Sequence[Mock],
    message: str,
) -> None:
    """Count metadata cannot permit excess memory use or partial results."""
    _outcomes(transport, *responses)

    with pytest.raises(PrideAPIError, match=message):
        fetch_files("PXD000001", delay=0)

    transport.session.close.assert_called_once_with()


def test_later_page_failure_closes_session_without_partial_result(
    transport: _Transport,
) -> None:
    """A later-page error aborts the operation while releasing its session."""
    _outcomes(transport, _response(_page(100)), _response(status=404))

    with pytest.raises(PrideNotFoundError):
        fetch_files("PXD000001", delay=0)

    assert transport.session.get.call_count == 2
    transport.session.close.assert_called_once_with()
