"""Tests for pxaudit.pride_client.

All HTTP interactions are mocked. No live API calls are made anywhere in this
module. The mock strategy patches ``pxaudit.pride_client.requests.Session``
(the class) and ``pxaudit.pride_client.time`` so both network and sleep calls
are completely under test control.

Each test asserts the *positive* and *negative* case where applicable:
  - correct return value on success
  - correct exception type on failure
  - correct call count (retry count proves no extra or missing requests)
  - correct sleep calls (backoff values and sequence)
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from unittest.mock import Mock, call, patch

import pytest
import requests

from pxaudit.pride_client import (
    _BACKOFF_BASE,
    _BASE_URL,
    _CONNECT_TIMEOUT,
    _MAX_RETRIES,
    _MAX_RETRY_DELAY,
    _PAGE_SIZE,
    _READ_TIMEOUT,
    _USER_AGENT,
    PrideAPIError,
    PrideNotFoundError,
    PrideRateLimitError,
    _retry_after_seconds,
    fetch_files,
    fetch_project,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EXPECTED_TIMEOUT = (_CONNECT_TIMEOUT, _READ_TIMEOUT)  # (30, 60)


def _ok_response(
    json_body: object,
    *,
    headers: dict[str, str] | None = None,
) -> Mock:
    """Return a mock Response with status 200 and the given JSON body."""
    resp = Mock()
    resp.status_code = 200
    resp.headers = headers or {}
    resp.json.return_value = json_body
    return resp


def _error_response(status_code: int) -> Mock:
    """Return a mock Response with the given non-200 status code."""
    resp = Mock()
    resp.status_code = status_code
    resp.headers = {}
    return resp


def _setup_session(
    MockSession: type[Mock],
    *,
    responses: Sequence[Mock] | None = None,
    side_effect: Exception | None = None,
) -> Mock:
    """
    Wire up a MockSession instance so headers is a real dict (for introspection)
    and .get() either returns responses in sequence or raises side_effect.
    """
    inst = MockSession.return_value
    inst.headers = {}
    if side_effect is not None:
        inst.get.side_effect = side_effect
    elif responses is not None:
        if len(responses) == 1:
            inst.get.return_value = responses[0]
        else:
            inst.get.side_effect = responses
    return inst


# ---------------------------------------------------------------------------
# 1. URL routing : confirms the real PRIDE v3 API paths are used
# ---------------------------------------------------------------------------


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_fetch_project_calls_correct_url(mock_time: Mock, MockSession: Mock) -> None:
    inst = _setup_session(MockSession, responses=[_ok_response({"accession": "PXD000001"})])
    fetch_project("pxd000001", delay=0)
    inst.get.assert_called_once_with(
        f"{_BASE_URL}/projects/PXD000001",
        timeout=_EXPECTED_TIMEOUT,
    )


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_fetch_files_calls_correct_url(mock_time: Mock, MockSession: Mock) -> None:
    inst = _setup_session(MockSession, responses=[_ok_response([{"fileName": "f.raw"}])])
    fetch_files("PXD000001", delay=0)
    inst.get.assert_called_once_with(
        f"{_BASE_URL}/projects/PXD000001/files"
        "?page=0&pageSize=100&sortDirection=DESC&sortCondition=id",
        timeout=_EXPECTED_TIMEOUT,
    )


# ---------------------------------------------------------------------------
# 2. Successful responses
# ---------------------------------------------------------------------------


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_fetch_project_200_returns_dict(mock_time: Mock, MockSession: Mock) -> None:
    payload = {"accession": "PXD000001", "title": "Test Study"}
    _setup_session(MockSession, responses=[_ok_response(payload)])
    result = fetch_project("PXD000001", delay=0)
    assert result == payload
    assert isinstance(result, dict)


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_fetch_files_200_returns_list(mock_time: Mock, MockSession: Mock) -> None:
    payload = [{"fileName": "data.raw", "fileCategory": {"name": "RAW"}}]
    _setup_session(MockSession, responses=[_ok_response(payload)])
    result = fetch_files("PXD000001", delay=0)
    assert result == payload
    assert isinstance(result, list)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# 3. HTTP 404 : raises immediately, no retry
# ---------------------------------------------------------------------------


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_404_raises_not_found_error(mock_time: Mock, MockSession: Mock) -> None:
    _setup_session(MockSession, responses=[_error_response(404)])
    with pytest.raises(PrideNotFoundError):
        fetch_project("PXD000001", delay=0)
    # PrideNotFoundError IS-A PrideAPIError : catches must work via either type
    with pytest.raises(PrideAPIError):
        fetch_project("PXD000001", delay=0)


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_404_call_count_is_one_no_retry(mock_time: Mock, MockSession: Mock) -> None:
    inst = _setup_session(MockSession, responses=[_error_response(404)])
    with pytest.raises(PrideNotFoundError):
        fetch_project("PXD000001", delay=0)
    assert inst.get.call_count == 1, "404 must not trigger any retries"


# ---------------------------------------------------------------------------
# 4. HTTP 429 : retries with backoff, then raises PrideRateLimitError
# ---------------------------------------------------------------------------


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_429_raises_rate_limit_error(mock_time: Mock, MockSession: Mock) -> None:
    _setup_session(MockSession, responses=[_error_response(429)] * (_MAX_RETRIES + 1))
    with pytest.raises(PrideRateLimitError):
        fetch_project("PXD000001", delay=0)
    # PrideRateLimitError IS-A PrideAPIError : same IS-A check as for 404
    _setup_session(MockSession, responses=[_error_response(429)] * (_MAX_RETRIES + 1))
    with pytest.raises(PrideAPIError):
        fetch_project("PXD000001", delay=0)


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_429_retry_count_is_three(mock_time: Mock, MockSession: Mock) -> None:
    """429 is retried: 1 initial attempt + 2 retries = 3 total GET calls."""
    inst = _setup_session(MockSession, responses=[_error_response(429)] * (_MAX_RETRIES + 1))
    with pytest.raises(PrideRateLimitError):
        fetch_project("PXD000001", delay=0)
    assert inst.get.call_count == _MAX_RETRIES + 1


# ---------------------------------------------------------------------------
# 5. HTTP 500 : retries, then raises PrideAPIError
# ---------------------------------------------------------------------------


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_500_raises_api_error_after_retries_exhausted(mock_time: Mock, MockSession: Mock) -> None:
    _setup_session(MockSession, responses=[_error_response(500)] * (_MAX_RETRIES + 1))
    with pytest.raises(PrideAPIError) as exc_info:
        fetch_project("PXD000001", delay=0)
    # Must not be the more-specific subclasses; it's the base PrideAPIError
    assert type(exc_info.value) is PrideAPIError


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_500_retry_count_is_three(mock_time: Mock, MockSession: Mock) -> None:
    """1 initial attempt + 2 retries = 3 total GET calls."""
    inst = _setup_session(MockSession, responses=[_error_response(500)] * (_MAX_RETRIES + 1))
    with pytest.raises(PrideAPIError):
        fetch_project("PXD000001", delay=0)
    assert inst.get.call_count == _MAX_RETRIES + 1


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_500_then_200_succeeds_without_error(mock_time: Mock, MockSession: Mock) -> None:
    """A transient 500 followed by a 200 on retry must succeed."""
    payload = {"accession": "PXD000001"}
    inst = _setup_session(
        MockSession,
        responses=[_error_response(500), _ok_response(payload)],
    )
    result = fetch_project("PXD000001", delay=0)
    assert result == payload
    assert inst.get.call_count == 2


# ---------------------------------------------------------------------------
# 6. Timeout : retries, then raises PrideAPIError
# ---------------------------------------------------------------------------


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_timeout_raises_api_error_after_retries_exhausted(
    mock_time: Mock, MockSession: Mock
) -> None:
    _setup_session(MockSession, side_effect=requests.Timeout())
    with pytest.raises(PrideAPIError) as exc_info:
        fetch_project("PXD000001", delay=0)
    # The chained cause must be the original Timeout
    assert isinstance(exc_info.value.__cause__, requests.Timeout)


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_connection_error_raises_pride_api_error(mock_time: Mock, MockSession: Mock) -> None:
    """Proxy/connection failures become PrideAPIError after retries."""
    _setup_session(MockSession, side_effect=requests.ConnectionError("proxy down"))
    with pytest.raises(PrideAPIError, match="PRIDE API request failed"):
        fetch_project("PXD000001", delay=0)


@pytest.mark.parametrize(
    ("responses", "message", "cause_type"),
    [
        (
            [_error_response(500), requests.Timeout(), requests.Timeout()],
            "request failed after",
            requests.Timeout,
        ),
        (
            [requests.Timeout(), _error_response(503), _error_response(503)],
            "HTTP 503",
            None,
        ),
    ],
)
@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_retry_exhaustion_reports_the_final_failure(
    mock_time: Mock,
    MockSession: Mock,
    responses: list[Mock | Exception],
    message: str,
    cause_type: type[Exception] | None,
) -> None:
    """Mixed retries report the failure from the final attempt."""
    inst = _setup_session(MockSession)
    inst.get.side_effect = responses

    with pytest.raises(PrideAPIError, match=message) as exc_info:
        fetch_project("PXD000001", delay=0)

    if cause_type is None:
        assert exc_info.value.__cause__ is None
    else:
        assert isinstance(exc_info.value.__cause__, cause_type)


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_timeout_retry_count_is_three(mock_time: Mock, MockSession: Mock) -> None:
    """Repeated timeouts must trigger the full 3-attempt cycle."""
    inst = _setup_session(MockSession, side_effect=requests.Timeout())
    with pytest.raises(PrideAPIError):
        fetch_project("PXD000001", delay=0)
    assert inst.get.call_count == _MAX_RETRIES + 1


@pytest.mark.parametrize(
    "exc_cls",
    [requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout],
)
@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_connect_timeout_and_read_timeout_both_retry(
    mock_time: Mock, MockSession: Mock, exc_cls: type
) -> None:
    """Both ConnectTimeout and ReadTimeout are subclasses of Timeout : both retry."""
    inst = _setup_session(MockSession, side_effect=exc_cls())
    with pytest.raises(PrideAPIError):
        fetch_project("PXD000001", delay=0)
    assert inst.get.call_count == _MAX_RETRIES + 1, f"{exc_cls.__name__} must retry"


# ---------------------------------------------------------------------------
# 7. Sleep / backoff : proves delay and exponential backoff are called correctly
# ---------------------------------------------------------------------------


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_politeness_delay_called_once_before_first_attempt(
    mock_time: Mock, MockSession: Mock
) -> None:
    _setup_session(MockSession, responses=[_ok_response({})])
    fetch_project("PXD000001", delay=0.5)
    # First sleep call is the politeness delay; no backoff on first attempt
    first_call = mock_time.sleep.call_args_list[0]
    assert first_call == call(0.5)
    assert mock_time.sleep.call_count == 1  # no backoff on success


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_backoff_sleep_sequence_on_500_retries(mock_time: Mock, MockSession: Mock) -> None:
    """Backoff must be: delay=0.5, then 1.0 s, then 2.0 s (no sleep before attempt 0)."""
    _setup_session(MockSession, responses=[_error_response(500)] * 3)
    with pytest.raises(PrideAPIError):
        fetch_project("PXD000001", delay=0.5)
    expected = [call(0.5), call(_BACKOFF_BASE * 1), call(_BACKOFF_BASE * 2)]
    assert mock_time.sleep.call_args_list == expected


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_delay_zero_produces_one_sleep_call(mock_time: Mock, MockSession: Mock) -> None:
    """delay=0 still calls sleep(0) once; backoff calls on retry only."""
    _setup_session(MockSession, responses=[_ok_response({})])
    fetch_project("PXD000001", delay=0)
    assert mock_time.sleep.call_args_list == [call(0)]


# ---------------------------------------------------------------------------
# 8. User-Agent header
# ---------------------------------------------------------------------------


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_user_agent_header_is_set_on_session(mock_time: Mock, MockSession: Mock) -> None:
    """The User-Agent must be set on every session before the GET is made."""
    inst = _setup_session(MockSession, responses=[_ok_response({})])
    fetch_project("PXD000001", delay=0)
    assert inst.headers["User-Agent"] == _USER_AGENT


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_user_agent_contains_version_and_repo(mock_time: Mock, MockSession: Mock) -> None:
    from pxaudit import __version__

    assert __version__ in _USER_AGENT
    assert "LangeLab/PXAudit" in _USER_AGENT
    assert _USER_AGENT.startswith("pxaudit/")


# ---------------------------------------------------------------------------
# 9. fetch_files-specific checks
# ---------------------------------------------------------------------------


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_fetch_files_404_raises_not_found(mock_time: Mock, MockSession: Mock) -> None:
    _setup_session(MockSession, responses=[_error_response(404)])
    with pytest.raises(PrideNotFoundError):
        fetch_files("PXD000001", delay=0)


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_fetch_files_500_retries_three_times(mock_time: Mock, MockSession: Mock) -> None:
    inst = _setup_session(MockSession, responses=[_error_response(500)] * 3)
    with pytest.raises(PrideAPIError):
        fetch_files("PXD000001", delay=0)
    assert inst.get.call_count == _MAX_RETRIES + 1


# ---------------------------------------------------------------------------
# 10. fetch_files pagination (ISS-004)
# ---------------------------------------------------------------------------


def _make_page(n: int, offset: int = 0) -> list[dict]:
    """Return a list of *n* minimal file dicts, with unique file names."""
    return [{"fileName": f"file_{offset + i}.raw"} for i in range(n)]


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_fetch_files_single_page_returns_all_items(mock_time: Mock, MockSession: Mock) -> None:
    """A single page with fewer than page_size items → one request, correct count."""
    payload = _make_page(3)
    inst = _setup_session(MockSession, responses=[_ok_response(payload)])
    result = fetch_files("PXD000001", delay=0)
    assert inst.get.call_count == 1
    assert result == payload


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_fetch_files_two_pages_concatenated(mock_time: Mock, MockSession: Mock) -> None:
    """Full first page (100) + partial second page → both batches accumulated."""
    page0 = _make_page(_PAGE_SIZE, offset=0)
    page1 = _make_page(7, offset=_PAGE_SIZE)
    inst = _setup_session(MockSession, responses=[_ok_response(page0), _ok_response(page1)])
    result = fetch_files("PXD000001", delay=0)
    assert inst.get.call_count == 2
    assert len(result) == _PAGE_SIZE + 7
    assert result == page0 + page1


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_fetch_files_three_pages_concatenated(mock_time: Mock, MockSession: Mock) -> None:
    """Two full pages + one final partial page → three requests, correct total."""
    page0 = _make_page(_PAGE_SIZE, offset=0)
    page1 = _make_page(_PAGE_SIZE, offset=_PAGE_SIZE)
    page2 = _make_page(1, offset=2 * _PAGE_SIZE)
    inst = _setup_session(
        MockSession,
        responses=[_ok_response(page0), _ok_response(page1), _ok_response(page2)],
    )
    result = fetch_files("PXD000001", delay=0)
    assert inst.get.call_count == 3
    assert len(result) == 2 * _PAGE_SIZE + 1


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_fetch_files_empty_first_page_returns_empty_list(
    mock_time: Mock, MockSession: Mock
) -> None:
    """A project with no files returns [], makes exactly one request."""
    inst = _setup_session(MockSession, responses=[_ok_response([])])
    result = fetch_files("PXD000001", delay=0)
    assert inst.get.call_count == 1
    assert result == []


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_fetch_files_page_url_increments(mock_time: Mock, MockSession: Mock) -> None:
    """Consecutive pages must use page=0 then page=1 in the URL."""
    page0 = _make_page(_PAGE_SIZE)
    page1 = _make_page(1, offset=_PAGE_SIZE)
    inst = _setup_session(MockSession, responses=[_ok_response(page0), _ok_response(page1)])
    fetch_files("PXD000001", delay=0)
    urls_called = [c.args[0] for c in inst.get.call_args_list]
    assert "page=0" in urls_called[0]
    assert "page=1" in urls_called[1]
    assert "pageSize=100" in urls_called[0]
    assert "pageSize=100" in urls_called[1]


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_fetch_files_delay_passed_per_page(mock_time: Mock, MockSession: Mock) -> None:
    """The politeness delay is forwarded to _request for every page."""
    from unittest.mock import call

    page0 = _make_page(_PAGE_SIZE)
    page1 = _make_page(1, offset=_PAGE_SIZE)
    _setup_session(MockSession, responses=[_ok_response(page0), _ok_response(page1)])
    fetch_files("PXD000001", delay=0.25)
    # time.sleep(0.25) called once per page (inside _request before each attempt)
    sleep_calls = mock_time.sleep.call_args_list
    assert sleep_calls.count(call(0.25)) == 2


# ---------------------------------------------------------------------------
# 11. Response validation, retry policy, and resource ownership
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status_code", [400, 401, 403, 409])
@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_permanent_client_errors_are_not_retried(
    mock_time: Mock,
    MockSession: Mock,
    status_code: int,
) -> None:
    """Permanent HTTP 4xx responses fail after one request."""
    inst = _setup_session(MockSession, responses=[_error_response(status_code)])
    with pytest.raises(PrideAPIError, match=f"HTTP {status_code}"):
        fetch_project("PXD000001", delay=0)
    assert inst.get.call_count == 1


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_nonretryable_request_exception_fails_immediately(
    mock_time: Mock,
    MockSession: Mock,
) -> None:
    """A request construction failure is not treated as a transient connection error."""
    inst = _setup_session(MockSession, side_effect=requests.exceptions.InvalidURL("bad URL"))
    with pytest.raises(PrideAPIError, match="request failed"):
        fetch_project("PXD000001", delay=0)
    assert inst.get.call_count == 1


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_retry_after_header_controls_429_wait(mock_time: Mock, MockSession: Mock) -> None:
    """A valid Retry-After delta replaces exponential backoff for HTTP 429."""
    rate_limited = _error_response(429)
    rate_limited.headers = {"Retry-After": "7"}
    _setup_session(MockSession, responses=[rate_limited, _ok_response({})])
    assert fetch_project("PXD000001", delay=0) == {}
    assert mock_time.sleep.call_args_list == [call(0), call(7.0)]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("invalid", None),
        ("0", 0.0),
        ("999999", _MAX_RETRY_DELAY),
        ("Wed, 21 Oct 2015 07:28:00 GMT", 0.0),
        ("Wed, 21 Oct 2015 07:28:00", None),
    ],
)
def test_retry_after_parsing_is_bounded(value: str | None, expected: float | None) -> None:
    """Delta and HTTP-date retry values are parsed without allowing unbounded sleeps."""
    assert _retry_after_seconds(value) == expected


@pytest.mark.parametrize(
    ("operation", "payload", "message"),
    [
        (fetch_project, [], "project response must be a JSON object"),
        (fetch_files, {}, "files response must be a JSON list"),
        (fetch_files, ["not-an-object"], "files response must be a JSON list"),
    ],
)
@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_successful_response_outer_shape_is_validated(
    mock_time: Mock,
    MockSession: Mock,
    operation: Callable[..., object],
    payload: object,
    message: str,
) -> None:
    """HTTP 200 does not bypass endpoint-specific response-shape validation."""
    _setup_session(MockSession, responses=[_ok_response(payload)])
    with pytest.raises(PrideAPIError, match=message):
        operation("PXD000001", delay=0)


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_invalid_json_is_wrapped_without_retry(mock_time: Mock, MockSession: Mock) -> None:
    """A decoding failure from HTTP 200 becomes a typed API error."""
    response = _ok_response({})
    response.json.side_effect = requests.exceptions.JSONDecodeError("bad", "{", 0)
    inst = _setup_session(MockSession, responses=[response])
    with pytest.raises(PrideAPIError, match="invalid JSON"):
        fetch_project("PXD000001", delay=0)
    assert inst.get.call_count == 1


@pytest.mark.parametrize(
    ("operation", "payload"),
    [
        (fetch_project, {}),
        (fetch_files, []),
    ],
)
@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_session_closes_after_success(
    mock_time: Mock,
    MockSession: Mock,
    operation: Callable[..., object],
    payload: object,
) -> None:
    """Every public fetch closes its owned session after success."""
    inst = _setup_session(MockSession, responses=[_ok_response(payload)])
    operation("PXD000001", delay=0)
    inst.close.assert_called_once()


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_session_closes_after_retry_exhaustion(mock_time: Mock, MockSession: Mock) -> None:
    """Retry exhaustion still closes the project session."""
    inst = _setup_session(MockSession, side_effect=requests.Timeout())
    with pytest.raises(PrideAPIError):
        fetch_project("PXD000001", delay=0)
    inst.close.assert_called_once()


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_session_closes_after_decode_error(mock_time: Mock, MockSession: Mock) -> None:
    """JSON decoding failure still closes the project session."""
    response = _ok_response({})
    response.json.side_effect = json.JSONDecodeError("bad", "{", 0)
    inst = _setup_session(MockSession, responses=[response])
    with pytest.raises(PrideAPIError):
        fetch_project("PXD000001", delay=0)
    inst.close.assert_called_once()


@patch("pxaudit.pride_client.requests.Session")
def test_direct_pride_fetch_rejects_partner_accession_before_session(MockSession: Mock) -> None:
    """The PRIDE transport boundary cannot route a partner identifier to a PRIDE URL."""
    with pytest.raises(ValueError, match="require a PXD"):
        fetch_project("MSV000079514", delay=0)
    MockSession.assert_not_called()


# ---------------------------------------------------------------------------
# 12. Pagination termination and corruption guards
# ---------------------------------------------------------------------------


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_exact_multiple_uses_total_records_without_empty_request(
    mock_time: Mock,
    MockSession: Mock,
) -> None:
    """Record-count metadata terminates a two-page exact multiple."""
    page0 = _ok_response(_make_page(_PAGE_SIZE), headers={"total_records": "200"})
    page1 = _ok_response(_make_page(_PAGE_SIZE, _PAGE_SIZE), headers={"total_records": "200"})
    inst = _setup_session(MockSession, responses=[page0, page1])
    assert len(fetch_files("PXD000001", delay=0)) == 200
    assert inst.get.call_count == 2


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_exact_multiple_without_metadata_requests_empty_final_page(
    mock_time: Mock,
    MockSession: Mock,
) -> None:
    """An empty final page safely terminates exact multiples without metadata."""
    full = _ok_response(_make_page(_PAGE_SIZE))
    inst = _setup_session(MockSession, responses=[full, _ok_response([])])
    assert len(fetch_files("PXD000001", delay=0)) == _PAGE_SIZE
    assert inst.get.call_count == 2


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_repeated_full_page_raises_typed_error_and_closes_session(
    mock_time: Mock,
    MockSession: Mock,
) -> None:
    """A stuck API page cannot duplicate records or loop indefinitely."""
    page = _make_page(_PAGE_SIZE)
    inst = _setup_session(MockSession, responses=[_ok_response(page), _ok_response(page)])
    with pytest.raises(PrideAPIError, match="repeated"):
        fetch_files("PXD000001", delay=0)
    inst.close.assert_called_once()


@patch("pxaudit.pride_client._MAX_PAGES", 2)
@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_maximum_page_guard_rejects_unterminated_pagination(
    mock_time: Mock,
    MockSession: Mock,
) -> None:
    """Distinct full pages cannot exceed the configured pagination bound."""
    inst = _setup_session(
        MockSession,
        responses=[_ok_response(_make_page(100)), _ok_response(_make_page(100, 100))],
    )
    with pytest.raises(PrideAPIError, match="exceeded 2 pages"):
        fetch_files("PXD000001", delay=0)
    inst.close.assert_called_once()


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_failure_after_successful_page_returns_no_partial_result(
    mock_time: Mock,
    MockSession: Mock,
) -> None:
    """A later page failure raises and closes instead of returning partial files."""
    inst = _setup_session(MockSession)
    inst.get.side_effect = [
        _ok_response(_make_page(100)),
        requests.ConnectionError("down"),
        requests.ConnectionError("down"),
        requests.ConnectionError("down"),
    ]
    with pytest.raises(PrideAPIError):
        fetch_files("PXD000001", delay=0)
    inst.close.assert_called_once()


@pytest.mark.parametrize("header", ["invalid", "-1", "+2", "1_0", " 2 "])
@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_invalid_total_records_metadata_is_rejected(
    mock_time: Mock,
    MockSession: Mock,
    header: str,
) -> None:
    """Non-decimal record-count headers are typed API failures."""
    _setup_session(MockSession, responses=[_ok_response([], headers={"total_records": header})])
    with pytest.raises(PrideAPIError, match="total_records"):
        fetch_files("PXD000001", delay=0)


@patch("pxaudit.pride_client._MAX_PAGES", 2)
@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_total_records_above_safety_capacity_is_rejected(
    mock_time: Mock,
    MockSession: Mock,
) -> None:
    """Declared file counts cannot exceed the bounded request capacity."""
    response = _ok_response(_make_page(100), headers={"total_records": "201"})
    _setup_session(MockSession, responses=[response])
    with pytest.raises(PrideAPIError, match="safety limit"):
        fetch_files("PXD000001", delay=0)


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_total_records_must_remain_stable_across_pages(
    mock_time: Mock,
    MockSession: Mock,
) -> None:
    """Changing count metadata cannot define a trustworthy termination point."""
    page0 = _ok_response(_make_page(100), headers={"total_records": "200"})
    page1 = _ok_response(_make_page(100, 100), headers={"total_records": "201"})
    _setup_session(MockSession, responses=[page0, page1])
    with pytest.raises(PrideAPIError, match="changed"):
        fetch_files("PXD000001", delay=0)


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_page_cannot_exceed_declared_total(mock_time: Mock, MockSession: Mock) -> None:
    """More returned records than declared metadata is rejected."""
    response = _ok_response(_make_page(2), headers={"total_records": "1"})
    _setup_session(MockSession, responses=[response])
    with pytest.raises(PrideAPIError, match="more files"):
        fetch_files("PXD000001", delay=0)


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_short_page_cannot_end_before_declared_total(mock_time: Mock, MockSession: Mock) -> None:
    """A premature short page is an incomplete response rather than a valid result."""
    response = _ok_response(_make_page(1), headers={"total_records": "2"})
    _setup_session(MockSession, responses=[response])
    with pytest.raises(PrideAPIError, match="before total_records"):
        fetch_files("PXD000001", delay=0)
