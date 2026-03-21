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

from unittest.mock import Mock, call, patch

import pytest
import requests

from pxaudit.pride_client import (
    _BACKOFF_BASE,
    _BASE_URL,
    _CONNECT_TIMEOUT,
    _MAX_RETRIES,
    _READ_TIMEOUT,
    _USER_AGENT,
    PrideAPIError,
    PrideNotFoundError,
    PrideRateLimitError,
    fetch_files,
    fetch_project,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EXPECTED_TIMEOUT = (_CONNECT_TIMEOUT, _READ_TIMEOUT)  # (30, 60)


def _ok_response(json_body: dict | list) -> Mock:
    """Return a mock Response with status 200 and the given JSON body."""
    resp = Mock()
    resp.status_code = 200
    resp.json.return_value = json_body
    return resp


def _error_response(status_code: int) -> Mock:
    """Return a mock Response with the given non-200 status code."""
    resp = Mock()
    resp.status_code = status_code
    return resp


def _setup_session(MockSession: Mock, *, responses=None, side_effect=None) -> Mock:
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
# 1. URL routing — confirms the real PRIDE v3 API paths are used
# ---------------------------------------------------------------------------


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_fetch_project_calls_correct_url(mock_time: Mock, MockSession: Mock) -> None:
    inst = _setup_session(MockSession, responses=[_ok_response({"accession": "PXD000001"})])
    fetch_project("PXD000001", delay=0)
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
        f"{_BASE_URL}/projects/PXD000001/files",
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
# 3. HTTP 404 — raises immediately, no retry
# ---------------------------------------------------------------------------


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_404_raises_not_found_error(mock_time: Mock, MockSession: Mock) -> None:
    _setup_session(MockSession, responses=[_error_response(404)])
    with pytest.raises(PrideNotFoundError):
        fetch_project("PXD000001", delay=0)
    # PrideNotFoundError IS-A PrideAPIError — catches must work via either type
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
# 4. HTTP 429 — raises immediately, no retry
# ---------------------------------------------------------------------------


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_429_raises_rate_limit_error(mock_time: Mock, MockSession: Mock) -> None:
    _setup_session(MockSession, responses=[_error_response(429)])
    with pytest.raises(PrideRateLimitError):
        fetch_project("PXD000001", delay=0)
    # PrideRateLimitError IS-A PrideAPIError — same IS-A check as for 404
    with pytest.raises(PrideAPIError):
        fetch_project("PXD000001", delay=0)


@patch("pxaudit.pride_client.requests.Session")
@patch("pxaudit.pride_client.time")
def test_429_call_count_is_one_no_retry(mock_time: Mock, MockSession: Mock) -> None:
    inst = _setup_session(MockSession, responses=[_error_response(429)])
    with pytest.raises(PrideRateLimitError):
        fetch_project("PXD000001", delay=0)
    assert inst.get.call_count == 1, "429 must not trigger any retries"


# ---------------------------------------------------------------------------
# 5. HTTP 500 — retries, then raises PrideAPIError
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
# 6. Timeout — retries, then raises PrideAPIError
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
    """Both ConnectTimeout and ReadTimeout are subclasses of Timeout — both retry."""
    inst = _setup_session(MockSession, side_effect=exc_cls())
    with pytest.raises(PrideAPIError):
        fetch_project("PXD000001", delay=0)
    assert inst.get.call_count == _MAX_RETRIES + 1, f"{exc_cls.__name__} must retry"


# ---------------------------------------------------------------------------
# 7. Sleep / backoff — proves delay and exponential backoff are called correctly
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
    _setup_session(MockSession, responses=[_ok_response({})])
    fetch_project("PXD000001", delay=0)
    assert "0.1.0" in _USER_AGENT
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
