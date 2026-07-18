"""Bounded client for the PRIDE Archive REST API v3."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from pxaudit import _PRIDE_PREFIX, __version__
from pxaudit.accession import InvalidAccessionError, normalize_accession

_BASE_URL = "https://www.ebi.ac.uk/pride/ws/archive/v3"
_USER_AGENT = f"pxaudit/{__version__} (https://github.com/LangeLab/PXAudit)"
_CONNECT_TIMEOUT = 30
_READ_TIMEOUT = 60
_MAX_RETRIES = 2
_BACKOFF_BASE = 1.0
_MAX_RETRY_DELAY = 60.0
_PAGE_SIZE = 100
_MAX_PAGES = 1000
_RETRYABLE_STATUS_CODES = frozenset(range(500, 600))


class PrideAPIError(Exception):
    """Raised when PRIDE transport or response validation fails."""


class PrideNotFoundError(PrideAPIError):
    """Raised when PRIDE returns HTTP 404 for an accession."""


class PrideRateLimitError(PrideAPIError):
    """Raised when HTTP 429 retries are exhausted."""


@dataclass(frozen=True)
class _JSONResponse:
    """Decoded response data and optional PRIDE record-count metadata."""

    data: Any
    total_records: int | None


def _retry_after_seconds(value: str | None) -> float | None:
    """Return a bounded delay from an HTTP ``Retry-After`` value."""
    if value is None:
        return None
    stripped = value.strip()
    try:
        if stripped.isdecimal():
            seconds = float(int(stripped))
        else:
            retry_at = parsedate_to_datetime(stripped)
            if retry_at.tzinfo is None:
                return None
            seconds = max(0.0, (retry_at.astimezone(UTC) - datetime.now(UTC)).total_seconds())
    except (OverflowError, TypeError, ValueError):
        return None
    return min(seconds, _MAX_RETRY_DELAY)


def _total_records(
    headers: requests.structures.CaseInsensitiveDict[str] | dict[str, str],
) -> int | None:
    """Validate PRIDE's optional ``total_records`` pagination header."""
    value = headers.get("total_records")
    if value is None:
        return None
    if not value.isascii() or not value.isdecimal():
        raise PrideAPIError("PRIDE API returned invalid total_records metadata")
    return int(value)


def _request(url: str, *, delay: float, session: requests.Session) -> _JSONResponse:
    """Issue one bounded GET operation with retries for transient failures.

    HTTP 429, 5xx responses, timeouts, and connection failures are retryable. Other HTTP 4xx
    responses and other request exceptions fail immediately. The caller owns ``session``.

    Parameters
    ----------
    url:
        Absolute PRIDE API URL.
    delay:
        Politeness delay before the first attempt, in seconds.
    session:
        Open session owned by the public fetch operation.

    Returns
    -------
    _JSONResponse
        Decoded JSON and optional record-count metadata.

    Raises
    ------
    PrideNotFoundError
        If PRIDE returns HTTP 404.
    PrideRateLimitError
        If HTTP 429 retries are exhausted.
    PrideAPIError
        If transport, status, decoding, or metadata validation fails.
    """
    time.sleep(delay)
    retry_delay = _BACKOFF_BASE
    last_error: requests.RequestException | PrideAPIError | None = None

    for attempt in range(_MAX_RETRIES + 1):
        if attempt:
            time.sleep(retry_delay)

        try:
            response = session.get(url, timeout=(_CONNECT_TIMEOUT, _READ_TIMEOUT))
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            retry_delay = min(_BACKOFF_BASE * (2**attempt), _MAX_RETRY_DELAY)
            continue
        except requests.RequestException as exc:
            raise PrideAPIError(f"PRIDE API request failed: {exc}") from exc

        if response.status_code == 200:
            try:
                data = response.json()
            except (
                json.JSONDecodeError,
                requests.exceptions.JSONDecodeError,
                UnicodeDecodeError,
            ) as exc:
                raise PrideAPIError("PRIDE API returned invalid JSON") from exc
            return _JSONResponse(data=data, total_records=_total_records(response.headers))

        if response.status_code == 404:
            raise PrideNotFoundError(f"Accession not found (HTTP 404): {url}")
        if response.status_code == 429:
            last_error = PrideRateLimitError(f"Rate limited by PRIDE API (HTTP 429): {url}")
            retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
            retry_delay = (
                retry_after
                if retry_after is not None
                else min(_BACKOFF_BASE * (2**attempt), _MAX_RETRY_DELAY)
            )
            continue
        if response.status_code in _RETRYABLE_STATUS_CODES:
            last_error = PrideAPIError(f"HTTP {response.status_code}: {url}")
            retry_delay = min(_BACKOFF_BASE * (2**attempt), _MAX_RETRY_DELAY)
            continue
        raise PrideAPIError(f"HTTP {response.status_code}: {url}")

    if isinstance(last_error, PrideAPIError):
        raise last_error
    raise PrideAPIError(
        f"PRIDE API request failed after {_MAX_RETRIES} retries: {url}"
    ) from last_error


def _pride_accession(accession: str) -> str:
    """Return a canonical PRIDE accession or reject a partner identifier."""
    canonical = normalize_accession(accession)
    if not canonical.startswith(_PRIDE_PREFIX):
        raise InvalidAccessionError("PRIDE API requests require a PXD accession")
    return canonical


def fetch_project(accession: str, *, delay: float = 0.5) -> dict:
    """Fetch and validate one PRIDE project response.

    Parameters
    ----------
    accession:
        PRIDE accession, canonicalized before URL construction.
    delay:
        Politeness delay before the request, in seconds.

    Returns
    -------
    dict
        Raw project mapping.

    Raises
    ------
    InvalidAccessionError
        If ``accession`` is not a valid PXD identifier.
    PrideAPIError
        If the request fails or a successful response is not a mapping.
    """
    canonical = _pride_accession(accession)
    session = requests.Session()
    try:
        session.headers["User-Agent"] = _USER_AGENT
        response = _request(f"{_BASE_URL}/projects/{canonical}", delay=delay, session=session)
        if not isinstance(response.data, dict):
            raise PrideAPIError("PRIDE project response must be a JSON object")
        return response.data
    finally:
        session.close()


def _page_fingerprint(batch: list[dict]) -> bytes:
    """Return a stable compact identity used to detect repeated pages."""
    encoded = json.dumps(batch, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(encoded).digest()


def fetch_files(accession: str, *, delay: float = 0.5) -> list[dict]:
    """Fetch every file for one PRIDE project with bounded pagination.

    PRIDE's ``total_records`` response header terminates exact-multiple result sets when
    available. Short or empty pages remain the fallback. Repeated pages, inconsistent
    metadata, and more than 1,000 pages fail instead of returning partial or duplicated data.

    Parameters
    ----------
    accession:
        PRIDE accession, canonicalized before URL construction.
    delay:
        Politeness delay before each page request, in seconds.

    Returns
    -------
    list[dict]
        Raw file mappings from every validated page.

    Raises
    ------
    InvalidAccessionError
        If ``accession`` is not a valid PXD identifier.
    PrideAPIError
        If transport, response shape, or pagination validation fails.
    """
    canonical = _pride_accession(accession)
    all_files: list[dict] = []
    page_fingerprints: set[bytes] = set()
    expected_total: int | None = None
    session = requests.Session()

    try:
        session.headers["User-Agent"] = _USER_AGENT
        for page in range(_MAX_PAGES):
            url = (
                f"{_BASE_URL}/projects/{canonical}/files"
                f"?page={page}&pageSize={_PAGE_SIZE}&sortDirection=DESC&sortCondition=id"
            )
            response = _request(url, delay=delay, session=session)
            if not isinstance(response.data, list) or not all(
                isinstance(item, dict) for item in response.data
            ):
                raise PrideAPIError("PRIDE files response must be a JSON list of objects")
            batch = response.data

            if response.total_records is not None:
                if expected_total is None:
                    expected_total = response.total_records
                    if expected_total > _PAGE_SIZE * _MAX_PAGES:
                        raise PrideAPIError("PRIDE file count exceeds the pagination safety limit")
                elif response.total_records != expected_total:
                    raise PrideAPIError("PRIDE total_records changed during pagination")

            if batch:
                fingerprint = _page_fingerprint(batch)
                if fingerprint in page_fingerprints:
                    raise PrideAPIError("PRIDE API repeated a files page during pagination")
                page_fingerprints.add(fingerprint)
                all_files.extend(batch)

            if expected_total is not None:
                if len(all_files) > expected_total:
                    raise PrideAPIError("PRIDE returned more files than total_records")
                if len(all_files) == expected_total:
                    return all_files
                if len(batch) < _PAGE_SIZE:
                    raise PrideAPIError("PRIDE pagination ended before total_records was reached")
            elif len(batch) < _PAGE_SIZE:
                return all_files

        raise PrideAPIError(f"PRIDE files pagination exceeded {_MAX_PAGES} pages")
    finally:
        session.close()


__all__ = [
    "PrideAPIError",
    "PrideNotFoundError",
    "PrideRateLimitError",
    "fetch_files",
    "fetch_project",
]
