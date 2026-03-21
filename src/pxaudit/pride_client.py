"""PRIDE Archive REST API client.

Endpoints used (PRIDE REST API v3):
  GET /projects/{accession}          → dict
  GET /projects/{accession}/files    → list[dict]

Both functions return raw parsed JSON; no transformation is applied here.
Transformation of CvParam objects, date strings, etc. is the caller's responsibility.
"""

from __future__ import annotations

import time
from typing import cast

import requests

from pxaudit import __version__

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BASE_URL = "https://www.ebi.ac.uk/pride/ws/archive/v3"
_USER_AGENT = f"pxaudit/{__version__} (https://github.com/LangeLab/PXAudit)"
_CONNECT_TIMEOUT = 30  # seconds
_READ_TIMEOUT = 60  # seconds
_MAX_RETRIES = 2  # total attempts = 1 + _MAX_RETRIES = 3
_BACKOFF_BASE = 1.0  # seconds; doubles on each retry (1s, 2s)

# ---------------------------------------------------------------------------
# Public exceptions
# ---------------------------------------------------------------------------


class PrideAPIError(Exception):
    """Raised when the PRIDE API returns an unexpected or server-side error."""


class PrideNotFoundError(PrideAPIError):
    """Raised when the requested accession is not found (HTTP 404)."""


class PrideRateLimitError(PrideAPIError):
    """Raised when the PRIDE API rate-limits the client (HTTP 429)."""


# ---------------------------------------------------------------------------
# Internal request helper
# ---------------------------------------------------------------------------


def _request(url: str, *, delay: float = 0.5) -> dict | list:
    """Issue a GET request to *url* with retry/backoff logic.

    Parameters
    ----------
    url:
        Full absolute URL to request.
    delay:
        Seconds to sleep before the first attempt (API politeness delay).
        Passed through from ``fetch_project`` / ``fetch_files``; set to ``0``
        to disable (e.g. in integration tests).

    Returns
    -------
    dict | list
        Parsed JSON response body.

    Raises
    ------
    PrideNotFoundError
        On HTTP 404. Never retried.
    PrideRateLimitError
        On HTTP 429. Never retried.
    PrideAPIError
        On 5xx status codes or repeated timeouts once all retries are exhausted.
    """
    time.sleep(delay)

    session = requests.Session()
    session.headers["User-Agent"] = _USER_AGENT

    last_exc: Exception | None = None

    for attempt in range(_MAX_RETRIES + 1):
        if attempt > 0:
            # Exponential backoff: 1 s before retry-1, 2 s before retry-2.
            time.sleep(_BACKOFF_BASE * (2 ** (attempt - 1)))

        try:
            resp = session.get(url, timeout=(_CONNECT_TIMEOUT, _READ_TIMEOUT))
        except requests.Timeout as exc:
            last_exc = exc
            continue

        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 404:
            raise PrideNotFoundError(f"Accession not found (HTTP 404): {url}")
        if resp.status_code == 429:
            raise PrideRateLimitError(f"Rate limited by PRIDE API (HTTP 429): {url}")

        # Any other non-2xx status (typically 5xx) — record and retry.
        last_exc = PrideAPIError(f"HTTP {resp.status_code}: {url}")

    raise PrideAPIError(
        f"PRIDE API request failed after {_MAX_RETRIES} retries: {url}"
    ) from last_exc


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_project(accession: str, *, delay: float = 0.5) -> dict:
    """Fetch project metadata dict from ``/projects/{accession}``.

    Raw JSON is returned unchanged. Raises ``PrideNotFoundError`` if the
    accession does not exist on PRIDE.
    """
    url = f"{_BASE_URL}/projects/{accession}"
    return cast(dict, _request(url, delay=delay))


def fetch_files(accession: str, *, delay: float = 0.5) -> list[dict]:
    """Fetch **all** files from ``/projects/{accession}/files``, paginating until exhausted.

    ISS-004 fix: the original implementation made a single un-paginated request.
    PRIDE's API caps its default page at 100 files; datasets with >100 files were
    silently truncated, causing file-level flags (``has_open_spectra``, etc.) to be
    derived from an incomplete file list and tier scores to be understated.

    The loop requests successive pages of 100 rows until a page returns fewer than
    100 rows, which signals the final (possibly empty) page has been reached.

    Note: No early exit keyed on ``fileCategory`` is used.  Stopping on
    RESULT/EXPERIMENTAL DESIGN categories can skip PEAK files on later pages and
    cause a Platinum/Diamond dataset to be mis-scored as Gold (Audit Issue 1).
    """
    all_files: list[dict] = []
    page = 0
    page_size = 100
    while True:
        url = (
            f"{_BASE_URL}/projects/{accession}/files"
            f"?page={page}&pageSize={page_size}&sortDirection=DESC&sortCondition=id"
        )
        batch = cast(list[dict], _request(url, delay=delay))
        all_files.extend(batch)
        if len(batch) < page_size:
            break
        page += 1
    return all_files
