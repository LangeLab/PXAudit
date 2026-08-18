"""Programmatic entry points for PXAudit audits."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from pxaudit.file_classifier import FileClass
from pxaudit.tier_engine import AuditResult, FlagOutcome, compute_audit

__all__ = [
    "AuditResult",
    "FileClass",
    "FlagOutcome",
    "audit_accessions",
    "check_accession",
    "compute_audit",
]


def check_accession(
    accession: str,
    *,
    db_path: str | Path = "pxaudit_results.db",
    no_cache: bool = False,
    refresh: bool = False,
    cache_dir: str | Path | None = None,
    cache_ttl_seconds: float | None = None,
    request_delay: float = 0.5,
) -> AuditResult:
    """Audit one accession and persist the completed result to SQLite.

    Parameters
    ----------
    accession:
        Proteomics Exchange accession to audit. Input is normalized using the same
        rules as the command-line interface.
    db_path:
        SQLite database path. The default is ``pxaudit_results.db``.
    no_cache:
        Skip all cache reads and writes for this audit.
    refresh:
        Skip fresh cache reads while retaining successful writes and stale fallback.
    cache_dir:
        Optional directory for JSON API cache entries.
    cache_ttl_seconds:
        Optional fresh-cache lifetime in seconds. ``None`` uses the cache default.
    request_delay:
        Seconds to wait before each PRIDE request.

    Returns
    -------
    AuditResult
        The computed audit result after the database write succeeds.

    Raises
    ------
    InvalidAccessionError
        If ``accession`` does not satisfy the supported identifier grammar.
    PrideAPIError
        If required PRIDE responses cannot be retrieved or validated.
    OSError
        If the cache or database cannot be accessed.
    sqlite3.DatabaseError
        If the audit cannot be persisted.

    Notes
    -----
    This function performs the same local persistence and remote-fetch work as
    ``pxaudit check``. It does not print terminal output.
    """
    from pxaudit.cli import _audit_single

    return _audit_single(
        accession,
        str(db_path),
        no_cache=no_cache,
        refresh=refresh,
        cache_dir=cache_dir,
        cache_ttl_seconds=cache_ttl_seconds,
        request_delay=request_delay,
    ).result


def audit_accessions(
    accessions: Iterable[str],
    *,
    db_path: str | Path = "pxaudit_results.db",
    no_cache: bool = False,
    refresh: bool = False,
    cache_dir: str | Path | None = None,
    cache_ttl_seconds: float | None = None,
    request_delay: float = 0.5,
) -> list[AuditResult]:
    """Audit accessions sequentially and return their completed results.

    Parameters
    ----------
    accessions:
        Iterable of Proteomics Exchange accessions. Iterables are consumed once in
        their supplied order.
    db_path:
        SQLite database path shared by all audits. The default is ``pxaudit_results.db``.
    no_cache:
        Skip all cache reads and writes for every audit.
    refresh:
        Skip fresh cache reads for every audit while retaining successful writes and
        stale fallback.
    cache_dir:
        Optional directory for JSON API cache entries.
    cache_ttl_seconds:
        Optional fresh-cache lifetime in seconds. ``None`` uses the cache default.
    request_delay:
        Seconds to wait before each PRIDE request.

    Returns
    -------
    list[AuditResult]
        Results in the same order as the supplied accessions.

    Raises
    ------
    InvalidAccessionError
        If an accession does not satisfy the supported identifier grammar.
    PrideAPIError
        If a required PRIDE response cannot be retrieved or validated.
    OSError
        If the cache or database cannot be accessed.
    sqlite3.DatabaseError
        If an audit cannot be persisted.

    Notes
    -----
    Auditing stops at the first error. Use the CLI ``bulk-audit --continue-on-error``
    command when per-accession failure recovery and export reporting are required.
    """
    return [
        check_accession(
            accession,
            db_path=db_path,
            no_cache=no_cache,
            refresh=refresh,
            cache_dir=cache_dir,
            cache_ttl_seconds=cache_ttl_seconds,
            request_delay=request_delay,
        )
        for accession in accessions
    ]
