"""Command-line interface for pxaudit.

Commands
--------
check        Audit a single Proteomics Exchange accession.
bulk-audit   Audit multiple Proteomics Exchange accessions in batch.
manifest     List files for an accession from the audit database.
report       Generate a self-contained HTML report from a populated database.
"""

from __future__ import annotations

import importlib.metadata
import json
import sqlite3
import sys
import time
import typing
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import click
import pandas as pd
from tqdm import tqdm

from pxaudit import _PRIDE_PREFIX
from pxaudit.cache import read_cache, read_cache_stale, write_cache
from pxaudit.db import get_or_create_db, insert_audit_record
from pxaudit.pride_client import PrideAPIError, fetch_files, fetch_project
from pxaudit.tier_engine import AuditResult, compute_audit

__all__ = [
    "main",
    "_audit_single",
    "_default_export_path",
    "_export_csv",
    "_export_json",
    "_export_tsv",
    "_extract_files_df",
    "_extract_study",
    "_print_result",
    "_read_accessions",
    "_result_to_row",
]


class AuditData(typing.NamedTuple):
    """Return type for :func:`_audit_single`.

    Attributes
    ----------
    result:
        Computed audit result with tier, quant tier, and flags.
    study:
        Extracted study metadata dict for the ``study`` table.
    files_df:
        Extracted file metadata DataFrame for the ``study_files`` table.
    files_data:
        Raw file list from the PRIDE API response.
    fetched_at:
        ISO 8601 timestamp of when the data was fetched.
    """

    result: AuditResult
    study: dict
    files_df: pd.DataFrame
    files_data: list[dict]
    fetched_at: str


@click.group()
@click.version_option(importlib.metadata.version("pxaudit"))
def main() -> None:
    """Audit Proteomics Exchange study metadata."""


# ---------------------------------------------------------------------------
# Data extraction helpers
# ---------------------------------------------------------------------------


def _parse_submission_year(date_str: str) -> int | None:
    """Extract the year from an ISO 8601 date string, returning None on failure."""
    if not date_str or len(date_str) < 4:
        return None
    try:
        return int(date_str[:4])
    except ValueError:
        return None


def _extract_study(accession: str, project: dict, fetched_at: str) -> dict:
    """Map a raw PRIDE /projects response to a ``study`` table row dict."""
    organisms: list[dict] = project.get("organisms") or []
    instruments: list[dict] = project.get("instruments") or []
    keywords: list[str] = project.get("keywords") or []
    date_str: str = project.get("submissionDate") or ""
    return {
        "accession": accession,
        "title": project.get("title") or None,
        "organism": organisms[0].get("name") if organisms else None,
        "organism_id": organisms[0].get("accession") if organisms else None,
        "instrument": instruments[0].get("name") if instruments else None,
        "submission_year": _parse_submission_year(date_str),
        "submission_type": project.get("submissionType") or None,
        "keywords": ", ".join(keywords) if keywords else None,
        "repository": "PRIDE",
        "fetched_at": fetched_at,
    }


def _extract_files_df(accession: str, files: list[dict]) -> pd.DataFrame:
    """Map a raw PRIDE /files response to a ``study_files`` DataFrame."""
    cols = [
        "accession",
        "file_name",
        "file_category",
        "file_extension",
        "ftp_location",
        "file_size",
        "checksum",
        "checksum_type",
    ]
    if not files:
        return pd.DataFrame(columns=cols)
    rows = [
        {
            "accession": accession,
            "file_name": f.get("fileName") or "",
            "file_category": (f.get("fileCategory") or {}).get("value") or None,
            "file_extension": Path(f.get("fileName") or "").suffix or None,
            "ftp_location": next(
                (
                    loc.get("value")
                    for loc in (f.get("publicFileLocations") or [])
                    if loc.get("name") == "FTP Protocol"
                ),
                None,
            ),
            "file_size": f.get("fileSizeBytes"),
            "checksum": f.get("fileChecksum") or None,
            "checksum_type": "MD5" if f.get("fileChecksum") else None,
        }
        for f in files
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Output formatter
# ---------------------------------------------------------------------------


def _print_result(result: AuditResult, study: dict, file_count: int) -> None:
    """Print a formatted audit summary to stdout."""
    tick = "\u2714"
    cross = "\u2718"

    def flag(val: bool) -> str:
        return tick if val else cross

    click.echo(f"Accession : {result.accession}")
    click.echo(f"Tier      : {result.tier}")
    click.echo(f"Quant Tier: {result.quant_tier}")
    click.echo("-" * 48)
    click.echo("Metadata")
    title = (study.get("title") or "")[:60]
    click.echo(f"  {flag(result.has_title)} Title         {title}")
    organism = study.get("organism") or ""
    organism_id = study.get("organism_id") or ""
    org_str = f"{organism} ({organism_id})" if organism_id else organism
    click.echo(f"  {flag(result.has_organism)} Organism      {org_str}")
    click.echo(f"  {flag(result.has_instrument)} Instrument    {study.get('instrument') or ''}")
    click.echo(f"  {flag(result.has_organism_part)} Organism part annotated")
    click.echo(f"  {flag(result.has_publication)} Publication   linked")
    click.echo(f"  {flag(result.has_quant_metadata)} Quant metadata (CV methods)")
    click.echo("-" * 48)
    click.echo(f"Files ({file_count} total)")
    click.echo(f"  {flag(result.has_result_files)} Result/Search files present")
    click.echo(f"  {flag(result.has_psi_results)} PSI-standard results (mzIdentML / mzTab-ID)")
    click.echo(f"  {flag(result.has_open_spectra)} Open spectra (mzML / MGF)")
    click.echo(f"  {flag(result.has_sdrf)} SDRF file present")
    click.echo(f"  {flag(result.has_mztab)} mzTab summary present")
    click.echo(f"  {flag(result.has_tabular_quant)} Tabular quant table (proteinGroups / evidence)")
    if result.files_fetch_failed:
        click.echo("  ! Files endpoint failed: file flags are unreliable")
    click.echo("-" * 48)


# ---------------------------------------------------------------------------
# Core audit pipeline (shared by check and bulk-audit)
# ---------------------------------------------------------------------------


def _audit_single(
    accession: str,
    db_path: str,
    *,
    no_cache: bool = False,
    refresh: bool = False,
) -> AuditData:
    """Fetch, compute, persist, and return audit data for one accession.

    On network failure, serves stale cached data with a warning if available.
    Only raises ``PrideAPIError`` when no cache exists at all.

    Parameters
    ----------
    accession:
        PRIDE accession string, e.g. ``"PXD000001"``.
    db_path:
        Path to the SQLite database file.
    no_cache:
        Skip cache reads and writes.
    refresh:
        Skip cache reads but write fresh data.

    Returns
    -------
    AuditData
        Named tuple with result, study, files_df, files_data, and fetched_at.

    Raises
    ------
    PrideAPIError
        If the PRIDE API is unreachable and no cached data is available.
    """
    fetched_at = datetime.now(UTC).isoformat()
    project_data: dict | None = None
    files_data: list[dict] | None = None
    files_fetch_failed = False

    use_cache = not (no_cache or refresh)
    persist_cache = not no_cache  # --no-cache disables writes too
    if accession.upper().startswith(_PRIDE_PREFIX):
        if use_cache:
            project_data = read_cache(accession, "project")  # type: ignore[assignment]
            files_data = read_cache(accession, "files")  # type: ignore[assignment]

        if project_data is None:
            try:
                project_data = fetch_project(accession)
                if persist_cache:
                    write_cache(accession, "project", project_data)
            except PrideAPIError:
                stale, age = read_cache_stale(accession, "project")
                if stale is not None:
                    project_data = stale  # type: ignore[assignment]
                    click.echo(
                        f"Warning: using stale cached project data for {accession} "
                        f"(cache age: {age:.0f}s). API unreachable.",
                        err=True,
                    )
                else:
                    raise

        if files_data is None:
            try:
                files_data = fetch_files(accession)
                if persist_cache:
                    write_cache(accession, "files", files_data)
            except PrideAPIError:
                stale, age = read_cache_stale(accession, "files")
                if stale is not None:
                    files_data = stale  # type: ignore[assignment]
                    click.echo(
                        f"Warning: using stale cached file list for {accession} "
                        f"(cache age: {age:.0f}s). API unreachable.",
                        err=True,
                    )
                else:
                    files_fetch_failed = True
                    files_data = []

    project_data = project_data or {}
    files_data = files_data or []

    result = compute_audit(
        accession, project_data, files_data, files_fetch_failed=files_fetch_failed
    )

    study = _extract_study(accession, project_data, fetched_at)
    files_df = _extract_files_df(accession, files_data)
    conn = get_or_create_db(db_path)
    try:
        insert_audit_record(conn, study, accession, files_df, asdict(result))
    finally:
        conn.close()

    return AuditData(result, study, files_df, files_data, fetched_at)


# ---------------------------------------------------------------------------
# Input helpers
# ---------------------------------------------------------------------------


def _read_accessions(input_path: str) -> list[str]:
    """Read newline-delimited accessions from a file or stdin (``-``).

    Strips whitespace, skips blank lines and ``#`` comment lines.
    Duplicates are preserved (caller should deduplicate).
    """
    lines: list[str]
    if input_path == "-":
        lines = sys.stdin.readlines()
    else:
        lines = Path(input_path).read_text().splitlines()

    accessions: list[str] = []
    for line in lines:
        acc = line.strip()
        if not acc or acc.startswith("#"):
            continue
        accessions.append(acc)
    return accessions


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------


def _default_export_path(fmt: str) -> str:
    """Generate a default export filename like ``pxaudit_bulk_20260525.tsv``."""
    date_str = datetime.now(UTC).strftime("%Y%m%d")
    return f"pxaudit_bulk_{date_str}.{fmt}"


_EXPORT_COLS = (
    "accession",
    "tier",
    "quant_tier",
    "has_title",
    "has_organism",
    "has_organism_id",
    "has_instrument",
    "has_result_files",
    "has_psi_results",
    "has_open_spectra",
    "has_organism_part",
    "has_publication",
    "has_tabular_quant",
    "has_quant_metadata",
    "has_sdrf",
    "has_mztab",
    "files_fetch_failed",
    "is_unverifiable",
    "tier_logic_version",
)


def _result_to_row(result: AuditResult) -> dict:
    """Convert an ``AuditResult`` to a flat dict for export."""
    d = asdict(result)
    return {c: d[c] for c in _EXPORT_COLS}


def _export_tsv(results: list[AuditResult], path: str) -> None:
    """Write results to a TSV file."""
    rows = [_result_to_row(r) for r in results]
    df = pd.DataFrame(rows, columns=_EXPORT_COLS)
    df.to_csv(path, sep="\t", index=False)


def _export_csv(results: list[AuditResult], path: str) -> None:
    """Write results to a CSV file."""
    rows = [_result_to_row(r) for r in results]
    df = pd.DataFrame(rows, columns=_EXPORT_COLS)
    df.to_csv(path, index=False)


def _export_json(results: list[AuditResult], path: str) -> None:
    """Write results to a JSON file."""
    rows = [_result_to_row(r) for r in results]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)


def _write_export(results: list[AuditResult], path: str, fmt: str) -> None:
    """Write *results* to *path* in the requested format (tsv/csv/json)."""
    if fmt == "tsv":
        _export_tsv(results, path)
    elif fmt == "csv":
        _export_csv(results, path)
    else:
        _export_json(results, path)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@main.command("check")
@click.argument("accession")
@click.option("--refresh", is_flag=True, default=False, help="Force re-fetch, updating cache.")
@click.option("--no-cache", "no_cache", is_flag=True, default=False, help="Skip cache reads.")
@click.option(
    "--db",
    "db_path",
    default="pxaudit_results.db",
    show_default=True,
    help="SQLite output path.",
)
def check(accession: str, refresh: bool, no_cache: bool, db_path: str) -> None:
    """Audit a single Proteomics Exchange accession."""
    if not accession or not accession[0].isalpha():
        click.echo(f"Error: invalid accession {accession!r}", err=True)
        sys.exit(2)

    try:
        data = _audit_single(accession, db_path, no_cache=no_cache, refresh=refresh)
        _print_result(data.result, data.study, len(data.files_data))
    except PrideAPIError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo("\nInterrupted.")
        sys.exit(130)


@main.command("bulk-audit")
@click.option(
    "--input",
    "input_path",
    required=True,
    help="Path to accession list (one per line), or '-' for stdin.",
)
@click.option(
    "--db",
    "db_path",
    default="pxaudit_results.db",
    show_default=True,
    help="SQLite output path.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["tsv", "json", "csv"], case_sensitive=False),
    default=None,
    help="Export format. Default: no export file written.",
)
@click.option(
    "--output",
    "export_path",
    default=None,
    help="Export file path. Default: pxaudit_bulk_<date>.<format>.",
)
@click.option(
    "--delay",
    default=1.0,
    type=float,
    show_default=True,
    help="Seconds to wait between API calls.",
)
@click.option(
    "--continue-on-error",
    "continue_on_error",
    is_flag=True,
    default=False,
    help="Skip failed accessions and continue.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing export file.",
)
def bulk_audit(
    input_path: str,
    db_path: str,
    fmt: str | None,
    export_path: str | None,
    delay: float,
    continue_on_error: bool,
    overwrite: bool,
) -> None:
    """Audit multiple Proteomics Exchange accessions."""
    # ------------------------------------------------------------------
    # 1.  Read input
    # ------------------------------------------------------------------
    try:
        raw_accessions = _read_accessions(input_path)
    except FileNotFoundError:
        click.echo(f"Error: input file not found: {input_path}", err=True)
        sys.exit(2)

    if not raw_accessions:
        click.echo("Warning: no accessions found in input.", err=True)
        sys.exit(0)

    # ------------------------------------------------------------------
    # 2.  Deduplicate
    # ------------------------------------------------------------------
    seen: set[str] = set()
    accessions: list[str] = []
    for acc in raw_accessions:
        if acc in seen:
            click.echo(f"Warning: duplicate accession {acc!r} skipped.", err=True)
        else:
            seen.add(acc)
            accessions.append(acc)

    total = len(accessions)

    # ------------------------------------------------------------------
    # 3.  Export setup
    # ------------------------------------------------------------------
    if fmt:
        fmt = fmt.lower()
        export_path = export_path or _default_export_path(fmt)
        if Path(export_path).exists() and not overwrite:
            click.echo(
                f"Error: output file {export_path!r} already exists. Use --overwrite to overwrite.",
                err=True,
            )
            sys.exit(2)
    else:
        export_path = None

    # ------------------------------------------------------------------
    # 4.  Batch audit
    # ------------------------------------------------------------------
    results: list[AuditResult] = []
    failed: list[str] = []
    start_time = time.time()

    try:
        for accession in tqdm(accessions, desc="Auditing", unit="accession"):
            try:
                data = _audit_single(accession, db_path)
                results.append(data.result)
            except PrideAPIError as exc:
                if continue_on_error:
                    click.echo(f"\nWarning: {accession} failed ({exc}). Skipping.", err=True)
                    failed.append(accession)
                else:
                    click.echo(f"\nError: {accession} failed ({exc}).", err=True)
                    click.echo("Use --continue-on-error to skip failures.", err=True)
                    # Write partial export before exiting.
                    if results and export_path:
                        _write_export(results, export_path, fmt or "tsv")
                        click.echo(
                            f"Partial export written to {export_path} "
                            f"({len(results)} accessions completed)."
                        )
                    elif results:
                        click.echo(f"Partial results: {len(results)} accessions completed.")
                    sys.exit(1)

            time.sleep(delay)

    except KeyboardInterrupt:
        click.echo("\nInterrupted. Partial results written to database.")
        # Write partial export on interrupt as well.
        if results and export_path:
            _write_export(results, export_path, fmt or "tsv")
            click.echo(f"Partial export written to {export_path}")

    # ------------------------------------------------------------------
    # 5.  Export
    # ------------------------------------------------------------------
    if results and export_path:
        _write_export(results, export_path, fmt or "tsv")
        click.echo(f"Exported {len(results)} results to {export_path}")

    # ------------------------------------------------------------------
    # 6.  Summary
    # ------------------------------------------------------------------
    elapsed = time.time() - start_time
    click.echo(f"\nBatch audit complete ({elapsed:.1f}s)")
    click.echo(f"  Total     : {total}")
    click.echo(f"  Completed : {len(results)}")
    click.echo(f"  Failed    : {len(failed)}")
    if results:
        tier_dist = pd.Series([r.tier for r in results]).value_counts()
        for tier, count in tier_dist.items():
            click.echo(f"    {tier:<12} {count}")


@main.command("manifest")
@click.argument("accession")
@click.option(
    "--db",
    "db_path",
    default="pxaudit_results.db",
    show_default=True,
    help="SQLite database path.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["tsv", "json"], case_sensitive=False),
    default="tsv",
    show_default=True,
    help="Output format.",
)
def manifest(accession: str, db_path: str, fmt: str) -> None:
    """List files for an accession from the audit database."""
    conn = get_or_create_db(db_path)
    try:
        cursor = conn.execute(
            "SELECT file_name, file_category, file_extension, ftp_location, "
            "file_size, checksum, checksum_type "
            "FROM study_files WHERE accession = ? ORDER BY file_name",
            (accession,),
        )
        rows = cursor.fetchall()
    finally:
        conn.close()

    if not rows:
        click.echo(
            f"No files found for {accession!r}. Run 'pxaudit check {accession}' first.",
            err=True,
        )
        sys.exit(1)

    columns = [
        "file_name",
        "file_category",
        "file_extension",
        "ftp_location",
        "file_size",
        "checksum",
        "checksum_type",
    ]
    df = pd.DataFrame(rows, columns=columns)

    if fmt == "json":
        click.echo(df.to_json(orient="records", indent=2))
    else:
        click.echo(df.to_csv(sep="\t", index=False))


@main.command("report")
@click.option("--db", "db_path", required=True, help="SQLite database path.")
@click.option(
    "--output",
    "output_dir",
    default=".",
    show_default=True,
    help="Output directory for the HTML report.",
)
@click.option(
    "--title",
    default="PXAudit Report",
    show_default=True,
    help="Report title shown in the page header.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing output directory.",
)
def report(db_path: str, output_dir: str, title: str, overwrite: bool) -> None:
    """Generate a self-contained HTML report from a populated database."""
    from pxaudit.report import generate_report

    if not Path(db_path).exists():
        click.echo(f"Error: database not found: {db_path}", err=True)
        sys.exit(2)

    out = Path(output_dir)
    if out.exists() and not overwrite:
        click.echo(
            f"Error: output directory {output_dir!r} already exists. Use --overwrite to overwrite.",
            err=True,
        )
        sys.exit(2)

    try:
        report_path = generate_report(db_path, output_dir, title)
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    except ImportError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    except FileNotFoundError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(2)
    except (PermissionError, sqlite3.DatabaseError) as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(2)

    click.echo(f"Report written to {report_path}")
