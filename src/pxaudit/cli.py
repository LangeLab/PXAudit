"""Command-line interface for pxaudit."""

from __future__ import annotations

import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import click
import pandas as pd

from pxaudit.cache import read_cache, write_cache
from pxaudit.db import get_or_create_db, insert_audit, insert_study, insert_study_files
from pxaudit.pride_client import PrideAPIError, fetch_files, fetch_project
from pxaudit.tier_engine import AuditResult, compute_audit

_PRIDE_PREFIX = "PXD"


@click.group()
def main() -> None:
    """Audit Proteomics Exchange study metadata."""


# ---------------------------------------------------------------------------
# Data extraction helpers
# ---------------------------------------------------------------------------


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
        "submission_year": int(date_str[:4]) if date_str else None,
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
    click.echo(f"  {flag(result.has_tabular_quant)} Tabular quant table (proteinGroups / mzTab-Q)")
    if result.files_fetch_failed:
        click.echo("  ! Files endpoint failed — file flags are unreliable")
    click.echo("-" * 48)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@main.command("check")
@click.argument("accession")
@click.option("--no-cache", "no_cache", is_flag=True, default=False, help="Skip cache reads.")
@click.option(
    "--db",
    "db_path",
    default="pxaudit_results.db",
    show_default=True,
    help="SQLite output path.",
)
def check(accession: str, no_cache: bool, db_path: str) -> None:
    """Audit a single Proteomics Exchange accession."""
    # ------------------------------------------------------------------
    # 1.  Validate accession
    # ------------------------------------------------------------------
    if not accession or not accession[0].isalpha():
        click.echo(f"Error: invalid accession {accession!r}", err=True)
        sys.exit(2)

    fetched_at = datetime.now(UTC).isoformat()
    project_data: dict | None = None
    files_data: list[dict] | None = None
    files_fetch_failed = False

    # ------------------------------------------------------------------
    # 2.  Fetch data (PRIDE only; non-PXD are Unverifiable by prefix)
    # ------------------------------------------------------------------
    if accession.upper().startswith(_PRIDE_PREFIX):
        if not no_cache:
            project_data = read_cache(accession, "project")
            files_data = read_cache(accession, "files")

        if project_data is None:
            try:
                project_data = fetch_project(accession)
                write_cache(accession, "project", project_data)
            except PrideAPIError as exc:
                click.echo(f"Error: {exc}", err=True)
                sys.exit(1)

        if files_data is None:
            try:
                files_data = fetch_files(accession)
                write_cache(accession, "files", files_data)
            except PrideAPIError:
                files_fetch_failed = True
                files_data = []

    project_data = project_data or {}
    files_data = files_data or []

    # ------------------------------------------------------------------
    # 3.  Compute audit
    # ------------------------------------------------------------------
    result = compute_audit(
        accession, project_data, files_data, files_fetch_failed=files_fetch_failed
    )

    # ------------------------------------------------------------------
    # 4.  Persist to SQLite
    # ------------------------------------------------------------------
    study = _extract_study(accession, project_data, fetched_at)
    files_df = _extract_files_df(accession, files_data)
    conn = get_or_create_db(db_path)
    try:
        insert_study(conn, study)
        insert_study_files(conn, accession, files_df)
        insert_audit(conn, asdict(result))
    finally:
        conn.close()

    # ------------------------------------------------------------------
    # 5.  Print
    # ------------------------------------------------------------------
    _print_result(result, study, len(files_data))
