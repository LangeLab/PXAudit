"""Command-line interface for pxaudit.

Commands
--------
check        Audit a single Proteomics Exchange accession.
bulk-audit   Audit multiple Proteomics Exchange accessions in batch.
manifest     List files for an accession from the audit database.
report       Generate a self-contained HTML report from a populated database.
config show  Print effective configuration with source tags.
cache info   Summarize the local API cache.
cache clear  Delete cached API responses.
"""

from __future__ import annotations

import importlib.metadata
import json
import math
import os
import sqlite3
import stat
import sys
import tempfile
import time
import typing
import uuid
from contextlib import suppress
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import click
import pandas as pd
from tqdm import tqdm

from pxaudit import _PRIDE_PREFIX, _output
from pxaudit.accession import InvalidAccessionError, normalize_accession
from pxaudit.cache import (
    CachedResponse,
    CacheError,
    CacheSafetyError,
    CacheWriteError,
    clear_cache,
    inspect_cache,
    read_cache_response,
    read_cache_stale_response,
    validate_cache_root,
    write_cache,
)
from pxaudit.config import (
    EffectiveConfig,
    format_config_show,
    load_file_config,
    merge_config,
)
from pxaudit.db import (
    TransactionBatch,
    get_or_create_db,
    insert_audit_record,
    open_existing_db,
)
from pxaudit.pride_client import (
    PrideAPIError,
    _validate_files_payload,
    _validate_project_payload,
    fetch_files,
    fetch_project,
)
from pxaudit.tier_engine import AuditResult, FlagOutcome, compute_audit

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
        ISO 8601 retrieval time of the project response. For a compatible older cache entry,
        this is the cache file modification time used as migration provenance.
    warnings:
        Messages the CLI should emit via :func:`pxaudit._output.warn`.
    details:
        Verbose diagnostics the CLI should emit via :func:`pxaudit._output.detail`.
    network_used:
        True if any live API fetch was attempted for this accession
        (including attempts that failed and fell back to stale cache).
    """

    result: AuditResult
    study: dict
    files_df: pd.DataFrame
    files_data: list[dict]
    fetched_at: str | None
    warnings: list[str]
    details: list[str]
    network_used: bool


class _IncompleteAuditError(RuntimeError):
    """Raised when unavailable evidence prevents a truthful completed audit."""


def _emit_config_warnings(cfg: EffectiveConfig) -> None:
    for message in cfg.warnings:
        _output.warn(message)


def _stderr_is_tty() -> bool:
    """Return True when stderr is attached to a terminal."""
    return sys.stderr.isatty()


def _resolve_effective(
    ctx: click.Context,
    *,
    db_path: str | None = None,
    bulk_delay: float | None = None,
    export_format: str | None = None,
) -> EffectiveConfig:
    """Merge file config with group-level and command-level CLI overrides."""
    file_values = ctx.obj.get("file_values", {})
    file_warnings = ctx.obj.get("file_warnings", ())
    cache_dir = ctx.obj.get("cli_cache_dir")
    cfg = merge_config(
        file_values,
        file_warnings=file_warnings,
        cache_dir=cache_dir,
        db_path=db_path,
        bulk_delay=bulk_delay,
        export_format=export_format,
    )
    return cfg


@click.group(
    epilog=(
        "Global options (-q/--quiet, -v/--verbose, --no-color, --cache-dir) must "
        "appear before the subcommand, e.g. 'pxaudit -q check PXD000001'."
    ),
)
@click.version_option(importlib.metadata.version("pxaudit"))
@click.option(
    "-q",
    "--quiet",
    is_flag=True,
    default=False,
    help="Compact output: one status line where applicable.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Include detail lines (cache hits, fetch steps).",
)
@click.option(
    "--no-color",
    is_flag=True,
    default=False,
    help="Disable ANSI colors.",
)
@click.option(
    "--cache-dir",
    "cache_dir",
    default=None,
    type=click.Path(),
    help="Override API cache directory.",
)
@click.pass_context
def main(
    ctx: click.Context,
    quiet: bool,
    verbose: bool,
    no_color: bool,
    cache_dir: str | None,
) -> None:
    """Audit Proteomics Exchange study metadata."""
    if quiet and verbose:
        click.echo("Error: --quiet and --verbose are mutually exclusive.", err=True)
        sys.exit(2)

    _output.configure(quiet=quiet, verbose=verbose, no_color=no_color)
    file_values, file_warnings = load_file_config()
    ctx.ensure_object(dict)
    ctx.obj["quiet"] = quiet
    ctx.obj["verbose"] = verbose
    ctx.obj["no_color"] = no_color
    ctx.obj["cli_cache_dir"] = cache_dir
    ctx.obj["file_values"] = file_values
    ctx.obj["file_warnings"] = file_warnings


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


def _extract_study(accession: str, project: dict, fetched_at: str | None) -> dict:
    """Map project metadata and accession provenance to a ``study`` row."""
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
        "repository": _repository_for_accession(accession),
        "fetched_at": fetched_at,
    }


def _repository_for_accession(accession: str) -> str | None:
    """Return the repository implied by a recognized accession prefix."""
    upper_accession = accession.upper()
    repositories = {
        "PXD": "PRIDE",
        "MSV": "MassIVE",
        "JPST": "jPOST",
        "IPX": "iProX",
    }
    return next(
        (
            repository
            for prefix, repository in repositories.items()
            if upper_accession.startswith(prefix)
        ),
        None,
    )


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
    rows = []
    checksum_types = {32: "MD5", 40: "SHA-1", 64: "SHA-256"}
    for file_data in files:
        raw_current_checksum = file_data.get("checksum")
        raw_legacy_checksum = file_data.get("fileChecksum")
        current_checksum = (
            raw_current_checksum.strip() if isinstance(raw_current_checksum, str) else ""
        )
        legacy_checksum = (
            raw_legacy_checksum.strip() if isinstance(raw_legacy_checksum, str) else ""
        )
        checksum = current_checksum or legacy_checksum or None
        if current_checksum and checksum:
            is_hex = all(character in "0123456789abcdefABCDEF" for character in checksum)
            checksum_type = checksum_types.get(len(checksum)) if is_hex else None
        else:
            checksum_type = "MD5" if legacy_checksum and checksum else None
        rows.append(
            {
                "accession": accession,
                "file_name": file_data.get("fileName") or "",
                "file_category": (file_data.get("fileCategory") or {}).get("value") or None,
                "file_extension": Path(file_data.get("fileName") or "").suffix or None,
                "ftp_location": next(
                    (
                        loc.get("value")
                        for loc in (file_data.get("publicFileLocations") or [])
                        if loc.get("name") == "FTP Protocol"
                    ),
                    None,
                ),
                "file_size": file_data.get("fileSizeBytes"),
                "checksum": checksum,
                "checksum_type": checksum_type,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Output formatter
# ---------------------------------------------------------------------------


def _print_result(result: AuditResult, study: dict, file_count: int) -> None:
    """Print a formatted audit summary to stdout."""
    tick = "\u2714"
    cross = "\u2718"

    def flag(value: FlagOutcome | object) -> str:
        """Render passed, failed, and unknown evidence without color."""
        raw = getattr(value, "value", value)
        if raw in (FlagOutcome.PASSED.value, True, 1):
            return tick
        if raw in (FlagOutcome.FAILED.value, False, 0):
            return cross
        return "?"

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
    click.echo(f"  {flag(result.has_tabular_quant)} Tabular quant summary or matrix")
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
    cache_dir: str | Path | None = None,
    cache_ttl_seconds: float | None = None,
    request_delay: float = 0.5,
    db_connection: sqlite3.Connection | None = None,
    transaction_batch: TransactionBatch | None = None,
) -> AuditData:
    """Fetch, compute, persist, and return audit data for one accession.

    Default and refresh modes may serve stale cached data after a network failure. Disabled
    cache mode performs no cache reads or writes. ``study.fetched_at`` records the project
    response retrieval time, not the audit execution time.

    Does not write to the terminal. Warnings and verbose details are returned
    for the CLI layer to emit. When ``db_connection`` is provided, the caller owns
    the connection and it remains open after this accession completes. When
    ``transaction_batch`` is provided, the caller owns its commit boundary.
    """
    accession = normalize_accession(accession)
    snapshot_id = uuid.uuid4().hex
    project_response: CachedResponse | None = None
    files_response: CachedResponse | None = None
    warnings: list[str] = []
    details: list[str] = []
    network_used = False

    cache_kwargs: dict = {}
    if cache_dir is not None:
        cache_kwargs["cache_dir"] = Path(cache_dir)
    ttl_kwargs: dict = dict(cache_kwargs)
    if cache_ttl_seconds is not None:
        ttl_kwargs["max_age"] = cache_ttl_seconds

    cache_mode = "disabled" if no_cache else "refresh" if refresh else "default"
    read_fresh_cache = cache_mode == "default"
    read_stale_cache = cache_mode != "disabled"
    persist_cache = cache_mode != "disabled"

    def live_response(data: dict | list) -> CachedResponse:
        """Attach retrieval and audit-snapshot provenance to a live response."""
        return CachedResponse(
            data=data,
            retrieved_at=datetime.now(UTC).isoformat(),
            snapshot_id=snapshot_id,
            age=0.0,
        )

    def persist_cache_entry(endpoint: str, response: CachedResponse) -> None:
        """Write cache data without turning a cache failure into an audit failure."""
        try:
            write_cache(
                accession,
                endpoint,
                response.data,
                retrieved_at=response.retrieved_at,
                snapshot_id=response.snapshot_id,
                **cache_kwargs,
            )
        except (CacheSafetyError, CacheWriteError):
            warnings.append(
                f"Warning: cache write failed for {accession} {endpoint}; "
                "the audit continued without updating that entry."
            )

    if accession.upper().startswith(_PRIDE_PREFIX):
        if read_fresh_cache:
            project_response = read_cache_response(accession, "project", **ttl_kwargs)
            files_response = read_cache_response(accession, "files", **ttl_kwargs)
            if project_response is not None:
                details.append(f"cache hit: {accession} project")
            else:
                details.append(f"cache miss: {accession} project")
            if files_response is not None:
                details.append(f"cache hit: {accession} files")
            else:
                details.append(f"cache miss: {accession} files")

        if project_response is None:
            try:
                details.append(f"fetch: {accession} project")
                project_response = live_response(fetch_project(accession, delay=request_delay))
                network_used = True
                if persist_cache:
                    persist_cache_entry("project", project_response)
            except PrideAPIError:
                # Count the attempt so bulk-audit still applies bulk_delay.
                network_used = True
                stale = (
                    read_cache_stale_response(accession, "project", **cache_kwargs)
                    if read_stale_cache
                    else None
                )
                if stale is not None:
                    project_response = stale
                    warnings.append(
                        f"Warning: using stale cached project data for {accession} "
                        f"(cache age: {stale.age:.0f}s). API unreachable."
                    )
                    details.append(f"stale cache: {accession} project age={stale.age:.0f}s")
                else:
                    raise

        if files_response is None:
            try:
                details.append(f"fetch: {accession} files")
                files_response = live_response(fetch_files(accession, delay=request_delay))
                network_used = True
                if persist_cache:
                    persist_cache_entry("files", files_response)
            except PrideAPIError as exc:
                network_used = True
                stale = (
                    read_cache_stale_response(accession, "files", **cache_kwargs)
                    if read_stale_cache
                    else None
                )
                if stale is not None:
                    files_response = stale
                    warnings.append(
                        f"Warning: using stale cached file list for {accession} "
                        f"(cache age: {stale.age:.0f}s). API unreachable."
                    )
                    details.append(f"stale cache: {accession} files age={stale.age:.0f}s")
                else:
                    raise _IncompleteAuditError(
                        f"files response unavailable for {accession}; the audit is incomplete "
                        "and no database records were created or replaced"
                    ) from exc

    if project_response is not None and files_response is not None:
        _validate_project_payload(project_response.data)
        _validate_files_payload(files_response.data)
        snapshots_match = (
            project_response.snapshot_id is not None
            and bool(project_response.snapshot_id.strip())
            and project_response.snapshot_id == files_response.snapshot_id
        )
        if not snapshots_match:
            warnings.append(
                "Warning: project and files responses are from different or unverified "
                f"snapshots (project retrieved {project_response.retrieved_at}; "
                f"files retrieved {files_response.retrieved_at}). The audit may combine "
                "responses from different retrievals."
            )

    project_data = typing.cast(dict, project_response.data) if project_response is not None else {}
    files_data = typing.cast(list[dict], files_response.data) if files_response is not None else []
    fetched_at = project_response.retrieved_at if project_response is not None else None

    result = compute_audit(accession, project_data, files_data, files_fetch_failed=False)

    study = _extract_study(accession, project_data, fetched_at)
    files_df = _extract_files_df(accession, files_data)
    conn = db_connection if db_connection is not None else get_or_create_db(db_path)
    if transaction_batch is not None:
        transaction_batch.begin()
        insert_audit_record(
            conn,
            study,
            accession,
            files_df,
            asdict(result),
            manage_transaction=False,
        )
    else:
        try:
            insert_audit_record(conn, study, accession, files_df, asdict(result))
        finally:
            if db_connection is None:
                conn.close()

    return AuditData(
        result,
        study,
        files_df,
        files_data,
        fetched_at,
        warnings,
        details,
        network_used,
    )


# ---------------------------------------------------------------------------
# Input helpers
# ---------------------------------------------------------------------------


def _read_accessions(input_path: str) -> list[tuple[int, str]]:
    """Read numbered accession records from a file or stdin (``-``).

    Blank lines and lines whose trimmed form begins with ``#`` are skipped. Validation and
    deduplication remain with the bulk command so errors can report source line numbers.
    """
    lines = (
        sys.stdin.readlines()
        if input_path == "-"
        else Path(input_path).read_text(encoding="utf-8").split("\n")
    )

    accessions: list[tuple[int, str]] = []
    for line_number, line in enumerate(lines, start=1):
        acc = line.strip(" \t\r\n")
        if not acc or acc.startswith("#"):
            continue
        accessions.append((line_number, acc))
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
    "ambiguity_count",
    "tier_logic_version",
)


def _result_to_row(result: AuditResult) -> dict:
    """Convert an ``AuditResult`` to a flat dict for export."""
    d = asdict(result)
    for column in _EXPORT_COLS:
        if column.startswith("has_"):
            value = d[column]
            if isinstance(value, FlagOutcome):
                d[column] = value.value
            elif isinstance(value, bool) or isinstance(value, int) and value in (0, 1):
                d[column] = "passed" if value else "failed"
            else:
                d[column] = "unknown"
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
    """Atomically write *results* to *path* in the requested format."""
    target = Path(path)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=target.parent,
        prefix=f".{target.name}.",
        suffix=".tmp",
        delete=False,
    ) as temporary:
        temporary_path = Path(temporary.name)

    try:
        if fmt == "tsv":
            _export_tsv(results, str(temporary_path))
        elif fmt == "csv":
            _export_csv(results, str(temporary_path))
        else:
            _export_json(results, str(temporary_path))
        os.replace(temporary_path, target)
    except BaseException:
        with suppress(OSError):
            temporary_path.unlink(missing_ok=True)
        raise


def _cache_stats(cache_dir: Path) -> tuple[int, int, int, float | None, float | None]:
    """Return owned count, ignored count, bytes, oldest mtime, and newest mtime."""
    inventory = inspect_cache(cache_dir)
    if not inventory.entries:
        return 0, inventory.ignored, 0, None, None
    total = sum(entry.size for entry in inventory.entries)
    mtimes = [entry.modified_at for entry in inventory.entries]
    return len(inventory.entries), inventory.ignored, total, min(mtimes), max(mtimes)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@main.command("check")
@click.argument("accession")
@click.option(
    "--refresh",
    is_flag=True,
    default=False,
    help=(
        "Skip fresh cache reads; fetch live responses, write successes, and allow stale "
        "fallback after failure."
    ),
)
@click.option(
    "--no-cache",
    "no_cache",
    is_flag=True,
    default=False,
    help="Skip cache reads and writes.",
)
@click.option(
    "--db",
    "db_path",
    default=None,
    help="SQLite output path (default: config or pxaudit_results.db).",
)
@click.pass_context
def check(
    ctx: click.Context,
    accession: str,
    refresh: bool,
    no_cache: bool,
    db_path: str | None,
) -> None:
    """Audit a single Proteomics Exchange accession."""
    cfg = _resolve_effective(ctx, db_path=db_path)
    _emit_config_warnings(cfg)
    resolved_db = cfg.db_path

    try:
        accession = normalize_accession(accession)
    except InvalidAccessionError as exc:
        _output.error(f"Error: invalid accession {accession!r}: {exc}")
        sys.exit(2)

    try:
        data = _audit_single(
            accession,
            resolved_db,
            no_cache=no_cache,
            refresh=refresh,
            cache_dir=cfg.cache_dir,
            cache_ttl_seconds=cfg.cache_ttl_seconds,
            request_delay=cfg.request_delay,
        )
        for message in data.warnings:
            _output.warn(message)
        for message in data.details:
            _output.detail(message)

        if ctx.obj["quiet"]:
            _output.status(
                f"{data.result.accession}  {data.result.tier}  "
                f"{data.result.quant_tier}  db={resolved_db}"
            )
        else:
            _print_result(data.result, data.study, len(data.files_data))
    except _IncompleteAuditError as exc:
        _output.warn(f"Warning: {exc}.")
        sys.exit(1)
    except PrideAPIError as exc:
        _output.error(f"Error: {exc}")
        sys.exit(1)
    except (CacheError, sqlite3.DatabaseError, OSError) as exc:
        _output.error(f"Error: audit failed: {exc}")
        sys.exit(1)
    except KeyboardInterrupt:
        _output.error("\nInterrupted.")
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
    default=None,
    help="SQLite output path (default: config or pxaudit_results.db).",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["tsv", "json", "csv"], case_sensitive=False),
    default=None,
    help="Export format. Default: config export_format or no export file.",
)
@click.option(
    "--output",
    "export_path",
    default=None,
    help="Export file path. Default: pxaudit_bulk_<date>.<format>.",
)
@click.option(
    "--delay",
    default=None,
    type=float,
    help="Seconds to wait between accessions after a network fetch (bulk_delay).",
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
@click.option(
    "--batch-size",
    type=click.IntRange(min=1),
    default=1,
    show_default=True,
    help="Commit database progress after this many completed accessions.",
)
@click.pass_context
def bulk_audit(
    ctx: click.Context,
    input_path: str,
    db_path: str | None,
    fmt: str | None,
    export_path: str | None,
    delay: float | None,
    continue_on_error: bool,
    overwrite: bool,
    batch_size: int,
) -> None:
    """Audit multiple Proteomics Exchange accessions."""
    cfg = _resolve_effective(ctx, db_path=db_path, bulk_delay=delay, export_format=fmt)
    _emit_config_warnings(cfg)
    if delay is not None and (not math.isfinite(delay) or delay < 0):
        _output.error("Error: --delay must be finite and non-negative.")
        sys.exit(2)
    resolved_db = cfg.db_path
    bulk_delay = cfg.bulk_delay
    quiet = ctx.obj["quiet"]

    try:
        raw_accessions = _read_accessions(input_path)
    except FileNotFoundError:
        _output.error(f"Error: input file not found: {input_path}")
        sys.exit(2)
    except (OSError, UnicodeError) as exc:
        _output.error(f"Error: cannot read input file {input_path!r}: {exc}")
        sys.exit(2)

    if not raw_accessions:
        _output.warn("Warning: no accessions found in input.")
        sys.exit(0)

    failed: list[str] = []
    seen: set[str] = set()
    accessions: list[str] = []
    for line_number, raw_accession in raw_accessions:
        try:
            accession = normalize_accession(raw_accession)
        except InvalidAccessionError as exc:
            message = f"line {line_number}: invalid accession {raw_accession!r}: {exc}"
            if continue_on_error:
                _output.warn(f"Warning: {message}. Skipping.")
                failed.append(f"line {line_number}")
                continue
            _output.error(f"Error: {message}.")
            _output.error("Use --continue-on-error to skip malformed input records.")
            sys.exit(2)

        if accession in seen:
            _output.warn(f"Warning: duplicate accession {accession!r} skipped.")
        else:
            seen.add(accession)
            accessions.append(accession)

    total = len(accessions) + len(failed)

    resolved_fmt = fmt.casefold() if fmt else cfg.export_format
    if resolved_fmt:
        resolved_fmt = resolved_fmt.casefold()
        export_path = export_path or _default_export_path(resolved_fmt)
        export_target = Path(export_path)
        if export_target.is_symlink():
            _output.error(f"Error: output path {export_path!r} is a symbolic link.")
            sys.exit(2)
        if export_target.exists() and not export_target.is_file():
            _output.error(f"Error: output path {export_path!r} is not a file.")
            sys.exit(2)
        if export_target.exists() and not overwrite:
            _output.error(
                f"Error: output file {export_path!r} already exists. Use --overwrite to overwrite."
            )
            sys.exit(2)
    else:
        export_path = None

    results: list[AuditResult] = []
    start_time = time.time()
    use_tqdm = (not quiet) and _stderr_is_tty()
    iterator = tqdm(accessions, desc="Auditing", unit="accession") if use_tqdm else accessions

    try:
        database_connection = get_or_create_db(resolved_db)
    except (CacheError, sqlite3.DatabaseError, OSError) as exc:
        _output.error(f"Error: bulk audit failed: {exc}")
        sys.exit(1)

    transaction_batch = (
        TransactionBatch(database_connection, batch_size) if batch_size > 1 else None
    )
    rolled_back = 0

    def rollback_active_batch() -> int:
        """Roll back pending rows and remove their results from partial exports."""
        if transaction_batch is None:
            return 0
        count = transaction_batch.rollback()
        if count:
            del results[-count:]
        return count

    try:
        for accession in iterator:
            data: AuditData | None = None
            try:
                data = _audit_single(
                    accession,
                    resolved_db,
                    cache_dir=cfg.cache_dir,
                    cache_ttl_seconds=cfg.cache_ttl_seconds,
                    request_delay=cfg.request_delay,
                    db_connection=database_connection,
                    transaction_batch=transaction_batch,
                )
                if transaction_batch is not None:
                    transaction_batch.record()
                results.append(data.result)
                for message in data.warnings:
                    _output.warn(message)
                for message in data.details:
                    _output.detail(message)
                if data.network_used:
                    time.sleep(bulk_delay)
            except (_IncompleteAuditError, PrideAPIError) as exc:
                if continue_on_error:
                    _output.warn(f"\nWarning: {accession} failed ({exc}). Skipping.")
                    failed.append(accession)
                    if transaction_batch is not None:
                        transaction_batch.commit()
                    if ctx.obj["verbose"]:
                        _output.detail(f"skipped: {accession}")
                    # API was attempted; apply bulk_delay before the next accession.
                    time.sleep(bulk_delay)
                else:
                    rolled_back = rollback_active_batch()
                    _output.error(f"\nError: {accession} failed ({exc}).")
                    _output.error("Use --continue-on-error to skip failures.")
                    if results and export_path:
                        try:
                            _write_export(results, export_path, resolved_fmt or "tsv")
                        except (OSError, TypeError, ValueError) as export_exc:
                            _output.error(
                                f"Error: partial export {export_path!r} could not be written: "
                                f"{export_exc}"
                            )
                            sys.exit(1)
                        _output.status(
                            f"Partial export written to {export_path} "
                            f"({len(results)} accessions completed)."
                        )
                    elif results:
                        _output.status(f"Partial results: {len(results)} accessions completed.")
                    if transaction_batch is not None:
                        _output.status(
                            f"Database progress: committed={transaction_batch.committed_count} "
                            f"rolled_back={rolled_back}."
                        )
                    sys.exit(1)
            finally:
                data = None

        if transaction_batch is not None:
            transaction_batch.commit()

    except KeyboardInterrupt:
        rolled_back = rollback_active_batch()
        _output.warn("\nInterrupted. Partial results written to database.")
        if results and export_path:
            try:
                _write_export(results, export_path, resolved_fmt or "tsv")
            except (OSError, TypeError, ValueError) as exc:
                _output.warn(f"Warning: partial export could not be written: {exc}")
            else:
                _output.status(f"Partial export written to {export_path}")
        if transaction_batch is not None:
            _output.status(
                f"Database progress: committed={transaction_batch.committed_count} "
                f"rolled_back={rolled_back}."
            )
        sys.exit(130)
    except (CacheError, sqlite3.DatabaseError, OSError) as exc:
        rolled_back = rollback_active_batch()
        if transaction_batch is not None:
            _output.error(
                f"Error: bulk audit failed: {exc}. "
                f"committed={transaction_batch.committed_count} rolled_back={rolled_back}."
            )
        else:
            _output.error(f"Error: bulk audit failed: {exc}")
        sys.exit(1)
    finally:
        database_connection.close()

    if results and export_path:
        try:
            _write_export(results, export_path, resolved_fmt or "tsv")
        except (OSError, TypeError, ValueError) as exc:
            _output.error(f"Error: cannot write export {export_path!r}: {exc}")
            sys.exit(1)
        if not quiet:
            _output.status(f"Exported {len(results)} results to {export_path}")

    elapsed = time.time() - start_time
    if quiet:
        export_bit = f"  export={export_path}" if export_path else ""
        _output.status(
            f"bulk-audit  total={total}  completed={len(results)}  failed={len(failed)}{export_bit}"
        )
    else:
        _output.status(f"\nBatch audit complete ({elapsed:.1f}s)")
        _output.status(f"  Total     : {total}")
        _output.status(f"  Completed : {len(results)}")
        _output.status(f"  Failed    : {len(failed)}")
        if results:
            tier_dist = pd.Series([r.tier for r in results]).value_counts()
            for tier, count in tier_dist.items():
                _output.status(f"    {tier:<12} {count}")
        for acc in failed:
            if ctx.obj["verbose"]:
                _output.detail(f"failed: {acc}")


@main.command("manifest")
@click.argument("accession")
@click.option(
    "--db",
    "db_path",
    default=None,
    help="SQLite database path (default: config or pxaudit_results.db).",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["tsv", "json"], case_sensitive=False),
    default="tsv",
    show_default=True,
    help="Output format.",
)
@click.pass_context
def manifest(ctx: click.Context, accession: str, db_path: str | None, fmt: str) -> None:
    """List files for an accession from the audit database."""
    cfg = _resolve_effective(ctx, db_path=db_path)
    _emit_config_warnings(cfg)
    resolved_db = cfg.db_path

    try:
        accession = normalize_accession(accession)
    except InvalidAccessionError as exc:
        _output.error(f"Error: invalid accession {accession!r}: {exc}")
        sys.exit(2)

    try:
        conn = open_existing_db(resolved_db)
    except FileNotFoundError as exc:
        _output.error(f"Error: {exc}")
        sys.exit(2)
    except sqlite3.DatabaseError as exc:
        _output.error(f"Error: cannot read database {resolved_db}: {exc}")
        sys.exit(1)

    try:
        cursor = conn.execute(
            "SELECT file_name, file_category, file_extension, ftp_location, "
            "file_size, checksum, checksum_type "
            "FROM study_files WHERE accession = ? ORDER BY file_name",
            (accession,),
        )
        rows = cursor.fetchall()
        study_exists = (
            conn.execute("SELECT 1 FROM study WHERE accession = ?", (accession,)).fetchone()
            is not None
        )
    except sqlite3.DatabaseError as exc:
        _output.error(f"Error: cannot read database {resolved_db}: {exc}")
        sys.exit(1)
    finally:
        conn.close()

    if not rows and not study_exists:
        _output.error(f"No files found for {accession!r}. Run 'pxaudit check {accession}' first.")
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

    try:
        body = (
            df.to_json(orient="records", indent=2)
            if fmt == "json"
            else df.to_csv(sep="\t", index=False, lineterminator="\n")
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        _output.error(f"Error: cannot format manifest for {accession!r}: {exc}")
        sys.exit(1)
    click.echo(body, nl=not body.endswith("\n"))


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
    help="Overwrite report.html if it already exists.",
)
@click.pass_context
def report(
    ctx: click.Context,
    db_path: str,
    output_dir: str,
    title: str,
    overwrite: bool,
) -> None:
    """Generate a self-contained HTML report from a populated database."""
    from pxaudit.report import generate_report

    _emit_config_warnings(_resolve_effective(ctx))

    database_path = Path(db_path)
    try:
        database_mode = database_path.stat().st_mode
    except FileNotFoundError:
        _output.error(f"Error: database not found: {db_path}")
        sys.exit(2)
    except OSError as exc:
        _output.error(f"Error: cannot access database {db_path}: {exc}")
        sys.exit(1)
    if not stat.S_ISREG(database_mode):
        _output.error(f"Error: database path is not a file: {db_path}")
        sys.exit(2)

    out = Path(output_dir)
    if out.exists() and not out.is_dir():
        _output.error(f"Error: output path {output_dir!r} is not a directory.")
        sys.exit(2)
    report_target = out / "report.html"
    if report_target.is_symlink():
        _output.error(f"Error: report target {str(report_target)!r} is a symbolic link.")
        sys.exit(2)
    if report_target.exists() and not report_target.is_file():
        _output.error(f"Error: report target {str(report_target)!r} is not a file.")
        sys.exit(2)
    if report_target.exists() and not overwrite:
        _output.error(
            f"Error: output file {str(report_target)!r} already exists. "
            "Use --overwrite to overwrite it."
        )
        sys.exit(2)

    try:
        report_path = generate_report(db_path, output_dir, title)
        if ctx.obj["verbose"]:
            conn = open_existing_db(db_path)
            try:
                n_audit = conn.execute("SELECT COUNT(*) FROM audit").fetchone()[0]
                n_files = conn.execute("SELECT COUNT(*) FROM study_files").fetchone()[0]
            finally:
                conn.close()
    except (ImportError, ValueError, OSError, sqlite3.DatabaseError) as exc:
        _output.error(f"Error: {exc}")
        sys.exit(1)

    _output.status(f"Report written to {report_path}")
    if ctx.obj["verbose"]:
        _output.detail(f"report rows={n_audit} files={n_files} db={db_path} output={report_path}")


@main.group("config")
def config_group() -> None:
    """Inspect effective configuration."""


@config_group.command("show")
@click.pass_context
def config_show(ctx: click.Context) -> None:
    """Print effective settings with source tags (default / config / flag)."""
    cfg = _resolve_effective(ctx)
    _emit_config_warnings(cfg)
    click.echo(format_config_show(cfg))


@main.group("cache")
def cache_group() -> None:
    """Inspect or clear the local API cache."""


@cache_group.command("info")
@click.pass_context
def cache_info(ctx: click.Context) -> None:
    """Print validated cache-entry counts, size, and modification times."""
    cfg = _resolve_effective(ctx)
    _emit_config_warnings(cfg)
    try:
        cache_dir = validate_cache_root(cfg.cache_dir)
        count, ignored, total, oldest, newest = _cache_stats(cache_dir)
    except CacheSafetyError as exc:
        _output.error(f"Error: unsafe cache directory: {exc}")
        sys.exit(2)
    _output.status(f"cache_dir={cache_dir}")
    _output.status(f"files={count}")
    _output.status(f"ignored={ignored}")
    _output.status(f"bytes={total}")
    if oldest is None or newest is None:
        _output.status("oldest=n/a")
        _output.status("newest=n/a")
    else:
        _output.status(f"oldest={datetime.fromtimestamp(oldest, tz=UTC).isoformat()}")
        _output.status(f"newest={datetime.fromtimestamp(newest, tz=UTC).isoformat()}")


@cache_group.command("clear")
@click.option(
    "--yes",
    is_flag=True,
    default=False,
    help="Skip confirmation; cache safety validation still applies.",
)
@click.pass_context
def cache_clear(ctx: click.Context, yes: bool) -> None:
    """Delete validated PXAudit-owned entries from the configured cache."""
    cfg = _resolve_effective(ctx)
    _emit_config_warnings(cfg)
    try:
        cache_dir = validate_cache_root(cfg.cache_dir)
        inventory = inspect_cache(cache_dir)
    except CacheSafetyError as exc:
        _output.error(f"Error: unsafe cache directory: {exc}")
        sys.exit(2)
    _output.status(f"cache_dir={cache_dir}")

    if not cache_dir.exists():
        _output.status("Cache directory does not exist; nothing to delete.")
        return

    if inventory.entries and not yes:
        click.confirm(
            f"Delete {len(inventory.entries)} validated cache file(s) under {cache_dir}?",
            abort=True,
        )

    try:
        removed, ignored, failed = clear_cache(cache_dir)
    except CacheSafetyError as exc:
        _output.error(f"Error: cache directory became unsafe: {exc}")
        sys.exit(1)
    _output.status(f"Removed {removed} file(s).")
    _output.status(f"Ignored entries: {ignored}.")
    if failed:
        _output.error(f"Error: failed to remove {failed} validated cache file(s).")
        sys.exit(1)
