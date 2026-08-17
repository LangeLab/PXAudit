"""HTML report generation from a populated pxaudit SQLite database.

The ``pxaudit report`` command generates a self-contained HTML report showing:

- Summary metrics: total audited accessions, tier distribution
- Tier and quant tier distribution charts (best-to-worst order)
- Full accession table sorted by quality (best tier first) with every
  metadata flag shown as a colored badge (present / missing / unknown)

Charts are produced with ``matplotlib`` and embedded as base64-encoded PNG
images. The page template is rendered with ``jinja2``. Both libraries are
optional dependencies installed from a source checkout via ``uv sync --extra report``.
"""

from __future__ import annotations

import base64
import importlib
import logging
import os
import sqlite3
import tempfile
import typing
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import metadata as importlib_metadata
from io import BytesIO
from pathlib import Path

import pandas as pd

_log = logging.getLogger(__name__)

__all__ = [
    "generate_report",
    "JINJA2_MISSING_MSG",
    "MATPLOTLIB_MISSING_MSG",
]

JINJA2_MISSING_MSG = (
    "jinja2 is required for report generation. Install with: uv sync --extra report"
)
MATPLOTLIB_MISSING_MSG = "matplotlib is required for charts. Install with: uv sync --extra report"

# ---------------------------------------------------------------------------
# Tier metadata (colours and display order)
# ---------------------------------------------------------------------------

_TIER_ORDER: list[str] = [
    "Diamond",
    "Platinum",
    "Gold",
    "Silver",
    "Bronze",
    "Raw",
    "None",
    "Unverifiable",
    "Unknown",
]

_TIER_COLORS: dict[str, str] = {
    "Diamond": "#2563eb",  # Brilliant blue - precious stone
    "Platinum": "#94a3b8",  # Silver/gray - precious metal
    "Gold": "#f59e0b",  # Gold - precious metal
    "Silver": "#cbd5e1",  # Light gray - precious metal
    "Bronze": "#b45309",  # Brown/bronze - precious metal
    "Raw": "#64748b",  # Dark gray - unprocessed
    "None": "#e2e8f0",  # Very light gray - missing
    "Unverifiable": "#ef4444",  # Red - error/unknown
    "Unknown": "#7c3aed",
}

_QUANT_TIER_ORDER: list[str] = [
    "Quant-Complete",
    "Quant-Ready",
    "Partial",
    "No Quant",
    "Unverifiable",
    "Unknown",
]

_QUANT_TIER_COLORS: dict[str, str] = {
    "Quant-Complete": "#16a34a",  # Green - full quantification
    "Quant-Ready": "#3b82f6",  # Blue - ready for quantification
    "Partial": "#f59e0b",  # Amber - partial quantification
    "No Quant": "#94a3b8",  # Gray - no quantification
    "Unverifiable": "#ef4444",  # Red - error/unknown
    "Unknown": "#7c3aed",
}

# All metadata flag columns in the audit table, in display order.
_FLAG_COLUMNS: list[tuple[str, str]] = [
    ("title", "has_title"),
    ("organism", "has_organism"),
    ("organism_id", "has_organism_id"),
    ("instrument", "has_instrument"),
    ("result_files", "has_result_files"),
    ("psi_results", "has_psi_results"),
    ("sdrf", "has_sdrf"),
    ("open_spectra", "has_open_spectra"),
    ("organism_part", "has_organism_part"),
    ("publication", "has_publication"),
    ("tabular_quant", "has_tabular_quant"),
    ("quant_metadata", "has_quant_metadata"),
    ("mztab", "has_mztab"),
]

# ---------------------------------------------------------------------------
# Jinja2 HTML template
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ title }}</title>
<style>
  *, *:before, *:after { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
      Oxygen-Sans, Ubuntu, Cantarell, "Helvetica Neue", sans-serif;
    max-width: 1200px; margin: 0 auto; padding: 1em 1.5em; color: #1a1a1a;
    background: #fafafa; line-height: 1.5;
  }
  h1 { font-size: 1.3em; border-bottom: 3px solid #2563eb; padding-bottom: 0.3em;
       margin: 0 0 0.5em 0; font-weight: 700; }
  h2 { font-size: 1.05em; color: #1f2937; margin: 1.2em 0 0.4em 0;
       padding-bottom: 0.2em; border-bottom: 1px solid #d1d5db; font-weight: 600; }

  .meta-card {
    background: #fff; border: 1px solid #e5e7eb; border-radius: 8px;
    padding: 0.7em 1em; margin-bottom: 0.8em; display: grid;
    grid-template-columns: auto 1fr; gap: 0.1em 1em; font-size: 0.82em;
  }
  .meta-card dt { color: #6b7280; font-weight: 500; }
  .meta-card dd { margin: 0; color: #1a1a1a; }

  .alert-warn {
    background: #fffbeb; border: 1px solid #f59e0b; border-radius: 6px;
    padding: 0.45em 0.7em; margin: 0.5em 0; font-size: 0.82em; color: #92400e;
  }

  .summary-grid { display: flex; gap: 0.6em; margin: 0.6em 0; flex-wrap: wrap; }
  .summary-card {
    flex: 1; min-width: 100px; background: #fff; border: 1px solid #e5e7eb;
    border-radius: 8px; padding: 0.5em 0.8em; text-align: center;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
  }
  .summary-card .num { font-size: 1.5em; font-weight: 700; color: #2563eb; }
  .summary-card .label { font-size: 0.72em; color: #6b7280; margin-top: 0.1em;
                         line-height: 1.3; }
  .summary-card .sub { font-size: 0.65em; color: #9ca3af; }

  .charts { display: flex; gap: 0.8em; flex-wrap: wrap; }
  .chart {
    flex: 1; min-width: 280px; background: #fff; padding: 0.5em;
    border-radius: 8px; border: 1px solid #e5e7eb;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06); text-align: center;
  }
  .chart img { max-width: 100%; height: auto; display: block; margin: 0 auto; }
  .chart-title { font-size: 0.78em; color: #374151; font-weight: 600;
                 margin-bottom: 0.2em; }

  .table-wrap {
    overflow-x: auto; background: #fff; border-radius: 8px;
    border: 1px solid #e5e7eb; box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    max-height: 80vh; overflow-y: auto;
  }
  table { border-collapse: collapse; width: 100%; font-size: 0.75em; }
  th {
    background: #f3f4f6; color: #374151; font-weight: 600;
    border-bottom: 2px solid #d1d5db; padding: 0.4em 0.4em;
    text-align: left; white-space: nowrap;
    position: sticky; top: 0; z-index: 1;
  }
  td { border-bottom: 1px solid #e5e7eb; padding: 0.3em 0.4em; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #f9fafb; }
  .col-acc { min-width: 80px; }
  .col-title { min-width: 140px; max-width: 200px; overflow: hidden;
               text-overflow: ellipsis; white-space: nowrap; }
  .col-tier { min-width: 55px; }
  .col-flag { min-width: 30px; text-align: center; }
  .acc { font-family: monospace; font-weight: 600; color: #2563eb;
         font-size: 0.9em; }

  .badge { display: inline-block; border-radius: 3px; padding: 0.05em 0.3em;
           font-size: 0.8em; font-weight: 600; min-width: 1.4em;
           text-align: center; }
  .badge-ok { color: #16a34a; background: #f0fdf4; }
  .badge-missing { color: #dc2626; background: #fef2f2; }
  .badge-unknown { color: #d97706; background: #fffbeb; }

  .tier-Diamond { color: #2563eb; font-weight: 600; }
  .tier-Platinum { color: #94a3b8; font-weight: 600; }
  .tier-Gold { color: #f59e0b; font-weight: 600; }
  .tier-Silver { color: #cbd5e1; font-weight: 600; }
  .tier-Bronze { color: #b45309; font-weight: 600; }
  .tier-Raw { color: #64748b; font-weight: 600; }
  .tier-None { color: #e2e8f0; }
  .tier-Unverifiable { color: #ef4444; }
  .tier-Unknown { color: #7c3aed; }

  .tier-Quant-Complete { color: #16a34a; font-weight: 600; }
  .tier-Quant-Ready { color: #3b82f6; font-weight: 600; }
  .tier-Partial { color: #f59e0b; font-weight: 600; }
  .tier-No-Quant { color: #94a3b8; }

  .footnote { font-size: 0.75em; color: #9ca3af; margin-top: 0.3em; }
  .flag-legend { font-size: 0.75em; color: #6b7280; margin: 0 0 0.3em 0; }
  .flag-legend .badge { font-size: 0.75em; }

  .tier-range-row { display: flex; gap: 0.6em; margin: 0.6em 0; flex-wrap: wrap; }
  .tier-range-card {
    flex: 1; min-width: 200px; background: #fff; border: 1px solid #e5e7eb;
    border-radius: 8px; padding: 0.5em 0.8em; text-align: center;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
  }
  .tier-range-label { font-size: 0.72em; color: #6b7280; margin-bottom: 0.2em; }
  .tier-range-values { font-size: 1.1em; font-weight: 600; }
  .tier-range-sep { color: #d1d5db; margin: 0 0.3em; font-weight: 400; }

  details { margin: 0.5em 0; }
  details summary {
    cursor: pointer; font-size: 0.82em; color: #6b7280; padding: 0.3em 0;
    user-select: none; list-style: none;
  }
  details summary::-webkit-details-marker { display: none; }
  details summary::before { content: "\\25B6 "; font-size: 0.7em; }
  details[open] summary::before { content: "\\25BC "; }

  .tier-legend { font-size: 0.78em; color: #374151; margin: 0.4em 0 0.8em 0; }
  .tier-legend-group { margin-bottom: 0.4em; }
  .tier-legend-title { font-weight: 600; color: #1f2937; margin-bottom: 0.15em; }
  .tier-legend-items { line-height: 1.6; }
  .tier-sep { color: #d1d5db; margin: 0 0.15em; }

  .gap-table { font-size: 0.78em; margin-top: 0.4em; }
  .gap-table th { font-size: 0.9em; }
  .gap-severity-critical { color: #dc2626; font-weight: 600; }
  .gap-severity-moderate { color: #d97706; font-weight: 600; }
  .gap-severity-acceptable { color: #16a34a; font-weight: 600; }

  .exemplar-card {
    background: #fff; border: 1px solid #e5e7eb; border-radius: 8px;
    padding: 0.5em 0.8em; margin: 0.4em 0; font-size: 0.78em;
  }
  .exemplar-accession { font-family: monospace; font-weight: 600; color: #2563eb; }
  .exemplar-why { color: #6b7280; margin-top: 0.2em; }

  @media print {
    body { max-width: none; padding: 0.5em; font-size: 10pt; }
    .table-wrap { max-height: none; overflow: visible; }
    details { display: block; }
    details summary { display: none; }
    .chart img { max-width: 100%; }
    .alert-warn { border-color: #ccc; background: #f9f9f9; }
  }
</style>
</head>
<body>
<h1>{{ title }}</h1>

<div class="meta-card">
  <dt>Generated</dt><dd>{{ generated_at }}</dd>
  <dt>PXAudit version</dt><dd>{{ version }}</dd>
  <dt>Database</dt><dd>{{ db_path }}</dd>
  <dt>Accessions</dt><dd>{{ total_count }}</dd>
</div>

{% if minimum_dataset_warning %}
<div class="alert-warn">Warning: fewer than 10 audited accessions.
Distributions may not be meaningful.</div>
{% endif %}

<div class="summary-grid">
  <div class="summary-card">
    <div class="num">{{ total_count }}</div>
    <div class="label">Total<br>accessions</div>
  </div>
  <div class="summary-card">
    <div class="num">{{ verifiable_count }}</div>
    <div class="label">Verifiable</div>
  </div>
  <div class="summary-card">
    <div class="num">{{ unverifiable_count }}</div>
    <div class="label">Unverifiable</div>
  </div>
</div>

<h2>Quality Distribution</h2>
<div class="charts">
  <div class="chart">
    <div class="chart-title">Qualitative</div>
    {{ tier_chart | safe }}
  </div>
  <div class="chart">
    <div class="chart-title">Quantitative</div>
    {{ quant_chart | safe }}
  </div>
</div>

<h2>Metadata Completeness</h2>
<p style="font-size: 0.78em; color: #6b7280; margin: 0.2em 0 0.4em 0;">
Percentage of verifiable studies missing each metadata field.
<span style="color: #ef4444; font-weight: 600;">Red</span> = critical (prevents use),
<span style="color: #f59e0b; font-weight: 600;">Amber</span> = moderate (limits use),
<span style="color: #16a34a; font-weight: 600;">Green</span> = acceptable (minor gap).
</p>
{{ gap_chart | safe }}
<div class="table-wrap">
<table class="gap-table">
<thead><tr><th>Field</th><th>Failed</th><th>Unknown</th><th>Passed</th><th>Missing %</th></tr></thead>
<tbody>
{% for item in gap_items %}
<tr>
  <td>{{ item.field }}</td>
  <td>{{ item.missing }}</td>
  <td>{{ item.unknown }}</td>
  <td>{{ item.present }}</td>
  <td>{{ item.pct_missing }}%</td>
</tr>
{% endfor %}
</tbody>
</table>
</div>

<h2>Cohort Analysis</h2>
<p style="font-size: 0.78em; color: #6b7280; margin: 0.2em 0 0.4em 0;">
Quality distribution by organism and instrument type.
</p>
<div class="charts">
  <div class="chart">
    <div class="chart-title">By Organism</div>
    {{ cohort_organism_chart | safe }}
  </div>
  <div class="chart">
    <div class="chart-title">By Instrument</div>
    {{ cohort_instrument_chart | safe }}
  </div>
</div>

<h2>Tier Reference</h2>
<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1em; margin: 0.5em 0;">
  <div style="background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 0.8em;">
    <div style="font-weight: 600; color: #1f2937; margin-bottom: 0.5em; font-size: 0.9em;">Qualitative Tiers (FAIR Ladder)</div>
    <div style="display: grid; grid-template-columns: auto 1fr; gap: 0.3em 0.8em; font-size: 0.78em;">
      <span style="color: #2563eb; font-weight: 600;">Diamond</span>
      <span style="color: #374151;">All FAIR criteria met: title, organism, instrument, result files, PSI standards, SDRF, open spectra, organism part, publication</span>
      <span style="color: #94a3b8; font-weight: 600;">Platinum</span>
      <span style="color: #374151;">Missing linked publication only</span>
      <span style="color: #f59e0b; font-weight: 600;">Gold</span>
      <span style="color: #374151;">Missing open spectra or organism part annotation</span>
      <span style="color: #cbd5e1; font-weight: 600;">Silver</span>
      <span style="color: #374151;">Missing SDRF experimental-design file</span>
      <span style="color: #b45309; font-weight: 600;">Bronze</span>
      <span style="color: #374151;">Has result files but no PSI-standard results (mzIdentML / mzTab)</span>
      <span style="color: #64748b; font-weight: 600;">Raw</span>
      <span style="color: #374151;">Has basic metadata but no processed result files</span>
      <span style="color: #e2e8f0; font-weight: 600;">None</span>
      <span style="color: #374151;">Missing required metadata: title, organism, or instrument</span>
    </div>
  </div>
  <div style="background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 0.8em;">
    <div style="font-weight: 600; color: #1f2937; margin-bottom: 0.5em; font-size: 0.9em;">Quantitative Tiers</div>
    <div style="display: grid; grid-template-columns: auto 1fr; gap: 0.3em 0.8em; font-size: 0.78em;">
      <span style="color: #16a34a; font-weight: 600;">Quant-Complete</span>
      <span style="color: #374151;">PSI results + quantification table + quantification metadata</span>
      <span style="color: #3b82f6; font-weight: 600;">Quant-Ready</span>
      <span style="color: #374151;">PSI results + quantification table but metadata missing</span>
      <span style="color: #f59e0b; font-weight: 600;">Partial</span>
      <span style="color: #374151;">PSI results present but no quant table, OR has quant table but no PSI results</span>
      <span style="color: #94a3b8; font-weight: 600;">No Quant</span>
      <span style="color: #374151;">No PSI results and no tabular quantification</span>
    </div>
  </div>
</div>

<h2>All Accessions</h2>
<p class="flag-legend">Each flag column:
<span class="badge badge-ok">present</span>
<span class="badge badge-missing">absent</span>
<span class="badge badge-unknown">unknown</span></p>
{% if large_table_warning %}
<div class="alert-warn">Showing {{ row_count }} accessions.</div>
{% endif %}
<details open>
<summary class="table-summary">Show/hide accession table ({{ row_count }} accessions)</summary>
<div class="table-wrap">
<table>
<thead>
<tr>
  <th class="col-acc">Accession</th>
  <th class="col-title">Title</th>
  <th class="col-tier">Qual</th>
  <th class="col-tier">Quant</th>
  {% for label, _ in flag_columns %}
  <th class="col-flag">{{ label }}</th>
  {% endfor %}
</tr>
</thead>
<tbody>
{% for row in rows %}
<tr>
  <td><span class="acc">{{ row.accession }}</span></td>
  <td class="col-title" title="{{ row.title }}">{{ row.title }}</td>
  <td class="col-tier"><span class="tier-{{ row.tier }}">{{ row.tier }}</span></td>
  <td class="col-tier"><span class="tier-{{ row.quant_tier | replace(' ', '-') }}">
{{ row.quant_tier }}</span></td>
  {% for flag in row.flags %}
  <td class="col-flag">{{ flag | safe }}</td>
  {% endfor %}
</tr>
{% endfor %}
</tbody>
</table>
</div>
<p class="footnote">{{ row_count }} accession(s)</p>
</details>

</body>
</html>
"""


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReportData:
    """Aggregated report data."""

    total_count: int
    verifiable_count: int
    unverifiable_count: int
    tier_dist: pd.DataFrame
    quant_dist: pd.DataFrame
    rows: list[dict]
    gap_items: list[dict]
    cohort_organism: pd.DataFrame
    cohort_instrument: pd.DataFrame


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _series_int_sum(series: pd.Series) -> int:
    """Return the integer sum of a numeric pandas Series."""
    return int(series.sum())


def _normalize_flag(value: object) -> str:
    """Normalize v2 integer and v3 text evidence values for read-only reports."""
    raw = getattr(value, "value", value)
    if isinstance(raw, bool):
        return "passed" if raw else "failed"
    if isinstance(raw, int) and raw in (0, 1):
        return "passed" if raw else "failed"
    if isinstance(raw, str):
        normalized = raw.casefold()
        if normalized in {"passed", "failed", "unknown"}:
            return normalized
        if normalized in {"0", "1"}:
            return "passed" if normalized == "1" else "failed"
    return "unknown"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_report(db_path: str | Path, output_dir: str | Path, title: str) -> Path:
    """Generate a self-contained HTML report from a populated database.

    The input database is opened read-only. The output is atomically written to
    ``report.html`` inside ``output_dir``; direct callers replace that file if it exists.

    Parameters
    ----------
    db_path:
        Existing PXAudit SQLite database.
    output_dir:
        Directory that will contain ``report.html``.
    title:
        Report title rendered in the page header and HTML title.

    Returns
    -------
    pathlib.Path
        Path to the completed report.

    Raises
    ------
    FileNotFoundError
        If the input database does not exist.
    ValueError
        If the database has no audited accessions.
    ImportError
        If a report dependency is unavailable.
    OSError
        If the output directory or report cannot be written.
    sqlite3.DatabaseError
        If the input is not a readable PXAudit database.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"database not found: {db_path}")

    conn = _open_db(db_path)
    try:
        data = _collect_report_data(conn, db_path)
    finally:
        conn.close()

    output_dir = Path(output_dir)
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except PermissionError as exc:
        raise PermissionError(f"cannot create output directory {output_dir}: {exc}") from exc
    except OSError as exc:
        raise OSError(f"cannot create output directory {output_dir}: {exc}") from exc

    html = _render_html(data, db_path, title)
    out_path = output_dir / "report.html"
    _write_report(out_path, html)
    return out_path


def _write_report(out_path: Path, html: str) -> None:
    """Atomically replace a report and clean its unique temporary file."""
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=out_path.parent,
            prefix=f".{out_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            handle.write(html)
        os.replace(temporary_path, out_path)
    except PermissionError as exc:
        raise PermissionError(f"cannot write {out_path}: {exc}") from exc
    except OSError as exc:
        raise OSError(f"cannot write {out_path}: {exc}") from exc
    finally:
        if temporary_path is not None and temporary_path.exists():
            try:
                temporary_path.unlink()
            except OSError:
                _log.warning("Could not remove report temporary file %s", temporary_path.name)


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------


def _open_db(db_path: Path) -> sqlite3.Connection:
    """Open an existing report database without creating or migrating it."""
    from pxaudit.db import open_existing_db

    return open_existing_db(db_path)


def _collect_report_data(conn: sqlite3.Connection, db_path: Path) -> ReportData:
    """Run all report queries and return aggregated data."""
    total_row = conn.execute("SELECT COUNT(*) FROM audit").fetchone()
    total_count = int(total_row[0]) if total_row else 0
    if total_count == 0:
        raise ValueError(
            f"no audited accessions found in {db_path}. "
            "Run 'pxaudit check' or 'pxaudit bulk-audit' first."
        )

    ver_row = conn.execute("SELECT COUNT(*) FROM audit WHERE is_unverifiable = 0").fetchone()
    verifiable_count = int(ver_row[0]) if ver_row else 0
    unverifiable_count = total_count - verifiable_count

    tier_dist = _query_tier_distribution(conn)
    quant_dist = _query_quant_tier_distribution(conn)
    rows = _query_all_accessions(conn)
    gap_items = _query_metadata_gaps(conn)
    cohort_organism = _query_cohort_organism(conn)
    cohort_instrument = _query_cohort_instrument(conn)

    return ReportData(
        total_count=total_count,
        verifiable_count=verifiable_count,
        unverifiable_count=unverifiable_count,
        tier_dist=tier_dist,
        quant_dist=quant_dist,
        rows=rows,
        gap_items=gap_items,
        cohort_organism=cohort_organism,
        cohort_instrument=cohort_instrument,
    )


def _now_iso() -> str:
    """Return the current UTC timestamp in ISO 8601 format."""
    return datetime.now(UTC).isoformat()


def _get_version() -> str:
    """Return the installed pxaudit version, or unknown if not installed."""
    try:
        return importlib_metadata.version("pxaudit")
    except importlib_metadata.PackageNotFoundError:
        return "unknown"


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------


def _query_tier_distribution(conn: sqlite3.Connection) -> pd.DataFrame:
    """Return counts for known tiers plus an explicit unknown bucket."""
    df = pd.read_sql_query("SELECT tier, COUNT(*) AS count FROM audit GROUP BY tier", conn)
    if df.empty:
        return pd.DataFrame(
            {
                "tier": _TIER_ORDER,
                "count": [0] * len(_TIER_ORDER),
                "percentage": [0.0] * len(_TIER_ORDER),
            }
        )
    df["tier"] = df["tier"].where(df["tier"].isin(_TIER_ORDER), "Unknown")
    df = df.groupby("tier", as_index=False, dropna=False).agg(count=("count", "sum"))
    total = _series_int_sum(df["count"])
    full = pd.DataFrame({"tier": _TIER_ORDER})
    df = full.merge(df, on="tier", how="left").fillna(0)
    df["count"] = df["count"].astype(int)
    df["percentage"] = df["count"].apply(lambda c: round(100.0 * c / total, 1) if total else 0.0)
    return df


def _query_quant_tier_distribution(conn: sqlite3.Connection) -> pd.DataFrame:
    """Return counts for known quant tiers plus an explicit unknown bucket."""
    df = pd.read_sql_query(
        "SELECT quant_tier, COUNT(*) AS count FROM audit GROUP BY quant_tier",
        conn,
    )
    df["quant_tier"] = df["quant_tier"].where(df["quant_tier"].isin(_QUANT_TIER_ORDER), "Unknown")
    df = df.groupby("quant_tier", as_index=False, dropna=False).agg(count=("count", "sum"))
    total = _series_int_sum(df["count"]) if not df.empty else 0
    full = pd.DataFrame({"quant_tier": _QUANT_TIER_ORDER})
    df = full.merge(df, on="quant_tier", how="left").fillna(0)
    df["count"] = df["count"].astype(int)
    df["percentage"] = df["count"].apply(lambda c: round(100.0 * c / total, 1) if total else 0.0)
    return df


def _query_all_accessions(conn: sqlite3.Connection) -> list[dict]:
    """Return all accessions with flags, sorted by tier then accession."""
    flag_cols = ", ".join(f"a.{col}" for _, col in _FLAG_COLUMNS)
    sql = (
        "SELECT a.accession, COALESCE(s.title, '') AS title, "
        "a.tier, a.quant_tier, " + flag_cols + " "
        "FROM audit a LEFT JOIN study s USING(accession) "
        "ORDER BY "
        "  CASE a.tier "
        "    WHEN 'Diamond' THEN 1 WHEN 'Platinum' THEN 2 "
        "    WHEN 'Gold' THEN 3 WHEN 'Silver' THEN 4 "
        "    WHEN 'Bronze' THEN 5 WHEN 'Raw' THEN 6 "
        "    WHEN 'None' THEN 7 "
        "    ELSE 8 "
        "  END, "
        "  a.accession"
    )
    rows: list[dict[str, typing.Any]] = []
    for db_row in conn.execute(sql).fetchall():
        row: dict[str, typing.Any] = {
            "accession": str(db_row[0]),
            "title": str(db_row[1]),
            "tier": db_row[2] if db_row[2] in _TIER_ORDER else "Unknown",
            "quant_tier": db_row[3] if db_row[3] in _QUANT_TIER_ORDER else "Unknown",
            "flags": [],
        }
        raw_flags = list(db_row[4:])
        for (_label, _col), val in zip(_FLAG_COLUMNS, raw_flags, strict=False):
            outcome = _normalize_flag(val)
            if outcome == "failed":
                cls, text = "badge badge-missing", "-"
            elif outcome == "passed":
                cls, text = "badge badge-ok", "+"
            else:
                cls, text = "badge badge-unknown", "?"
            row["flags"].append(f'<span class="{cls}">{text}</span>')
        rows.append(row)
    return rows


# Metadata gap severity levels.
_GAP_SEVERITY: dict[str, str] = {
    "result_files": "critical",
    "instrument": "critical",
    "organism": "critical",
    "title": "acceptable",
    "organism_id": "acceptable",
    "psi_results": "moderate",
    "sdrf": "moderate",
    "open_spectra": "moderate",
    "organism_part": "moderate",
    "publication": "moderate",
    "tabular_quant": "moderate",
    "quant_metadata": "moderate",
    "mztab": "moderate",
}


def _query_metadata_gaps(conn: sqlite3.Connection) -> list[dict]:
    """Return present, missing, and unknown flag counts for verifiable studies."""
    columns = [col for _, col in _FLAG_COLUMNS]
    rows = conn.execute(
        "SELECT " + ", ".join(columns) + " FROM audit WHERE is_unverifiable = 0"
    ).fetchall()
    counts = {column: {"passed": 0, "failed": 0, "unknown": 0} for column in columns}
    for row in rows:
        for column, value in zip(columns, row, strict=True):
            counts[column][_normalize_flag(value)] += 1

    results: list[dict[str, typing.Any]] = []
    for label, col in _FLAG_COLUMNS:
        results.append(
            {
                "field": label,
                "missing": counts[col]["failed"],
                "unknown": counts[col]["unknown"],
                "present": counts[col]["passed"],
                "severity": _GAP_SEVERITY.get(label, "moderate"),
            }
        )
    verifiable = len(rows)
    for item in results:
        item["pct_missing"] = round(100.0 * item["missing"] / verifiable, 1) if verifiable else 0.0
    results.sort(key=lambda x: x["missing"], reverse=True)
    return results


def _query_cohort_organism(conn: sqlite3.Connection) -> pd.DataFrame:
    """Return quality distribution for the ten largest organism cohorts."""
    sql = (
        "SELECT COALESCE(s.organism, 'Unknown') AS organism, a.tier, COUNT(*) AS count "
        "FROM audit a LEFT JOIN study s USING(accession) "
        "WHERE a.is_unverifiable = 0 "
        "GROUP BY organism, a.tier "
        "ORDER BY organism, "
        "  CASE a.tier "
        "    WHEN 'Diamond' THEN 1 WHEN 'Platinum' THEN 2 "
        "    WHEN 'Gold' THEN 3 WHEN 'Silver' THEN 4 "
        "    WHEN 'Bronze' THEN 5 WHEN 'Raw' THEN 6 "
        "    WHEN 'None' THEN 7 ELSE 8 "
        "  END"
    )
    return _limit_cohorts(pd.read_sql_query(sql, conn), "organism")


def _query_cohort_instrument(conn: sqlite3.Connection) -> pd.DataFrame:
    """Return quality distribution for the ten largest instrument cohorts."""
    sql = (
        "SELECT COALESCE(s.instrument, 'Unknown') AS instrument, a.tier, COUNT(*) AS count "
        "FROM audit a LEFT JOIN study s USING(accession) "
        "WHERE a.is_unverifiable = 0 "
        "GROUP BY instrument, a.tier "
        "ORDER BY instrument, "
        "  CASE a.tier "
        "    WHEN 'Diamond' THEN 1 WHEN 'Platinum' THEN 2 "
        "    WHEN 'Gold' THEN 3 WHEN 'Silver' THEN 4 "
        "    WHEN 'Bronze' THEN 5 WHEN 'Raw' THEN 6 "
        "    WHEN 'None' THEN 7 ELSE 8 "
        "  END"
    )
    return _limit_cohorts(pd.read_sql_query(sql, conn), "instrument")


def _limit_cohorts(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    """Normalize tiers and select the ten largest cohorts with alphabetical ties."""
    if df.empty:
        return df
    df["tier"] = df["tier"].where(df["tier"].isin(_TIER_ORDER), "Unknown")
    df = df.groupby([group_col, "tier"], as_index=False, dropna=False).agg(count=("count", "sum"))
    totals = (
        df.groupby(group_col, as_index=False)
        .agg(count=("count", "sum"))
        .sort_values(["count", group_col], ascending=[False, True])
        .head(10)
    )
    return df[df[group_col].isin(totals[group_col])].copy()


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------


def _render_html(data: ReportData, db_path: Path, title: str) -> str:
    """Build the full HTML report string from collected data."""
    jinja2 = _import_jinja2()
    env = jinja2.Environment(autoescape=True, undefined=jinja2.StrictUndefined)
    template = env.from_string(_HTML_TEMPLATE)

    tier_dist = data.tier_dist
    # Find best/worst tiers that actually have data.
    tier_with_data = tier_dist[tier_dist["count"] > 0]
    best_tier = tier_with_data.iloc[0]["tier"] if not tier_with_data.empty else "-"
    worst_tier = tier_with_data.iloc[-1]["tier"] if not tier_with_data.empty else "-"

    quant_dist = data.quant_dist
    quant_with_data = quant_dist[quant_dist["count"] > 0]
    best_quant = quant_with_data.iloc[0]["quant_tier"] if not quant_with_data.empty else "-"
    worst_quant = quant_with_data.iloc[-1]["quant_tier"] if not quant_with_data.empty else "-"

    context = {
        "title": title,
        "generated_at": _now_iso(),
        "version": _get_version(),
        "db_path": str(db_path),
        "total_count": data.total_count,
        "verifiable_count": data.verifiable_count,
        "unverifiable_count": data.unverifiable_count,
        "best_tier": best_tier,
        "worst_tier": worst_tier,
        "best_quant": best_quant,
        "worst_quant": worst_quant,
        "minimum_dataset_warning": data.total_count < 10,
        "large_table_warning": len(data.rows) > 100,
        "tier_chart": _render_tier_chart(data.tier_dist),
        "quant_chart": _render_quant_tier_chart(data.quant_dist),
        "gap_chart": _render_gap_chart(data.gap_items),
        "gap_items": data.gap_items,
        "cohort_organism_chart": _render_cohort_chart(data.cohort_organism, "organism"),
        "cohort_instrument_chart": _render_cohort_chart(data.cohort_instrument, "instrument"),
        "flag_columns": _FLAG_COLUMNS,
        "rows": data.rows,
        "row_count": len(data.rows),
    }
    return str(template.render(**context))


# ---------------------------------------------------------------------------
# Chart rendering
# ---------------------------------------------------------------------------


def _render_chart(func: typing.Any, *args: typing.Any, **kwargs: typing.Any) -> str:
    """Render a matplotlib chart as a base64-encoded PNG string."""
    try:
        import matplotlib
    except ImportError as exc:
        raise ImportError(MATPLOTLIB_MISSING_MSG) from exc
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": "#d1d5db",
            "axes.grid": False,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "font.family": "sans-serif",
            "font.size": 10,
            "axes.titlesize": 12,
            "axes.titleweight": "bold",
            "axes.labelsize": 10,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "legend.fontsize": 9,
            "figure.dpi": 150,
            "savefig.dpi": 150,
            "savefig.bbox": "tight",
            "savefig.pad_inches": 0.1,
        }
    )
    fig = func(*args, **kwargs)
    try:
        buf = BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight", dpi=150, facecolor="white")
        encoded = base64.b64encode(buf.getvalue()).decode("ascii")
    finally:
        plt.close(fig)
    return f'<img src="data:image/png;base64,{encoded}" alt="chart">'


def _render_tier_chart(df: pd.DataFrame) -> str:
    """Render the tier distribution donut chart with full legend."""
    if df.empty or _series_int_sum(df["count"]) == 0:
        return '<p class="placeholder">No data available.</p>'

    def _build() -> typing.Any:
        import matplotlib.pyplot as plt

        fig, (ax_pie, ax_legend) = plt.subplots(
            1, 2, figsize=(8, 4), gridspec_kw={"width_ratios": [1, 0.8]}
        )

        # Filter out zero counts for the pie chart.
        df_plot = df[df["count"] > 0].copy()
        total = _series_int_sum(df["count"])

        if not df_plot.empty:
            colors = [_TIER_COLORS.get(t, "#999999") for t in df_plot["tier"]]
            wedges, texts, autotexts = ax_pie.pie(
                df_plot["count"],
                labels=None,
                colors=colors,
                autopct=lambda pct: f"{pct:.1f}%" if pct > 3 else "",
                pctdistance=0.75,
                startangle=90,
                wedgeprops=dict(width=0.4, edgecolor="white", linewidth=2),
            )
            for autotext in autotexts:
                autotext.set_fontsize(9)
                autotext.set_fontweight("bold")
                autotext.set_color("white")
        else:
            pass  # pragma: no cover: outer empty check prevents this branch

        # Center text.
        ax_pie.text(
            0,
            0,
            f"{total}\naccessions",
            ha="center",
            va="center",
            fontsize=12,
            fontweight="bold",
            color="#1f2937",
        )
        ax_pie.set_aspect("equal")
        ax_pie.set_title(
            "Qualitative Tiers", fontsize=11, fontweight="600", color="#1f2937", pad=10
        )

        # Full legend showing ALL tiers (even with count=0).
        ax_legend.axis("off")
        y_pos = 0.95
        for tier in _TIER_ORDER:
            cnt = int(df[df["tier"] == tier]["count"].sum()) if not df.empty else 0
            pct = round(100.0 * cnt / total, 1) if total > 0 else 0.0
            color = _TIER_COLORS.get(tier, "#999999")

            # Draw colored square.
            ax_legend.add_patch(
                plt.Rectangle(
                    (0.05, y_pos - 0.02),
                    0.08,
                    0.04,
                    facecolor=color,
                    edgecolor="white",
                    linewidth=1,
                )
            )
            # Tier name.
            ax_legend.text(
                0.18, y_pos, tier, fontsize=9, fontweight="600", color="#1f2937", va="center"
            )
            # Count and percentage.
            ax_legend.text(
                0.75, y_pos, f"{cnt} ({pct}%)", fontsize=8, color="#6b7280", va="center", ha="right"
            )
            y_pos -= 0.12

        ax_legend.set_xlim(0, 1)
        ax_legend.set_ylim(y_pos, 1.05)

        plt.tight_layout()
        return fig

    return _render_chart(_build)


def _render_quant_tier_chart(df: pd.DataFrame) -> str:
    """Render the quant tier distribution donut chart with full legend."""
    if df.empty or _series_int_sum(df["count"]) == 0:
        return '<p class="placeholder">No data available.</p>'

    def _build() -> typing.Any:
        import matplotlib.pyplot as plt

        fig, (ax_pie, ax_legend) = plt.subplots(
            1, 2, figsize=(8, 4), gridspec_kw={"width_ratios": [1, 0.8]}
        )

        # Filter out zero counts for the pie chart.
        df_plot = df[df["count"] > 0].copy()
        total = _series_int_sum(df["count"])

        if not df_plot.empty:
            colors = [_QUANT_TIER_COLORS.get(t, "#999999") for t in df_plot["quant_tier"]]
            wedges, texts, autotexts = ax_pie.pie(
                df_plot["count"],
                labels=None,
                colors=colors,
                autopct=lambda pct: f"{pct:.1f}%" if pct > 3 else "",
                pctdistance=0.75,
                startangle=90,
                wedgeprops=dict(width=0.4, edgecolor="white", linewidth=2),
            )
            for autotext in autotexts:
                autotext.set_fontsize(9)
                autotext.set_fontweight("bold")
                autotext.set_color("white")
        else:
            pass  # pragma: no cover: outer empty check prevents this branch

        # Center text.
        ax_pie.text(
            0,
            0,
            f"{total}\naccessions",
            ha="center",
            va="center",
            fontsize=12,
            fontweight="bold",
            color="#1f2937",
        )
        ax_pie.set_aspect("equal")
        ax_pie.set_title(
            "Quantitative Tiers", fontsize=11, fontweight="600", color="#1f2937", pad=10
        )

        # Full legend showing ALL tiers (even with count=0).
        ax_legend.axis("off")
        y_pos = 0.95
        for tier in _QUANT_TIER_ORDER:
            cnt = int(df[df["quant_tier"] == tier]["count"].sum()) if not df.empty else 0
            pct = round(100.0 * cnt / total, 1) if total > 0 else 0.0
            color = _QUANT_TIER_COLORS.get(tier, "#999999")

            # Draw colored square.
            ax_legend.add_patch(
                plt.Rectangle(
                    (0.05, y_pos - 0.02),
                    0.08,
                    0.04,
                    facecolor=color,
                    edgecolor="white",
                    linewidth=1,
                )
            )
            # Tier name.
            ax_legend.text(
                0.18, y_pos, tier, fontsize=9, fontweight="600", color="#1f2937", va="center"
            )
            # Count and percentage.
            ax_legend.text(
                0.75, y_pos, f"{cnt} ({pct}%)", fontsize=8, color="#6b7280", va="center", ha="right"
            )
            y_pos -= 0.12

        ax_legend.set_xlim(0, 1)
        ax_legend.set_ylim(y_pos, 1.05)

        plt.tight_layout()
        return fig

    return _render_chart(_build)


def _render_gap_chart(gap_items: list[dict]) -> str:
    """Render the metadata gaps horizontal bar chart."""
    if not gap_items:
        return '<p class="placeholder">No data available.</p>'  # pragma: no cover

    def _build() -> typing.Any:
        import matplotlib.pyplot as plt

        fields = [item["field"] for item in gap_items]
        pcts = [item["pct_missing"] for item in gap_items]
        severities = [item["severity"] for item in gap_items]
        color_map = {"critical": "#dc2626", "moderate": "#d97706", "acceptable": "#16a34a"}
        colors = [color_map.get(s, "#999999") for s in severities]

        fig, ax = plt.subplots(figsize=(6, 0.35 * len(fields) + 0.6))
        bars = ax.barh(fields, pcts, color=colors, edgecolor="white", height=0.6)
        ax.set_xlabel("% Missing")
        ax.set_xlim(0, 100)
        ax.invert_yaxis()
        for bar, pct in zip(bars, pcts, strict=False):
            w = bar.get_width()
            ax.text(
                w + 1,
                bar.get_y() + bar.get_height() / 2,
                f"  {pct:.1f}%",
                va="center",
                fontsize=8,
            )
        ax.margins(x=0.15)
        return fig

    return _render_chart(_build)


def _render_cohort_chart(df: pd.DataFrame, group_col: str) -> str:
    """Render a cohort quality stacked bar chart."""
    if df.empty:
        return '<p class="placeholder">No cohort data available.</p>'

    def _build() -> typing.Any:
        import matplotlib.pyplot as plt

        pivot = df.pivot_table(index=group_col, columns="tier", values="count", fill_value=0)
        # Reorder columns to match _TIER_ORDER.
        ordered_cols = [t for t in _TIER_ORDER if t in pivot.columns]
        pivot = pivot[ordered_cols]
        # Sort rows by total count descending.
        pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=True).index]
        fig, ax = plt.subplots(figsize=(6, 0.35 * len(pivot) + 0.8))
        colors = [_TIER_COLORS.get(t, "#999999") for t in ordered_cols]
        pivot.plot(kind="barh", stacked=True, ax=ax, color=colors, edgecolor="white")
        ax.set_xlabel("Count")
        ax.legend().remove()
        ax.margins(x=0.15)
        return fig

    return _render_chart(_build)


# ---------------------------------------------------------------------------
# Optional dependency import helper
# ---------------------------------------------------------------------------


def _import_jinja2() -> typing.Any:
    """Import jinja2 or raise an ImportError with a helpful install message."""
    try:
        return importlib.import_module("jinja2")
    except ImportError as exc:
        raise ImportError(JINJA2_MISSING_MSG) from exc
