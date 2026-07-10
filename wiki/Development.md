# Development

## Setup

```bash
git clone https://github.com/LangeLab/PXAudit.git
cd PXAudit
uv sync
uv run pre-commit install
```

## Project layout

```bash
src/pxaudit/
├── __init__.py        # Version, PRIDE prefix
├── cli.py             # Click commands: check, bulk-audit, manifest, report
├── tier_engine.py     # FAIR ladder scoring + quant tier logic
├── file_classifier.py # Filename to FileClass mapping
├── pride_client.py    # PRIDE REST API v3 client
├── db.py              # SQLite schema, inserts, migrations
├── report.py          # HTML report generation (Jinja2 + matplotlib)
└── cache.py           # Local JSON response cache
```

## Running tests

```bash
uv run pytest                              # unit tests (no network)
uv run pytest --cov=pxaudit                # with coverage report
uv run pytest -m integration -v --no-cov   # live API tests (requires network)
```

There are 514 unit tests and 12 integration tests, all at 100% branch coverage.

## Code quality checks

Everything is enforced in CI and pre-commit:

- `ruff check`: linting (E, F, W, I, UP, B, SIM rules)
- `ruff format --check`: formatting (line length 100)
- `mypy src/ tests/`: strict type checking
- `pytest --cov-fail-under=100`: coverage must stay at 100%

Run them all locally:

```bash
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run mypy src/ tests/
uv run pytest --cov=pxaudit --cov-fail-under=100
```

Note: `report.py` requires `jinja2` and `matplotlib` (optional deps). Install with `uv sync --extra report`.

## Style guide

Conventions are enforced by `ruff`, `mypy`, and pre-commit (see Development Setup above). Prefer reStructuredText docstrings, `from __future__ import annotations`, double quotes, and line length 100. Library code raises typed exceptions; the CLI validates input and exits with a status code.

## Adding documentation

Wiki pages live in `wiki/` and use standard Markdown. GitHub Wiki links use `[[Page Name]]` syntax. Add new pages to `_Sidebar.md` under the appropriate section. The `_Footer.md` renders at the bottom of every page when synced to GitHub Wiki.

Page template:

```markdown
# Title

Brief description of what this page covers (one paragraph).

## Section one

Content here. Use `: ` for definition lists instead of `--`.

## Section two

```bash
# Code examples where useful
```

When adding a page that documents a feature, include version tags in the footer comment to track when it was introduced or changed.
