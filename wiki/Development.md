# Development

The part I worry about most is a test that passes while encoding the wrong scientific meaning. Coverage still matters, but it cannot tell me whether the evidence behind a tier is defensible. Changes to PXAudit need both software evidence and domain evidence.

This page describes the public repository workflow for PXAudit 0.5.3.

## Set up a checkout

PXAudit supports Python 3.12 through 3.14 and uses [uv](https://docs.astral.sh/uv/) for environments and locked dependencies.

```bash
git clone https://github.com/LangeLab/PXAudit.git
cd PXAudit
uv sync --all-extras
uv run pre-commit install
```

Confirm the environment:

```bash
uv run pxaudit --version
uv run pytest
```

`--all-extras` installs the optional report dependencies in addition to the development tools. Use `uv sync` when report development is not needed.

## Architecture

```text
CLI input
  -> accession validation
  -> PRIDE client and local cache
  -> file classifier
  -> tier engine
  -> SQLite transaction
  -> terminal, export, manifest, or HTML report
```

| Module | Owns | Does not own |
| --- | --- | --- |
| `accession.py` | Identifier grammar and canonicalization | HTTP routing or scoring |
| `pride_client.py` | PRIDE requests, retries, response validation, pagination | Cache, database, terminal output |
| `cache.py` | Cache identity, provenance, freshness, atomic writes, safe maintenance | Network requests or tier logic |
| `file_classifier.py` | Filename and PRIDE-category classification | I/O or audit persistence |
| `tier_engine.py` | Evidence flags and both tier calculations | Fetching, SQLite, presentation |
| `db.py` | Schema, migrations, transactions, read-only connections | Scientific interpretation |
| `cli.py` | Command parsing and orchestration | Owning classifier or tier rules |
| `_output.py` | Terminal presentation modes | Domain decisions |
| `report.py` | Read-only report queries, charts, and HTML | Re-scoring or database mutation |

Keep a rule in the narrowest module that owns it. For example, a new filename format belongs in `file_classifier.py`; the tier engine should consume the resulting class rather than learn a second copy of the filename rule.

## Repository layout

```text
src/pxaudit/          package source
tests/                offline, recorded, and live test modules
tests/fixtures/pride/ reviewed sanitized PRIDE projections
wiki/                 GitHub Wiki source pages
.github/              CI, live verification, and Wiki synchronization
assets/               banner and report preview
```

## Make a change

1. Read the affected source and tests completely.
2. Write down the observable behavior that should change and the behavior that must not change.
3. Add or update the narrowest test that would fail without the intended result.
4. Change the owning module without mixing in unrelated cleanup.
5. Run the focused test first.
6. Run the complete offline quality gates.
7. Update every user-facing contract affected by the change.

Do not change a test merely because current code fails it. First decide whether the implementation or the expectation has the defensible meaning.

## Test evidence levels

PXAudit separates test evidence by boundary.

### L0: deterministic unit and contract tests

L0 tests exercise one module or public contract using synthetic inputs. They cover valid, empty, malformed, boundary, and failure cases without using the network or real user paths.

Examples include accession grammar, classifier precedence, every qualitative flag combination, quant-tier combinations, cache envelope validation, and CLI output contracts.

### L1: component workflows

L1 tests combine real PXAudit components with temporary cache directories, SQLite databases, exports, and reports while mocking external PRIDE requests. They prove cross-module effects such as atomic persistence and cache-to-database provenance.

```bash
uv run pytest -m component --no-cov
```

### L2: recorded PRIDE payloads

L2 tests use reviewed and sanitized PRIDE response projections. Each fixture records its accession, retrieval date, API version, source URL, and scope. These tests protect against real payload shapes while remaining deterministic.

```bash
uv run pytest -m recorded --no-cov
```

Recorded evidence is not live evidence. It proves behavior against the reviewed response, not the current remote record.

### L3: live PRIDE integration

L3 tests query the current PRIDE Archive API and are marked `integration`. They are excluded from the default suite because network availability and remote records can change.

```bash
uv run pytest -m integration -v --no-cov
```

The manual **Live PRIDE verification** workflow writes a dated JSON artifact containing the API version, accession inventory, observed evidence, tier changes, and completeness. Run it before a release and after changes to PRIDE parsing, file classification, or tier semantics.

## Test isolation

An autouse fixture gives every offline test a temporary:

- home directory;
- working directory;
- cache directory;
- configuration path;
- matplotlib cache; and
- output location.

It also blocks socket connection, address lookup, and datagram methods. An offline test that attempts network access fails immediately rather than depending on the developer's connection.

Tests must not inspect or mutate the real home directory, cache, configuration, database, or current-directory output. Use `tmp_path` for filesystem behavior and close sessions, files, and database connections on success and failure paths.

## Quality gates

Run the repository checks before handing off code:

```bash
uv run pytest
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run mypy src/ tests/
```

> [!IMPORTANT]
> The default pytest command runs L0 through L2, excludes live integration tests, and enforces 100% statement and branch coverage. Coverage is a floor. Semantic assertions still need to prove the intended outcome.

Pre-commit can apply Ruff fixes and formatting, then run mypy:

```bash
uv run pre-commit run --all-files
```

## CI

The main CI workflow runs:

- Ruff lint, Ruff format checking, and mypy on Ubuntu with Python 3.12;
- the complete offline suite on Python 3.12, 3.13, and 3.14 across Ubuntu, macOS, and Windows;
- a hash-verified dependency audit from the lockfile; and
- a secret scan with the full Git history available.

Workflow actions are pinned to commit SHAs, permissions are scoped per job, redundant runs on the same ref are cancelled, and jobs have explicit timeouts.

The live PRIDE workflow is manual, separate from default CI, and uploads its verification record even when the live assertions detect drift.

## Style

Python code targets a 100-character line length, double quotes, modern annotations, and `from __future__ import annotations`. Library functions are fully annotated. Public modules, classes, and functions use NumPy-style docstrings where sections add useful contract information.

Comments are rare. Use one when the reason for a scientific, safety, ordering, or compatibility constraint cannot be expressed by the code itself. Explain why the constraint exists, not what the next line does.

Library modules raise typed errors. The CLI converts expected errors into stable messages and exit codes. Preserve original exceptions as causes when translating failures.

## Scientific changes

A file-classification or tier change needs more than a happy-path test:

1. Add representative positive, negative, ambiguous, and compressed filenames.
2. Prove broad processed-result evidence does not become narrow PSI evidence accidentally.
3. Check both qualitative and quantitative effects.
4. Compare known recorded payloads.
5. Bump `tier_logic_version` when stored outcomes can change.
6. Add a re-scoring note to the changelog and [[Tier System]].
7. Run live verification when current PRIDE behavior matters.

Never use a failed or incomplete remote fetch as confirmed negative evidence.

## Database and filesystem changes

Treat configured paths, cache contents, filenames, and database values as untrusted input.

- Validate ownership before deleting or replacing a file.
- Keep writes atomic where interruption can leave corrupt state.
- Test refusal paths without touching broad real directories.
- Preserve prior completed database rows when new evidence is incomplete.
- Open read-only commands without creating or migrating their input database.
- Test rollback when any stage of a multi-table write fails.

## Performance work

Do not call a change faster or more memory-efficient because it looks vectorized. Define a verified reference result, run the reference and candidate on identical representative inputs, and reject the candidate if any semantic output changes.

Record workload size, machine context, wall time, peak memory, database size when relevant, and repeated measurements. Optimize only after identifying a measured bottleneck.

## Documentation

Wiki pages live in `wiki/` and use standard Markdown plus GitHub Wiki links such as `[[Tier System]]`. Add or reorder pages in `_Sidebar.md`; `_Footer.md` appears on each synced wiki page.

Each page has one job:

- `Home.md` gets a new reader to a useful result.
- `CLI-Reference.md` documents commands and runtime behavior.
- `Tier-System.md` owns scoring and evidence semantics.
- `Database-Schema.md` owns persistence and query contracts.
- `FAQ.md` handles decisions and troubleshooting.
- `Development.md` owns contributor workflow.

Verify commands, flags, defaults, versions, schema names, and examples against source. Use personal language only for an actual preference, motivation, or decision. Use direct professional language for behavior that can be checked.

## Release metadata

For a version change, keep these surfaces synchronized:

- `pyproject.toml` and the root package entry in `uv.lock`;
- `CHANGELOG.md`;
- `CITATION.cff` and citation examples;
- the README badge and citation;
- wiki version statements and footer; and
- CLI `--version` output after reinstalling the editable package.

Do not rewrite version numbers inside historical changelog entries or recorded fixture metadata.
