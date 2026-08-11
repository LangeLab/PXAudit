<!-- markdownlint-disable MD010 MD033 MD036 MD041 -->
<p align="center">
  <img src="https://raw.githubusercontent.com/LangeLab/PXAudit/main/assets/banner.svg" alt="PXAudit" width="180">
</p>

<p align="center">
  Audit Proteomics Exchange (PRIDE) study metadata from the command line.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.12--3.14-2D7D46?style=flat-square&logo=python&logoColor=white" alt="Python 3.12-3.14">
  <img src="https://img.shields.io/badge/version-0.5.2-8B5CF6?style=flat-square" alt="v0.5.2">
  <img src="https://img.shields.io/badge/status-beta-C17D10?style=flat-square" alt="Beta">
  <a href="https://github.com/LangeLab/PXAudit/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/LangeLab/PXAudit/ci.yml?branch=main&style=flat-square&logo=github&label=CI" alt="CI"></a>
  <a href="https://codecov.io/gh/LangeLab/PXAudit"><img src="https://img.shields.io/codecov/c/github/LangeLab/PXAudit?branch=main&style=flat-square&logo=codecov&logoColor=white" alt="Coverage"></a>
  <img src="https://img.shields.io/badge/license-MIT-4B9D6E?style=flat-square" alt="MIT">
</p>

<p align="center">
  <a href="CHANGELOG.md"><img src="https://img.shields.io/badge/changelog-CHANGELOG-E05D44?style=flat-square" alt="Changelog"></a>
  <a href="CITATION.cff"><img src="https://img.shields.io/badge/cite-CITATION.cff-0066CC?style=flat-square" alt="Citation"></a>
  <a href="https://github.com/LangeLab/PXAudit/wiki"><img src="https://img.shields.io/badge/docs-Wiki-0F766E?style=flat-square" alt="Docs"></a>
</p>

I made PXAudit because I was checking PRIDE submissions by hand and got tired of repeating the same steps.

PXAudit reads project metadata and file listings from the [PRIDE Archive](https://www.ebi.ac.uk/pride/), classifies the deposited files, assigns a seven-level [FAIR](https://doi.org/10.1038/sdata.2016.18) tier and a separate quantification-readiness tier, then saves the audit to SQLite. It does not download deposited data files.

## Quick start

PXAudit supports Python 3.12 through 3.14. The source checkout uses [uv](https://docs.astral.sh/uv/):

```bash
git clone https://github.com/LangeLab/PXAudit.git
cd PXAudit
uv sync
uv run pxaudit check PXD000001
```

The first audit queries PRIDE and creates `pxaudit_results.db` in the current directory. API responses are cached under `~/.pxaudit_cache/`, so a fresh repeat audit does not need another request.

PXAudit currently audits PRIDE `PXD` accessions. Safe identifiers from other ProteomeXchange repositories are accepted as `Unverifiable`; repository adapters are not implemented yet.

## Common tasks

```bash
# Audit a list containing one accession per line
uv run pxaudit bulk-audit --input accessions.txt --format tsv --output results.tsv
# For large local runs, add --batch-size N to commit after each N accessions.

# Inspect the stored file inventory
uv run pxaudit manifest PXD000001

# Review effective settings and cache state
uv run pxaudit config show
uv run pxaudit cache info

# Generate a self-contained HTML report
uv sync --extra report
uv run pxaudit report --db pxaudit_results.db --output report/
```

The default batch size is `1`, preserving per-accession durability. A stop-on-error or interrupted run rolls back its active batch; `--continue-on-error` commits pending successes before skipping the failed accession.

Run `uv run pxaudit --help` or `uv run pxaudit COMMAND --help` for command-line help.

## Documentation

The [wiki](https://github.com/LangeLab/PXAudit/wiki) contains the detailed contracts and examples:

- [Home](https://github.com/LangeLab/PXAudit/wiki/Home): workflow overview and report preview
- [CLI Reference](https://github.com/LangeLab/PXAudit/wiki/CLI-Reference): commands, flags, configuration, caching, errors, and exit codes
- [Tier System](https://github.com/LangeLab/PXAudit/wiki/Tier-System): FAIR and quantification tiers, evidence flags, and scoring rules
- [Database Schema](https://github.com/LangeLab/PXAudit/wiki/Database-Schema): tables, columns, migrations, and example queries
- [FAQ](https://github.com/LangeLab/PXAudit/wiki/FAQ): supported accessions, file classification, offline use, and common scoring questions
- [Development](https://github.com/LangeLab/PXAudit/wiki/Development): setup, architecture, testing, style, and CI

Contributions and issue reports are much appreciated. The development guide explains the local checks and documentation workflow.

---

## Citation

If you use PXAudit in your research, please cite it as:

```bibtex
@software{ergin_pxaudit_2026,
  author   = {Ergin, Enes Kemal},
  title    = {{PXAudit}: A command-line tool for auditing {Proteomics Exchange} study metadata},
  year     = {2026},
  version  = {0.5.2},
  url      = {https://github.com/LangeLab/PXAudit},
  license  = {MIT},
}
```

A `CITATION.cff` file is included in the repository root for tools that parse it automatically (e.g. GitHub's _Cite this repository_ button, Zenodo).

---

## License

MIT License. See [LICENSE](LICENSE) for details.
