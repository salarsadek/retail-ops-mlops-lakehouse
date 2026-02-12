# Retail Ops MLOps Lakehouse (M5)

[![CI](https://github.com/salarsadek/retail-ops-mlops-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/salarsadek/retail-ops-mlops-lakehouse/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/salarsadek/retail-ops-mlops-lakehouse)](https://github.com/salarsadek/retail-ops-mlops-lakehouse/releases)

PowerShell-robust MLOps mini-lakehouse for **M5-style retail forecasting**:

- Reproducible pipeline (ensure dirs → train → eval)
- Data quality gate
- LaTeX-friendly outputs (tables/figures)
- Cross-platform CI (Windows + Ubuntu)

## Quickstart

### Windows (PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -e ".[dev]"

python -m pre_commit run --all-files
python -m pytest -q

pwsh .\scripts\run_m5.ps1 -Force
```

### Linux/macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install -e ".[dev]"

python -m pre_commit run --all-files
python -m pytest -q

pwsh ./scripts/run_m5.ps1 -Force
```

## What gets generated?

- **Models:** `outputs/models/`
- **Tables:** `outputs/tables/` (CSV + optional `.tex`)
- **Figures:** `outputs/figures/` (PDF + PNG)
- **Reports:** `outputs/reports/` (JSON summaries)

## CLI entrypoints

The PowerShell runner calls these (you can also run them directly):

```bash
python -m retail_ops_mlops.cli ensure-dirs
python -m retail_ops_mlops.cli train-m5 --force
python -m retail_ops_mlops.cli eval-m5 --force
```

## Notes

- The repo is designed to be **PowerShell-first** (robust paths, venv detection, CI friendly).
- Without `-Force`, scripts may reuse existing outputs when possible.

## License

MIT (update if you prefer another license).
