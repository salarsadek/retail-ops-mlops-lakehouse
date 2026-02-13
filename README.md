# Retail Ops MLOps Lakehouse (M5)

[![CI](https://github.com/salarsadek/retail-ops-mlops-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/salarsadek/retail-ops-mlops-lakehouse/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/salarsadek/retail-ops-mlops-lakehouse)](https://github.com/salarsadek/retail-ops-mlops-lakehouse/releases)

A **PowerShell-first**, cross-platform MLOps mini-lakehouse for **M5-style retail forecasting**.

**Highlights**
- Reproducible end-to-end runner: `ensure-dirs → train → eval`
- Data quality gate + validation artifacts
- LaTeX-friendly outputs (CSV + `.tex`, PDF + PNG)
- CI on **Windows + Ubuntu** (Python 3.10)

---

## Quickstart

### 1) Create and activate a virtual environment

#### Windows (PowerShell)

```powershell
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -e ".[dev]"
```

#### Linux/macOS (bash)

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install -e ".[dev]"
```

---

### 2) Run quality gate + tests

```powershell
python -m pre_commit run --all-files
python -m pytest -q
```

---

### 3) Run the pipeline (recommended)

This runs the end-to-end baseline and writes artifacts to `outputs/`.

```powershell
.\scriptsun_m5.ps1 -Force
```

> The runner auto-detects the venv Python and works in both Windows PowerShell and `pwsh` (PowerShell 7).

---

## What gets generated?

After a successful run you should see:

- **Models:** `outputs/models/` (e.g. `m5_ridge_baseline.joblib`)
- **Tables:** `outputs/tables/` (CSV + LaTeX `.tex`)
- **Figures:** `outputs/figures/` (PDF + PNG)
- **Reports:** `outputs/reports/` (JSON summaries per step)

Example:

```text
outputs/
  models/
  tables/
  figures/
  reports/
```

---

## CLI entrypoints (optional)

The PowerShell runner calls these commands (you can run them manually too):

```powershell
python -m retail_ops_mlops.cli ensure-dirs
python -m retail_ops_mlops.cli train-m5 --force
python -m retail_ops_mlops.cli eval-m5 --force
```

---

## Repo structure (high level)

```text
configs/              # configuration (paths, logging, etc.)
data/                 # raw/interim/processed (gitignored as appropriate)
outputs/              # generated artifacts (gitignored; .gitkeep kept)
scripts/run_m5.ps1    # PowerShell-first pipeline runner
src/retail_ops_mlops/ # library + pipelines + CLI
tests/                # smoke tests (CI)
```

---

## Troubleshooting

### “pwsh: not found” (Linux)
On Ubuntu CI runners `pwsh` is available by default; locally you may need to install PowerShell 7.
Alternatively, run the pipeline via the CLI entrypoints instead of the PS runner.

### Windows execution policy blocks activation
If `Activate.ps1` is blocked:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

### Where do I configure paths?
Edit `configs/default.yaml`. The runner and CLI read config from there.

---

## License

MIT (update if you prefer another license).
