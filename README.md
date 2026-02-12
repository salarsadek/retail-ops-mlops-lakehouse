# retail-ops-mlops-lakehouse

A portfolio-ready **Retail Ops / MLOps mini-lakehouse** project built around an **M5-style time-series forecasting workflow**.
Designed to be **PowerShell-first**, **reproducible from a clean checkout**, and to produce **report-ready artifacts** (tables/figures) in `outputs/`.

---

## What this repo does

- **Build features** for an M5-like dataset (lags + rolling stats)
- Run a **Data Quality (DQ) gate** that fails fast on invalid data
- **Train** a baseline model and persist it
- **Evaluate** on a forecasting horizon and write:
  - metrics tables (CSV + LaTeX)
  - prediction dumps (CSV)
  - figures (PDF + PNG)
  - JSON reports

---

## Project structure (high-level)

- `src/retail_ops_mlops/` — package code (CLI + pipelines)
- `configs/` — configuration (paths, etc.)
- `scripts/` — PowerShell entrypoints (one-command run)
- `data/processed/m5/gold/` — model-ready dataset location
- `outputs/` — generated artifacts (models/reports/tables/figures)
- `tests/` — smoke tests ensuring “clean-checkout” robustness
- `.github/workflows/` — CI pipeline

> Note: `data/**` and `outputs/**` are typically **not** committed.

---

## Quickstart (Windows / PowerShell)

### 1) Create & activate venv

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -e .
```

### 2) Run formatting/lint/tests (robust via `python -m ...`)

```powershell
python -m pip install -U ruff pre-commit pytest
python -m ruff check src tests --fix
python -m ruff format src tests
python -m pre_commit run --all-files
python -m pytest -q
```

### 3) Run the M5 pipeline (end-to-end)

```powershell
.\scriptsun_m5.ps1 -Force
```

This will:
1) ensure directories exist
2) run DQ gate
3) train model
4) evaluate + write artifacts

---

## CLI usage

```powershell
python -m retail_ops_mlops.cli --help
python -m retail_ops_mlops.cli ensure-dirs
python -m retail_ops_mlops.cli train-m5 --force
python -m retail_ops_mlops.cli eval-m5 --force
```

---

## Outputs you should expect

After a successful run:

- `outputs/models/`
  - `m5_ridge_baseline.joblib`
- `outputs/reports/`
  - `train_m5.json`
  - `eval_m5.json`
- `outputs/tables/`
  - `eval_m5_metrics.csv`
  - `eval_m5_metrics.tex`
  - `eval_m5_predictions.csv`
- `outputs/figures/`
  - `eval_m5_pred_vs_true.pdf` (+ `.png`)
  - `eval_m5_residuals.pdf` (+ `.png`)

---

## Reproducibility guarantees

This repo aims for:

- **PowerShell-robust commands** (no fragile placeholders)
- **CI smoke test** that runs the pipeline on minimal data
- **Fail-fast DQ** so you don’t train on broken inputs

---

## Roadmap (next upgrades)

- Stronger baselines (LightGBM / XGBoost / CatBoost)
- Better backtesting (rolling origin evaluation)
- MLflow tracking + model registry patterns
- Packaging polish (Makefile equivalent for PS, docs, versioning)

---

## License

MIT (or choose a license that fits your portfolio needs).
